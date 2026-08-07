use crate::forward::frame_message;
use crate::registry::SharedRegistry;
use anyhow::{Context, Result, anyhow};
use nng::options::{Options, SendTimeout};
use nng::{Message, Protocol, Socket};
use std::sync::mpsc::{self, SyncSender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const PUBLISH_CHANNEL_CAPACITY: usize = 1024;
const SEND_TIMEOUT_MS: u64 = 1000;

/// The NNG PUB socket serving dynamic `type__instrument` topics over TCP.
///
/// Publishing is queued to a dedicated sender thread so the async caller never
/// blocks on a slow subscriber. Subscriber counts are tracked in a shared
/// [`SharedRegistry`] because NNG does not expose subscription state to a
/// publisher.
pub struct Broker {
    sender: Option<SyncSender<Vec<u8>>>,
    registry: SharedRegistry,
    handle: Option<JoinHandle<()>>,
}

impl Broker {
    /// Bind an NNG PUB socket on `tcp://0.0.0.0:{port}` and spawn its sender thread.
    pub fn bind(port: u16, registry: SharedRegistry) -> Result<Self> {
        let socket = Socket::new(Protocol::Pub0).context("failed to create NNG pub socket")?;
        socket
            .set_opt::<SendTimeout>(Some(Duration::from_millis(SEND_TIMEOUT_MS)))
            .context("failed to set NNG send timeout")?;
        socket
            .listen(&format!("tcp://0.0.0.0:{port}"))
            .with_context(|| format!("failed to bind NNG broker on port {port}"))?;

        let (sender, receiver) = mpsc::sync_channel::<Vec<u8>>(PUBLISH_CHANNEL_CAPACITY);
        let handle = thread::Builder::new()
            .name("nng-broker".to_string())
            .spawn(move || sender_thread(socket, receiver))
            .context("failed to spawn NNG broker thread")?;

        Ok(Self {
            sender: Some(sender),
            registry,
            handle: Some(handle),
        })
    }

    /// Queue a message for the given topic. Never blocks the caller: when the
    /// channel is full the message is dropped and a warning is logged.
    pub fn publish(&self, topic: &str, payload: &[u8]) -> Result<()> {
        self.registry.record_topic(topic);
        let sender = self
            .sender
            .as_ref()
            .ok_or_else(|| anyhow!("broker is closed"))?;
        let framed = frame_message(topic, payload);
        match sender.try_send(framed) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Full(_)) => {
                tracing::warn!("[system]: publish backlog full, dropping message for {topic}");
                Ok(())
            }
            Err(mpsc::TrySendError::Disconnected(_)) => Err(anyhow!("NNG broker thread is gone")),
        }
    }

    /// Read access to the shared subscriber registry.
    pub fn registry(&self) -> &SharedRegistry {
        &self.registry
    }

    /// Stop the sender thread and drop the socket.
    pub fn close(&mut self) {
        self.sender.take();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        self.close();
    }
}

fn sender_thread(socket: Socket, receiver: mpsc::Receiver<Vec<u8>>) {
    while let Ok(framed) = receiver.recv() {
        let mut message = Message::new();
        message.push_back(&framed);
        if let Err(err) = socket.send(message) {
            tracing::warn!("[system]: NNG send failed: {err:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::Subscriber;
    use nng::options::RecvTimeout;
    use nng::options::protocol::pubsub::Subscribe;

    /// Pick an ephemeral port by binding a TcpListener and releasing it.
    fn ephemeral_port() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    }

    #[test]
    fn delivers_published_message_to_subscriber() {
        let port = ephemeral_port();
        let registry = SharedRegistry::new();
        let broker = Broker::bind(port, registry.clone()).unwrap();

        let sub = Socket::new(Protocol::Sub0).unwrap();
        sub.set_opt::<Subscribe>(b"lob__btcusdt".to_vec()).unwrap();
        sub.set_opt::<RecvTimeout>(Some(Duration::from_millis(2000)))
            .unwrap();
        sub.dial(&format!("tcp://127.0.0.1:{port}")).unwrap();

        std::thread::sleep(Duration::from_millis(100));
        broker
            .publish("lob__btcusdt", br#"{"ts":123}"#)
            .expect("publish succeeds");

        let message = sub.recv().expect("subscriber receives the message");
        let (topic, payload) =
            crate::forward::split_frame(message.as_slice()).expect("message is well framed");
        assert_eq!(topic, "lob__btcusdt");
        assert_eq!(payload, br#"{"ts":123}"#);
    }

    #[test]
    fn records_published_topics_in_registry() {
        let port = ephemeral_port();
        let registry = SharedRegistry::new();
        let broker = Broker::bind(port, registry.clone()).unwrap();
        broker.publish("lob__btcusdt", b"{}").unwrap();
        broker.publish("trade__btcusdt", b"{}").unwrap();
        assert_eq!(registry.count_for_topic("lob__btcusdt"), 0);
        assert_eq!(registry.subscriber_count(), 0);
    }

    #[test]
    fn counts_subscribers_in_registry() {
        let registry = SharedRegistry::new();
        registry.add(Subscriber::all("stdout_subscriber"));
        registry.record_topic("lob__btcusdt");
        assert_eq!(registry.count_for_topic("lob__btcusdt"), 1);
    }
}
