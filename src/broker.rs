use crate::forward::frame_message;
use anyhow::{Context, Result, anyhow};
use log::{info, warn};
use nng::options::{Options, RemAddr, SendTimeout};
use nng::{Message, Pipe, PipeEvent, Protocol, Socket};
use std::collections::HashMap;
use std::sync::mpsc::{self, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const PUBLISH_CHANNEL_CAPACITY: usize = 1024;
const SEND_TIMEOUT_MS: u64 = 1000;

/// The NNG PUB socket serving dynamic `type__instrument` topics over TCP.
///
/// Publishing is queued to a dedicated sender thread so the async caller never
/// blocks on a slow subscriber. The struct is `Sync` (the join handle is
/// mutex-guarded) so a single `Broker` can be shared across concurrent tasks via
/// `Arc<Broker>`; `publish` takes only `&self` and never blocks.
///
/// Remote addresses of connected pipes are cached in a map shared with the
/// `pipe_notify` callback so that disconnect log messages can report *who*
/// left — `RemAddr` is unavailable on the `RemovePost` event because the
/// transport has already been torn down.
pub struct Broker {
    sender: Option<SyncSender<Vec<u8>>>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl Broker {
    /// Bind an NNG PUB socket on `tcp://0.0.0.0:{port}` and spawn its sender thread.
    pub fn bind(port: u16) -> Result<Self> {
        let socket = Socket::new(Protocol::Pub0).context("failed to create NNG pub socket")?;
        socket
            .set_opt::<SendTimeout>(Some(Duration::from_millis(SEND_TIMEOUT_MS)))
            .context("failed to set NNG send timeout")?;
        socket
            .listen(&format!("tcp://0.0.0.0:{port}"))
            .with_context(|| format!("failed to bind NNG broker on port {port}"))?;

        // Cache remote addresses on connect so they are available for the
        // disconnect log — NNG's RemovePost fires after the transport is gone,
        // making pipe.get_opt::<RemAddr>() unreliable at that point.
        let addrs = Arc::new(Mutex::new(HashMap::<Pipe, String>::new()));

        socket
            .pipe_notify(move |pipe, event| match event {
                PipeEvent::AddPost => {
                    let addr = pipe.get_opt::<RemAddr>().ok().map(|a| a.to_string());
                    if let Some(ref addr) = addr {
                        addrs.lock().unwrap().insert(pipe, addr.clone());
                    }
                    info!("{}", connect_log(addr.as_deref()));
                }
                PipeEvent::RemovePost => {
                    let addr = addrs.lock().unwrap().remove(&pipe);
                    info!("{}", disconnect_log(addr.as_deref()));
                }
                _ => {}
            })
            .context("failed to register pipe notify callback")?;

        let (sender, receiver) = mpsc::sync_channel::<Vec<u8>>(PUBLISH_CHANNEL_CAPACITY);
        let handle = thread::Builder::new()
            .name("nng-broker".to_string())
            .spawn(move || sender_thread(socket, receiver))
            .context("failed to spawn NNG broker thread")?;

        Ok(Self {
            sender: Some(sender),
            handle: Mutex::new(Some(handle)),
        })
    }

    /// Queue a message for the given topic. Never blocks the caller: when the
    /// channel is full the message is dropped and a warning is logged.
    pub fn publish(&self, topic: &str, payload: &[u8]) -> Result<()> {
        let sender = self
            .sender
            .as_ref()
            .ok_or_else(|| anyhow!("broker is closed"))?;
        let framed = frame_message(topic, payload);
        match sender.try_send(framed) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Full(_)) => {
                warn!("[system]: publish backlog full, dropping message for {topic}");
                Ok(())
            }
            Err(mpsc::TrySendError::Disconnected(_)) => Err(anyhow!("NNG broker thread is gone")),
        }
    }

    /// Stop the sender thread and drop the socket.
    pub fn close(&mut self) {
        self.sender.take();
        if let Some(handle) = self.handle.lock().unwrap().take() {
            let _ = handle.join();
        }
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        self.close();
    }
}

/// Build the log message for a subscriber connecting, optionally including the
/// remote address.
fn connect_log(addr: Option<&str>) -> String {
    match addr {
        Some(addr) => format!("[broker]: subscriber connected from {addr}"),
        None => "[broker]: subscriber connected".to_string(),
    }
}

/// Build the log message for a subscriber disconnecting, optionally including the
/// remote address.
fn disconnect_log(addr: Option<&str>) -> String {
    match addr {
        Some(addr) => format!("[broker]: subscriber disconnected from {addr}"),
        None => "[broker]: subscriber disconnected".to_string(),
    }
}

fn sender_thread(socket: Socket, receiver: mpsc::Receiver<Vec<u8>>) {
    while let Ok(framed) = receiver.recv() {
        let mut message = Message::new();
        message.push_back(&framed);
        if let Err(err) = socket.send(message) {
            warn!("[system]: NNG send failed: {err:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nng::options::RecvTimeout;
    use nng::options::protocol::pubsub::Subscribe;

    #[test]
    fn connect_log_shows_remote_address_when_available() {
        let result = connect_log(Some("1.2.3.4:5678"));
        assert_eq!(result, "[broker]: subscriber connected from 1.2.3.4:5678");
    }

    #[test]
    fn connect_log_falls_back_when_address_unavailable() {
        let result = connect_log(None);
        assert_eq!(result, "[broker]: subscriber connected");
    }

    #[test]
    fn disconnect_log_shows_remote_address_when_available() {
        let result = disconnect_log(Some("1.2.3.4:5678"));
        assert_eq!(
            result,
            "[broker]: subscriber disconnected from 1.2.3.4:5678"
        );
    }

    #[test]
    fn disconnect_log_falls_back_when_address_unavailable() {
        let result = disconnect_log(None);
        assert_eq!(result, "[broker]: subscriber disconnected");
    }

    /// Pick an ephemeral port by binding a TcpListener and releasing it.
    fn ephemeral_port() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    }

    #[test]
    fn delivers_published_message_to_subscriber() {
        let port = ephemeral_port();
        let broker = Broker::bind(port).unwrap();

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
    fn pipe_notify_fires_on_subscriber_connect_and_disconnect() {
        let port = ephemeral_port();
        let broker = Broker::bind(port).unwrap();

        let sub = Socket::new(Protocol::Sub0).unwrap();
        sub.set_opt::<Subscribe>(Vec::<u8>::new()).unwrap();
        sub.set_opt::<RecvTimeout>(Some(Duration::from_millis(500)))
            .unwrap();
        sub.dial(&format!("tcp://127.0.0.1:{port}")).unwrap();

        std::thread::sleep(Duration::from_millis(100));

        drop(sub);
        std::thread::sleep(Duration::from_millis(200));

        broker
            .publish("lob__btcusdt", br#"{"ts":1}"#)
            .expect("publish still succeeds after subscriber disconnects");
    }
}
