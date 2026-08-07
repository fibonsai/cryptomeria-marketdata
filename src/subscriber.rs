use crate::forward::split_frame;
use anyhow::{Context, Result};
use nng::options::protocol::pubsub::Subscribe;
use nng::options::{Options, RecvTimeout};
use nng::{Error, Protocol, Socket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const RECV_TIMEOUT_MS: u64 = 500;

/// The built-in log subscriber: connects to the local NNG TCP port, subscribes
/// to every current and future topic and logs received messages to stdout with
/// tracing. Only loaded when `--data-out` is passed.
pub struct StdoutSubscriber {
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
    #[allow(dead_code)]
    exchange: String,
}

impl StdoutSubscriber {
    /// Connect to `tcp://127.0.0.1:{port}` and subscribe to all topics.
    pub fn connect(port: u16, exchange: &str) -> Result<Self> {
        let socket = Socket::new(Protocol::Sub0).context("failed to create NNG sub socket")?;
        socket
            .set_opt::<Subscribe>(Vec::<u8>::new())
            .context("failed to subscribe to all topics")?;
        socket
            .set_opt::<RecvTimeout>(Some(Duration::from_millis(RECV_TIMEOUT_MS)))
            .context("failed to set NNG recv timeout")?;
        socket
            .dial(&format!("tcp://127.0.0.1:{port}"))
            .with_context(|| format!("failed to connect log subscriber to port {port}"))?;

        tracing::info!(
            "[stdout_subscriber]: connected to tcp://127.0.0.1:{port}, subscribing to all topics"
        );

        let shutdown = Arc::new(AtomicBool::new(false));
        let exchange = exchange.to_string();
        let exchange_clone = exchange.clone();
        let handle = thread::Builder::new()
            .name("stdout-subscriber".to_string())
            .spawn({
                let shutdown = Arc::clone(&shutdown);
                let exchange = exchange_clone;
                move || receive_loop(socket, shutdown, exchange)
            })
            .context("failed to spawn log subscriber thread")?;

        Ok(Self {
            shutdown,
            handle: Some(handle),
            exchange,
        })
    }

    /// Stop the receive loop.
    pub fn close(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        tracing::info!("[stdout_subscriber]: shutting down");
    }
}

impl Drop for StdoutSubscriber {
    fn drop(&mut self) {
        self.close();
    }
}

fn receive_loop(socket: Socket, shutdown: Arc<AtomicBool>, exchange: String) {
    while !shutdown.load(Ordering::Relaxed) {
        match socket.recv() {
            Ok(message) => log_message(&message, &exchange),
            Err(Error::TimedOut) => continue,
            Err(err) => {
                tracing::warn!("[stdout_subscriber]: receive error: {err}");
                break;
            }
        }
    }
}

fn log_message(message: &nng::Message, exchange: &str) {
    let bytes = message.as_slice();
    let Some((topic, payload)) = split_frame(bytes) else {
        tracing::warn!("[stdout_subscriber]: received malformed frame, skipping");
        return;
    };
    let kind = topic.split("__").next().unwrap_or("data");
    tracing::info!("[{kind}-{exchange}]: {}", String::from_utf8_lossy(payload));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forward::frame_message;

    #[test]
    fn builds_log_prefix_from_topic_and_exchange() {
        let framed = frame_message("lob__btcusdt", br#"{"ts":1}"#);
        let (topic, _payload) = split_frame(&framed).unwrap();
        let kind = topic.split("__").next().unwrap();
        let exchange = "okx";
        assert_eq!(format!("[{kind}-{exchange}]"), "[lob-okx]");
    }
}
