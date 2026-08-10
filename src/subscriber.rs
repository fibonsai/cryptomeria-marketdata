use anyhow::{Context, Result};
use log::{info, warn};
use nng::options::protocol::pubsub::Subscribe;
use nng::options::{Options, RecvTimeout};
use nng::{Error, Protocol, Socket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const RECV_TIMEOUT_MS: u64 = 500;

/// The built-in log subscriber: connects to the local NNG TCP port, subscribes
/// to every current and future topic and logs received messages to stdout
/// in a structured JSON schema. Only loaded when `--data-out` is passed.
pub struct StdoutSubscriber {
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl StdoutSubscriber {
    /// Connect to `tcp://127.0.0.1:{port}` and subscribe to all topics.
    pub fn connect(port: u16) -> Result<Self> {
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

        info!(
            "[stdout_subscriber]: connected to tcp://127.0.0.1:{port}, subscribing to all topics"
        );

        let shutdown = Arc::new(AtomicBool::new(false));
        let handle = thread::Builder::new()
            .name("stdout-subscriber".to_string())
            .spawn({
                let shutdown = Arc::clone(&shutdown);
                move || receive_loop(socket, shutdown)
            })
            .context("failed to spawn log subscriber thread")?;

        Ok(Self {
            shutdown,
            handle: Some(handle),
        })
    }

    /// Stop the receive loop.
    pub fn close(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        info!("[stdout_subscriber]: shutting down");
    }
}

impl Drop for StdoutSubscriber {
    fn drop(&mut self) {
        self.close();
    }
}

fn receive_loop(socket: Socket, shutdown: Arc<AtomicBool>) {
    while !shutdown.load(Ordering::Relaxed) {
        match socket.recv() {
            Ok(message) => log_message(&message),
            Err(Error::TimedOut) => continue,
            Err(err) => {
                warn!("[stdout_subscriber]: receive error: {err}");
                break;
            }
        }
    }
}

fn log_message(message: &nng::Message) {
    let bytes = message.as_slice();
    match crate::forward::build_log_entry(bytes) {
        Ok(entry) => info!("{entry}"),
        Err(e) => warn!("[stdout_subscriber]: failed to parse frame: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forward::frame_message;
    use nng::Message;

    #[test]
    fn log_message_emits_structured_json_schema() {
        let framed = frame_message("lob__btcusdt", br#"{"exchange":"okx","ts":123}"#);
        let mut message = Message::new();
        message.push_back(&framed);

        let entry = crate::forward::build_log_entry(&framed).expect("frame should parse");
        let parsed: serde_json::Value =
            serde_json::from_str(&entry).expect("output should be valid JSON");
        assert_eq!(parsed["topic"], "lob__btcusdt");
        assert_eq!(parsed["payload"]["exchange"], "okx");
        assert_eq!(parsed["payload"]["ts"], 123);

        log_message(&message);
    }

    #[test]
    fn log_message_emits_json_schema_for_trade_topic() {
        let framed = frame_message(
            "trade__btcusdt",
            br#"{"price":100.0,"size":1.0,"side":"buy"}"#,
        );
        let mut message = Message::new();
        message.push_back(&framed);

        let entry = crate::forward::build_log_entry(&framed).expect("frame should parse");
        let parsed: serde_json::Value =
            serde_json::from_str(&entry).expect("output should be valid JSON");
        assert_eq!(parsed["topic"], "trade__btcusdt");
        assert_eq!(parsed["payload"]["price"], 100.0);
        assert_eq!(parsed["payload"]["size"], 1.0);
        assert_eq!(parsed["payload"]["side"], "buy");

        log_message(&message);
    }
}
