use anyhow::{Context, Result};
use nng::options::protocol::pubsub::Subscribe;
use nng::options::{Options, RecvTimeout};
use nng::{Error, Protocol, Socket};
use rasant::Logger;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const RECV_TIMEOUT_MS: u64 = 500;

/// The built-in log subscriber: connects to the local NNG TCP port, subscribes
/// to every current and future topic and logs received messages to stdout with
/// rasant in a structured JSON schema. Only loaded when `--data-out` is passed.
pub struct StdoutSubscriber {
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
    log: Logger,
}

impl StdoutSubscriber {
    /// Connect to `tcp://127.0.0.1:{port}` and subscribe to all topics.
    pub fn connect(port: u16, mut log: Logger) -> Result<Self> {
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

        rasant::info!(
            log,
            &format!(
                "[stdout_subscriber]: connected to tcp://127.0.0.1:{port}, subscribing to all topics"
            )
        );

        let shutdown = Arc::new(AtomicBool::new(false));
        let handle = thread::Builder::new()
            .name("stdout-subscriber".to_string())
            .spawn({
                let shutdown = Arc::clone(&shutdown);
                let log = log.clone();
                move || receive_loop(socket, shutdown, log)
            })
            .context("failed to spawn log subscriber thread")?;

        Ok(Self {
            shutdown,
            handle: Some(handle),
            log,
        })
    }

    /// Stop the receive loop.
    pub fn close(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        rasant::info!(self.log.clone(), "[stdout_subscriber]: shutting down");
    }
}

impl Drop for StdoutSubscriber {
    fn drop(&mut self) {
        self.close();
    }
}

fn receive_loop(socket: Socket, shutdown: Arc<AtomicBool>, mut log: Logger) {
    while !shutdown.load(Ordering::Relaxed) {
        match socket.recv() {
            Ok(message) => log_message(log.clone(), &message),
            Err(Error::TimedOut) => continue,
            Err(err) => {
                rasant::warn!(log, &format!("[stdout_subscriber]: receive error: {err}"));
                break;
            }
        }
    }
}

fn log_message(mut log: Logger, message: &nng::Message) {
    let bytes = message.as_slice();
    match crate::forward::build_log_entry(bytes) {
        Ok(entry) => rasant::info!(log, entry.as_str()),
        Err(e) => rasant::warn!(
            log,
            &format!("[stdout_subscriber]: failed to parse frame: {e}")
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forward::frame_message;
    use nng::Message;

    #[test]
    fn log_message_emits_structured_json_schema() {
        let mut log = rasant::Logger::new();
        let mem_sink = rasant::sink::memory::default();
        let output = mem_sink.output();
        log.add_sink(mem_sink).set_level(rasant::Level::Info);

        let framed = frame_message("lob__btcusdt", br#"{"exchange":"okx","ts":123}"#);
        let mut message = Message::new();
        message.push_back(&framed);
        log_message(log, &message);

        let result = output.as_string();
        let json_start = result
            .find('{')
            .expect("output should contain JSON message");
        let parsed: serde_json::Value =
            serde_json::from_str(&result[json_start..]).expect("message should be valid JSON");
        assert_eq!(parsed["topic"], "lob__btcusdt");
        assert_eq!(parsed["payload"]["exchange"], "okx");
        assert_eq!(parsed["payload"]["ts"], 123);
    }

    #[test]
    fn log_message_emits_json_schema_for_trade_topic() {
        let mut log = rasant::Logger::new();
        let mem_sink = rasant::sink::memory::default();
        let output = mem_sink.output();
        log.add_sink(mem_sink).set_level(rasant::Level::Info);

        let framed = frame_message(
            "trade__btcusdt",
            br#"{"price":100.0,"size":1.0,"side":"buy"}"#,
        );
        let mut message = Message::new();
        message.push_back(&framed);
        log_message(log, &message);

        let result = output.as_string();
        let json_start = result
            .find('{')
            .expect("output should contain JSON message");
        let parsed: serde_json::Value =
            serde_json::from_str(&result[json_start..]).expect("message should be valid JSON");
        assert_eq!(parsed["topic"], "trade__btcusdt");
        assert_eq!(parsed["payload"]["price"], 100.0);
        assert_eq!(parsed["payload"]["size"], 1.0);
        assert_eq!(parsed["payload"]["side"], "buy");
    }
}
