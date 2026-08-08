use anyhow::Result;
use cryptomeria_ingest::MarketDataItem;

/// The kind of market data item: order book or trade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemKind {
    /// Limit order book snapshot/update.
    Lob,
    /// Trade execution.
    Trade,
}

impl ItemKind {
    /// Machine-readable topic segment for the kind: `"lob"` or `"trade"`.
    pub fn as_str(self) -> &'static str {
        match self {
            ItemKind::Lob => "lob",
            ItemKind::Trade => "trade",
        }
    }
}

impl From<&MarketDataItem> for ItemKind {
    fn from(item: &MarketDataItem) -> Self {
        match item {
            MarketDataItem::Lob(_) => ItemKind::Lob,
            MarketDataItem::Trade(_) => ItemKind::Trade,
        }
    }
}

/// Normalize an instrument symbol into the form used in topic names:
/// lowercase, non-alphanumeric characters stripped (e.g. `BTC-USDT` -> `btcusdt`).
pub fn normalize_instrument(instrument: &str) -> String {
    instrument
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

/// Build the dynamic topic for an item: `{kind}__{instrument}` (e.g. `lob__btcusdt`).
///
/// The topic does **not** include the exchange. When several exchanges are run
/// in parallel against the same instrument and kind the topics collide (last
/// writer wins); operators must use distinct instruments per exchange to keep
/// streams separate.
pub fn topic_for(instrument: &str, item: &MarketDataItem) -> String {
    format!(
        "{}__{}",
        ItemKind::from(item).as_str(),
        normalize_instrument(instrument)
    )
}

/// Serialize an item to JSON exactly as received from cryptomeria-ingest.
pub fn build_payload(item: &MarketDataItem) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(item)?)
}

const FRAME_SEPARATOR: u8 = b'\0';

/// Frame a topic and payload into the wire bytes sent over NNG:
/// `topic\0payload`. The topic stays a prefix so NNG SUB topic filtering
/// works while the payload can be split back out by the subscriber.
pub fn frame_message(topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(topic.len() + 1 + payload.len());
    bytes.extend_from_slice(topic.as_bytes());
    bytes.push(FRAME_SEPARATOR);
    bytes.extend_from_slice(payload);
    bytes
}

/// Split a framed wire message back into `(topic, payload)`. Returns `None`
/// when the frame separator is missing.
pub fn split_frame(bytes: &[u8]) -> Option<(String, &[u8])> {
    let idx = bytes.iter().position(|&b| b == FRAME_SEPARATOR)?;
    let topic = String::from_utf8_lossy(&bytes[..idx]).into_owned();
    Some((topic, &bytes[idx + 1..]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cryptomeria_ingest::{LobItem, TradeItem};

    fn lob_item() -> MarketDataItem {
        MarketDataItem::Lob(LobItem {
            ts: 123,
            exchange: "okx".into(),
            bids: vec![],
            asks: vec![],
        })
    }

    fn trade_item() -> MarketDataItem {
        MarketDataItem::Trade(TradeItem {
            ts: 456,
            exchange: "okx".into(),
            price: 100.0,
            size: 1.0,
            side: "buy".into(),
            trade_id: None,
            seq_id: None,
        })
    }

    #[test]
    fn classifies_lob_and_trade_kinds() {
        assert_eq!(ItemKind::from(&lob_item()), ItemKind::Lob);
        assert_eq!(ItemKind::from(&trade_item()), ItemKind::Trade);
        assert_eq!(ItemKind::Lob.as_str(), "lob");
        assert_eq!(ItemKind::Trade.as_str(), "trade");
    }

    #[test]
    fn normalizes_instrument_to_topic_segment() {
        assert_eq!(normalize_instrument("BTC-USDT"), "btcusdt");
        assert_eq!(normalize_instrument("BTC/USDT"), "btcusdt");
        assert_eq!(normalize_instrument("btcusdt"), "btcusdt");
        assert_eq!(normalize_instrument("123-ABC"), "123abc");
    }

    #[test]
    fn builds_lob_topic_with_kind_and_instrument() {
        assert_eq!(topic_for("BTC-USDT", &lob_item()), "lob__btcusdt");
    }

    #[test]
    fn builds_trade_topic_with_kind_and_instrument() {
        assert_eq!(topic_for("BTC-USDT", &trade_item()), "trade__btcusdt");
    }

    #[test]
    fn payload_preserves_item_exactly_as_received() {
        let item = trade_item();
        let expected = serde_json::to_vec(&item).unwrap();
        assert_eq!(build_payload(&item).unwrap(), expected);
    }

    #[test]
    fn frames_topic_and_payload_with_separator() {
        let bytes = frame_message("lob__btcusdt", b"{\"ts\":1}");
        assert_eq!(bytes, b"lob__btcusdt\0{\"ts\":1}");
    }

    #[test]
    fn splits_frame_back_into_topic_and_payload() {
        let bytes = frame_message("trade__btcusdt", b"{\"price\":1.0}");
        let (topic, payload) = split_frame(&bytes).unwrap();
        assert_eq!(topic, "trade__btcusdt");
        assert_eq!(payload, b"{\"price\":1.0}");
    }

    #[test]
    fn split_frame_returns_none_without_separator() {
        assert!(split_frame(b"no separator here").is_none());
    }
}
