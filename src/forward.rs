use crate::config::NatsConfig;
use anyhow::Result;
use cryptomeria_ingest::MarketDataItem;

/// Choose the NATS subject for a market data item based on its kind.
pub fn resolve_subject<'a>(cfg: &'a NatsConfig, item: &MarketDataItem) -> &'a str {
    match item {
        MarketDataItem::Lob(_) => &cfg.subject_lob,
        MarketDataItem::Trade(_) => &cfg.subject_trade,
    }
}

/// Serialize a market data item as JSON.
pub fn encode(item: &MarketDataItem) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(item)?)
}

/// A connected NATS client that forwards normalized items to their subject.
pub struct Publisher {
    client: async_nats::Client,
    cfg: NatsConfig,
}

impl Publisher {
    /// Establish a connection and hold the subjects for publishing.
    pub async fn connect(cfg: NatsConfig) -> Result<Self> {
        let client = async_nats::connect(cfg.url.clone()).await?;
        Ok(Self { client, cfg })
    }

    /// Publish a single market data item to its configured subject.
    pub async fn publish(&self, item: &MarketDataItem) -> Result<()> {
        let subject = resolve_subject(&self.cfg, item);
        let payload = encode(item)?;
        self.client
            .publish(subject.to_string(), payload.into())
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cryptomeria_ingest::{LobItem, TradeItem};

    fn nats_config() -> NatsConfig {
        NatsConfig {
            url: "nats://localhost:4222".into(),
            subject_lob: "marketdata.lob".into(),
            subject_trade: "marketdata.trade".into(),
        }
    }

    fn lob_item() -> MarketDataItem {
        MarketDataItem::Lob(LobItem {
            ts: 123,
            bids: vec![],
            asks: vec![],
        })
    }

    fn trade_item() -> MarketDataItem {
        MarketDataItem::Trade(TradeItem {
            ts: 456,
            price: 100.0,
            size: 1.0,
            side: "buy".into(),
            trade_id: None,
            seq_id: None,
        })
    }

    #[test]
    fn routes_lob_to_lob_subject() {
        let cfg = nats_config();
        assert_eq!(resolve_subject(&cfg, &lob_item()), "marketdata.lob");
    }

    #[test]
    fn routes_trade_to_trade_subject() {
        let cfg = nats_config();
        assert_eq!(resolve_subject(&cfg, &trade_item()), "marketdata.trade");
    }

    #[test]
    fn encodes_lob_as_json() {
        let bytes = encode(&lob_item()).unwrap();
        let json = String::from_utf8(bytes).unwrap();
        assert!(json.contains("\"ts\":123"));
        assert!(json.contains("\"bids\""));
        assert!(json.contains("\"asks\""));
    }

    #[test]
    fn encodes_trade_as_json() {
        let bytes = encode(&trade_item()).unwrap();
        let json = String::from_utf8(bytes).unwrap();
        assert!(json.contains("\"side\":\"buy\""));
        assert!(json.contains("\"price\":100.0"));
    }
}
