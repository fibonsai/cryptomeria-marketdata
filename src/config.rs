use cryptomeria_ingest::{DataKind, DataSourceConfig, ResilienceConfig};
use serde::Deserialize;

const DEFAULT_SNAPSHOT_DEPTH: usize = 400;
const DEFAULT_SUBJECT_LOB: &str = "marketdata.lob";
const DEFAULT_SUBJECT_TRADE: &str = "marketdata.trade";

/// Top-level application configuration, loaded from a TOML file.
#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub source: SourceConfig,
    pub nats: NatsConfig,
}

/// Exchange WebSocket subscription settings.
#[derive(Debug, Clone, Deserialize)]
pub struct SourceConfig {
    pub exchange: String,
    pub region: String,
    pub instrument: String,
    /// One of "lob", "trade", "both", "lob|trade".
    pub data_kind: String,
    #[serde(default)]
    pub max_level: Option<usize>,
    #[serde(default)]
    pub max_level_pct: f64,
    #[serde(default = "default_snapshot_depth")]
    pub snapshot_depth: usize,
    #[serde(default)]
    pub resilience: ResilienceConfig,
}

/// NATS broker settings.
#[derive(Debug, Clone, Deserialize)]
pub struct NatsConfig {
    pub url: String,
    #[serde(default = "default_subject_lob")]
    pub subject_lob: String,
    #[serde(default = "default_subject_trade")]
    pub subject_trade: String,
}

fn default_snapshot_depth() -> usize {
    DEFAULT_SNAPSHOT_DEPTH
}

fn default_subject_lob() -> String {
    DEFAULT_SUBJECT_LOB.into()
}

fn default_subject_trade() -> String {
    DEFAULT_SUBJECT_TRADE.into()
}

/// Configuration parsing/validation error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    InvalidToml(String),
    InvalidDataKind(String),
    InvalidSource(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::InvalidToml(msg) => write!(f, "invalid config toml: {msg}"),
            ConfigError::InvalidDataKind(kind) => {
                write!(
                    f,
                    "invalid data_kind: {kind} (use lob, trade, both, or lob|trade)"
                )
            }
            ConfigError::InvalidSource(msg) => write!(f, "invalid source config: {msg}"),
        }
    }
}

impl std::error::Error for ConfigError {}

/// Parse application configuration from TOML content.
pub fn parse_config(content: &str) -> Result<AppConfig, ConfigError> {
    toml::from_str(content).map_err(|e| ConfigError::InvalidToml(e.to_string()))
}

/// Parse the string representation of a data kind into the ingest flags type.
pub fn parse_data_kind(kind: &str) -> Result<DataKind, ConfigError> {
    match kind.trim().to_ascii_lowercase().as_str() {
        "lob" => Ok(DataKind::LOB),
        "trade" => Ok(DataKind::TRADE),
        "both" | "lob|trade" | "lob,trade" => Ok(DataKind::LOB | DataKind::TRADE),
        other => Err(ConfigError::InvalidDataKind(other.into())),
    }
}

impl SourceConfig {
    /// Resolve the configured data kind flags.
    pub fn data_kind(&self) -> Result<DataKind, ConfigError> {
        parse_data_kind(&self.data_kind)
    }

    /// Convert to the ingest `DataSourceConfig`, validating exchange/region.
    pub fn to_data_source(&self) -> Result<DataSourceConfig, ConfigError> {
        let data_source = DataSourceConfig {
            exchange: self.exchange.clone(),
            region: self.region.clone(),
            instrument: self.instrument.clone(),
            data_kind: self.data_kind()?,
            max_level: self.max_level,
            max_level_pct: self.max_level_pct,
            snapshot_depth: self.snapshot_depth,
            resilience: self.resilience.clone(),
        };
        data_source
            .validate()
            .map_err(|e| ConfigError::InvalidSource(e.to_string()))?;
        Ok(data_source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_TOML: &str = r#"
[source]
exchange = "okx"
region = "global"
instrument = "BTC-USDT"
data_kind = "both"

[nats]
url = "nats://localhost:4222"
"#;

    #[test]
    fn parses_valid_config() {
        let config = parse_config(VALID_TOML).unwrap();
        assert_eq!(config.source.exchange, "okx");
        assert_eq!(config.source.region, "global");
        assert_eq!(config.source.instrument, "BTC-USDT");
        assert_eq!(config.source.data_kind, "both");
        assert_eq!(config.nats.url, "nats://localhost:4222");
    }

    #[test]
    fn applies_defaults_for_optional_fields() {
        let config = parse_config(VALID_TOML).unwrap();
        assert_eq!(config.source.snapshot_depth, DEFAULT_SNAPSHOT_DEPTH);
        assert_eq!(config.source.max_level, None);
        assert_eq!(config.nats.subject_lob, DEFAULT_SUBJECT_LOB);
        assert_eq!(config.nats.subject_trade, DEFAULT_SUBJECT_TRADE);
    }

    #[test]
    fn parses_resilience_section() {
        let toml = r#"
[source]
exchange = "okx"
region = "global"
instrument = "BTC-USDT"
data_kind = "lob"
[source.resilience]
initial_backoff_ms = 500
max_backoff_ms = 5000
backoff_multiplier = 2.0
jitter_ms = 100

[nats]
url = "nats://localhost:4222"
"#;
        let config = parse_config(toml).unwrap();
        assert_eq!(config.source.resilience.initial_backoff_ms, 500);
        assert_eq!(config.source.resilience.max_backoff_ms, 5000);
        assert_eq!(config.source.resilience.max_attempts, None);
    }

    #[test]
    fn rejects_malformed_toml() {
        let err = parse_config("this is not = [valid toml").unwrap_err();
        assert!(matches!(err, ConfigError::InvalidToml(_)));
    }

    #[test]
    fn rejects_missing_nats_url() {
        let toml = r#"
[source]
exchange = "okx"
region = "global"
instrument = "BTC-USDT"
data_kind = "lob"
"#;
        let err = parse_config(toml).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidToml(_)));
    }

    #[test]
    fn parses_lob_kind() {
        assert_eq!(parse_data_kind("lob").unwrap(), DataKind::LOB);
    }

    #[test]
    fn parses_trade_kind() {
        assert_eq!(parse_data_kind("trade").unwrap(), DataKind::TRADE);
    }

    #[test]
    fn parses_both_kinds() {
        assert_eq!(
            parse_data_kind("both").unwrap(),
            DataKind::LOB | DataKind::TRADE
        );
        assert_eq!(
            parse_data_kind("lob|trade").unwrap(),
            DataKind::LOB | DataKind::TRADE
        );
        assert_eq!(
            parse_data_kind("LOB|TRADE").unwrap(),
            DataKind::LOB | DataKind::TRADE
        );
    }

    #[test]
    fn rejects_unknown_kind() {
        let err = parse_data_kind("bogus").unwrap_err();
        assert!(matches!(err, ConfigError::InvalidDataKind(_)));
    }

    #[test]
    fn converts_to_valid_data_source() {
        let config = parse_config(VALID_TOML).unwrap();
        let source = config.source.to_data_source().unwrap();
        assert_eq!(source.exchange, "okx");
        assert!(source.data_kind.contains(DataKind::LOB));
        assert!(source.data_kind.contains(DataKind::TRADE));
    }

    #[test]
    fn rejects_unknown_exchange_on_conversion() {
        let toml = VALID_TOML.replace("\"okx\"", "\"binance\"");
        let config = parse_config(&toml).unwrap();
        let err = config.source.to_data_source().unwrap_err();
        assert!(matches!(err, ConfigError::InvalidSource(_)));
    }
}
