use cryptomeria_ingest::{DataKind, DataSourceConfig, ExchangeFallbackMapping, ResilienceConfig};
use serde::Deserialize;
use std::collections::HashMap;

const DEFAULT_SNAPSHOT_DEPTH: usize = 400;
const DEFAULT_NNG_PORT: u16 = 14242;

/// Top-level application configuration, loaded from a TOML file.
#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub source: SourceConfig,
    pub nng: NngConfig,
}

/// Exchange WebSocket subscription settings.
#[derive(Debug, Clone, Deserialize)]
pub struct SourceConfig {
    pub exchange: String,
    pub region: String,
    pub instrument: String,
    /// Optional alias used to select a per-exchange fallback mapping
    /// (`fallback[exchange][alias]`). Defaults to the exchange-only rule.
    #[serde(default)]
    pub alias: Option<String>,
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
    /// Per-exchange fallback mappings, keyed by exchange name and then by
    /// instrument alias. See `cryptomeria-ingest` README for details.
    #[serde(default)]
    pub fallback: HashMap<String, HashMap<String, ExchangeFallbackMapping>>,
}

/// NNG PUB/SUB broker settings (TCP transport).
#[derive(Debug, Clone, Deserialize)]
pub struct NngConfig {
    #[serde(default = "default_nng_port")]
    pub port: u16,
}

fn default_snapshot_depth() -> usize {
    DEFAULT_SNAPSHOT_DEPTH
}

fn default_nng_port() -> u16 {
    DEFAULT_NNG_PORT
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
            alias: self.alias.clone(),
            max_level: self.max_level,
            max_level_pct: self.max_level_pct,
            snapshot_depth: self.snapshot_depth,
            resilience: self.resilience.clone(),
            fallback: self.fallback.clone(),
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
    use cryptomeria_ingest::CaseFallback;

    const VALID_TOML: &str = r#"
[source]
exchange = "okx"
region = "global"
instrument = "BTC-USDT"
data_kind = "both"

[nng]
port = 14242
"#;

    #[test]
    fn parses_valid_config() {
        let config = parse_config(VALID_TOML).unwrap();
        assert_eq!(config.source.exchange, "okx");
        assert_eq!(config.source.region, "global");
        assert_eq!(config.source.instrument, "BTC-USDT");
        assert_eq!(config.source.data_kind, "both");
        assert_eq!(config.nng.port, 14242);
    }

    #[test]
    fn applies_defaults_for_optional_fields() {
        let config = parse_config(VALID_TOML).unwrap();
        assert_eq!(config.source.snapshot_depth, DEFAULT_SNAPSHOT_DEPTH);
        assert_eq!(config.source.max_level, None);
        assert_eq!(config.nng.port, DEFAULT_NNG_PORT);
    }

    #[test]
    fn nng_port_defaults_to_14242_when_omitted() {
        let toml = r#"
[source]
exchange = "okx"
region = "global"
instrument = "BTC-USDT"
data_kind = "lob"

[nng]
"#;
        let config = parse_config(toml).unwrap();
        assert_eq!(config.nng.port, DEFAULT_NNG_PORT);
    }

    #[test]
    fn parses_custom_nng_port() {
        let toml = r#"
[source]
exchange = "okx"
region = "global"
instrument = "BTC-USDT"
data_kind = "lob"

[nng]
port = 9999
"#;
        let config = parse_config(toml).unwrap();
        assert_eq!(config.nng.port, 9999);
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

[nng]
port = 14242
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
    fn rejects_missing_source_section() {
        let toml = r#"
[nng]
port = 14242
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

    #[test]
    fn parses_alias_and_fallback_mapping() {
        let toml = r#"
[source]
exchange = "okx"
region = "global"
instrument = "btc/usdt"
alias = "btcusd"
data_kind = "lob"

[source.fallback.okx.btcusd]
base_mappings = ["BTC", "XBT"]
quote_mappings = ["USDT", "USD"]
separator_mappings = ["-", "/"]
case_fallback = "upper"

[nng]
port = 14242
"#;
        let config = parse_config(toml).unwrap();
        assert_eq!(config.source.alias.as_deref(), Some("btcusd"));
        let mapping = config
            .source
            .fallback
            .get("okx")
            .and_then(|a| a.get("btcusd"))
            .expect("fallback mapping should be present");
        assert_eq!(mapping.base_mappings, vec!["BTC", "XBT"]);
        assert_eq!(mapping.quote_mappings, vec!["USDT", "USD"]);
        assert_eq!(mapping.separator_mappings, vec!["-", "/"]);
        assert_eq!(mapping.case_fallback, CaseFallback::Upper);
    }

    #[test]
    fn applies_defaults_for_alias_and_fallback_when_omitted() {
        let config = parse_config(VALID_TOML).unwrap();
        assert_eq!(config.source.alias, None);
        assert!(config.source.fallback.is_empty());
    }

    #[test]
    fn to_data_source_forwards_alias_and_fallback() {
        let toml = r#"
[source]
exchange = "okx"
region = "global"
instrument = "btc/usdt"
alias = "btcusd"
data_kind = "lob"

[source.fallback.okx.btcusd]
base_mappings = ["BTC"]
quote_mappings = ["USDT"]
separator_mappings = ["-"]
case_fallback = "upper"

[nng]
port = 14242
"#;
        let config = parse_config(toml).unwrap();
        let source = config.source.to_data_source().unwrap();
        assert_eq!(source.alias.as_deref(), Some("btcusd"));
        let mapping = source
            .fallback
            .get("okx")
            .and_then(|a| a.get("btcusd"))
            .expect("fallback should be forwarded");
        assert_eq!(mapping.base_mappings, vec!["BTC"]);
    }
}
