use cryptomeria_ingest::{DataKind, DataSourceConfig, ExchangeFallbackMapping, ResilienceConfig};
use serde::Deserialize;
use std::collections::HashMap;

const DEFAULT_NNG_PORT: u16 = 14242;

/// A single validated exchange source: exchange id, instrument symbol,
/// data source config, and optional topic suffix override.
pub type ValidatedSource = (String, String, DataSourceConfig, Option<String>);

/// Top-level application configuration, loaded from a TOML file.
#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub source: HashMap<String, SourceConfig>,
    pub nng: NngConfig,
}

impl AppConfig {
    /// Return every configured exchange source, sorted by exchange id for
    /// deterministic ordering.
    ///
    /// Unlike the previous single-exchange helper, this supports running
    /// multiple exchanges in parallel. Returns an error only when no exchange
    /// is configured at all.
    pub fn exchange_sources(&self) -> Result<Vec<(&String, &SourceConfig)>, ConfigError> {
        if self.source.is_empty() {
            return Err(ConfigError::InvalidSource(
                "no [source.<exchange>] section found in config".into(),
            ));
        }
        let mut entries: Vec<(&String, &SourceConfig)> = self.source.iter().collect();
        entries.sort_by_key(|(exchange, _)| *exchange);
        Ok(entries)
    }

    /// Build a validated `DataSourceConfig` for every configured exchange.
    ///
    /// Returns `ValidatedSource` tuples sorted by exchange
    /// id. The suffix is `None` when not configured. Validation is local
    /// (exchange/region/kind checks only); instrument resolution against each
    /// exchange still happens inside `ingest::stream`. This lets the caller
    /// fail fast on bad configs before binding the broker.
    pub fn validated_sources(&self) -> Result<Vec<ValidatedSource>, ConfigError> {
        self.exchange_sources()?
            .into_iter()
            .map(|(exchange, source)| {
                let data_source = source.to_data_source(exchange)?;
                Ok((
                    exchange.clone(),
                    source.instrument.clone(),
                    data_source,
                    source.suffix_topic.clone(),
                ))
            })
            .collect()
    }

    /// Override `silence_timeout_secs` on every configured source.
    ///
    /// When `secs` is `Some(n)`, every `SourceConfig.resilience.silence_timeout_secs`
    /// is set to `Some(n)` so that all exchange WebSocket streams share the same
    /// silence-detection window. When `None`, individual source settings are
    /// left untouched (used to apply a CLI override only when the flag is present).
    pub fn override_silence_timeout_secs(&mut self, secs: Option<u64>) {
        if let Some(secs) = secs {
            for source in self.source.values_mut() {
                source.resilience.silence_timeout_secs = Some(secs);
            }
        }
    }
}

/// Exchange WebSocket subscription settings.
#[derive(Debug, Clone, Deserialize)]
pub struct SourceConfig {
    pub region: String,
    pub instrument: String,
    /// Optional alias used to select a per-exchange fallback mapping
    /// under `[source.<exchange>.fallback.<alias>]`. Defaults to the
    /// exchange-only rule.
    #[serde(default)]
    pub alias: Option<String>,
    /// One of "lob", "trade", "both", "lob|trade".
    pub data_kind: String,
    #[serde(default)]
    pub max_level: Option<usize>,
    #[serde(default)]
    pub max_level_pct: f64,
    #[serde(default)]
    pub resilience: ResilienceConfig,
    /// Per-alias fallback mappings for this exchange, keyed by instrument
    /// alias. See `cryptomeria-ingest` README for details.
    #[serde(default)]
    pub fallback: HashMap<String, ExchangeFallbackMapping>,
    /// Optional suffix to override the normalized instrument name in NNG
    /// topic names. When `Some(value)`, topics use `{kind}__{value}`
    /// verbatim (no normalization); when `None`/absent, topics use the
    /// normalized instrument `{kind}__{normalized}` as before.
    #[serde(default)]
    pub suffix_topic: Option<String>,
}

/// NNG PUB/SUB broker settings (TCP transport).
#[derive(Debug, Clone, Deserialize)]
pub struct NngConfig {
    #[serde(default = "default_nng_port")]
    pub port: u16,
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
    ///
    /// `exchange` is the key from the parent `[source.<exchange>]` section.
    pub fn to_data_source(&self, exchange: &str) -> Result<DataSourceConfig, ConfigError> {
        let data_source = DataSourceConfig {
            exchange: exchange.to_string(),
            region: self.region.clone(),
            instrument: self.instrument.clone(),
            data_kind: self.data_kind()?,
            alias: self.alias.clone(),
            max_level: self.max_level,
            max_level_pct: self.max_level_pct,
            resilience: self.resilience.clone(),
            fallback: HashMap::from([(exchange.to_string(), self.fallback.clone())]),
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
[source.okx]
region = "global"
instrument = "BTC-USDT"
data_kind = "both"

[nng]
port = 14242
"#;

    const MULTI_EXCHANGE_TOML: &str = r#"
[source.okx]
region = "global"
instrument = "BTC-USDT"
data_kind = "lob"

[source.kraken]
region = "global"
instrument = "XBT/USD"
data_kind = "trade"

[nng]
port = 14242
"#;

    #[test]
    fn parses_valid_config() {
        let config = parse_config(VALID_TOML).unwrap();
        let source = config
            .source
            .get("okx")
            .expect("okx source should be present");
        assert_eq!(source.region, "global");
        assert_eq!(source.instrument, "BTC-USDT");
        assert_eq!(source.data_kind, "both");
        assert_eq!(config.nng.port, 14242);
    }

    #[test]
    fn applies_defaults_for_optional_fields() {
        let config = parse_config(VALID_TOML).unwrap();
        let source = config.source.get("okx").unwrap();
        assert_eq!(source.max_level, None);
        assert_eq!(source.max_level_pct, 0.0);
        assert_eq!(config.nng.port, DEFAULT_NNG_PORT);
    }

    #[test]
    fn nng_port_defaults_to_14242_when_omitted() {
        let toml = r#"
[source.okx]
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
[source.okx]
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
    fn parses_resilience_section_under_exchange_subkey() {
        let toml = r#"
[source.okx]
region = "global"
instrument = "BTC-USDT"
data_kind = "lob"
[source.okx.resilience]
initial_backoff_ms = 500
max_backoff_ms = 5000
backoff_multiplier = 2.0
jitter_ms = 100

[nng]
port = 14242
"#;
        let config = parse_config(toml).unwrap();
        let source = config.source.get("okx").unwrap();
        assert_eq!(source.resilience.initial_backoff_ms, 500);
        assert_eq!(source.resilience.max_backoff_ms, 5000);
        assert_eq!(source.resilience.max_attempts, None);
    }

    #[test]
    fn parses_silence_timeout_secs_under_resilience() {
        let toml = r#"
[source.okx]
region = "global"
instrument = "BTC-USDT"
data_kind = "lob"
[source.okx.resilience]
initial_backoff_ms = 500
max_backoff_ms = 5000
backoff_multiplier = 2.0
jitter_ms = 100
silence_timeout_secs = 30

[nng]
port = 14242
"#;
        let config = parse_config(toml).unwrap();
        let source = config.source.get("okx").unwrap();
        assert_eq!(source.resilience.silence_timeout_secs, Some(30));
    }

    #[test]
    fn silence_timeout_secs_defaults_to_none_when_omitted() {
        let config = parse_config(VALID_TOML).unwrap();
        let source = config.source.get("okx").unwrap();
        assert_eq!(source.resilience.silence_timeout_secs, None);
    }

    #[test]
    fn to_data_source_forwards_silence_timeout_secs() {
        let toml = r#"
[source.okx]
region = "global"
instrument = "BTC-USDT"
data_kind = "lob"
[source.okx.resilience]
initial_backoff_ms = 1000
max_backoff_ms = 60000
backoff_multiplier = 2.0
jitter_ms = 1000
silence_timeout_secs = 30

[nng]
port = 14242
"#;
        let config = parse_config(toml).unwrap();
        let source = config.source.get("okx").unwrap();
        let data_source = source.to_data_source("okx").unwrap();
        assert_eq!(data_source.resilience.silence_timeout_secs, Some(30));
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
        let source = config
            .exchange_sources()
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .1;
        let data_source = source.to_data_source("okx").unwrap();
        assert_eq!(data_source.exchange, "okx");
        assert!(data_source.data_kind.contains(DataKind::LOB));
        assert!(data_source.data_kind.contains(DataKind::TRADE));
    }

    #[test]
    fn rejects_unknown_exchange_on_conversion() {
        let toml = r#"
[source.binance]
region = "global"
instrument = "BTCUSDT"
data_kind = "lob"

[nng]
port = 14242
"#;
        let config = parse_config(toml).unwrap();
        let source = config.source.get("binance").unwrap();
        let err = source.to_data_source("binance").unwrap_err();
        assert!(matches!(err, ConfigError::InvalidSource(_)));
    }

    #[test]
    fn parses_alias_and_fallback_mapping_under_exchange_subkey() {
        let toml = r#"
[source.okx]
region = "global"
instrument = "btc/usdt"
alias = "btcusd"
data_kind = "lob"

[source.okx.fallback.btcusd]
base_mappings = ["BTC", "XBT"]
quote_mappings = ["USDT", "USD"]
separator_mappings = ["-", "/"]
case_fallback = "upper"

[nng]
port = 14242
"#;
        let config = parse_config(toml).unwrap();
        let source = config.source.get("okx").unwrap();
        assert_eq!(source.alias.as_deref(), Some("btcusd"));
        let mapping = source
            .fallback
            .get("btcusd")
            .expect("fallback mapping should be present");
        assert_eq!(mapping.base_mappings, vec!["BTC", "XBT"]);
        assert_eq!(mapping.quote_mappings, vec!["USDT", "USD"]);
        assert_eq!(mapping.separator_mappings, vec!["-", "/"]);
        assert_eq!(mapping.case_fallback, CaseFallback::Upper);
    }

    #[test]
    fn applies_defaults_for_alias_and_fallback_when_omitted() {
        let config = parse_config(VALID_TOML).unwrap();
        let source = config.source.get("okx").unwrap();
        assert_eq!(source.alias, None);
        assert!(source.fallback.is_empty());
    }

    #[test]
    fn suffix_topic_defaults_to_none_when_omitted() {
        let config = parse_config(VALID_TOML).unwrap();
        let source = config.source.get("okx").unwrap();
        assert_eq!(source.suffix_topic, None);
    }

    #[test]
    fn parses_suffix_topic_when_present() {
        let toml = r#"
[source.okx]
region = "global"
instrument = "BTC-USDT"
data_kind = "both"
suffix_topic = "mytopic"

[nng]
port = 14242
"#;
        let config = parse_config(toml).unwrap();
        let source = config.source.get("okx").unwrap();
        assert_eq!(source.suffix_topic.as_deref(), Some("mytopic"));
    }

    #[test]
    fn validated_sources_includes_suffix_topic() {
        let toml = r#"
[source.okx]
region = "global"
instrument = "BTC-USDT"
data_kind = "both"
suffix_topic = "btcusdt"

[nng]
port = 14242
"#;
        let config = parse_config(toml).unwrap();
        let sources = config.validated_sources().unwrap();
        let (_exchange, _instrument, _data_source, suffix) = sources
            .iter()
            .find(|(e, _, _, _)| *e == "okx")
            .expect("okx should be present");
        assert_eq!(suffix, &Some("btcusdt".to_string()));
    }

    #[test]
    fn to_data_source_forwards_alias_and_fallback() {
        let toml = r#"
[source.okx]
region = "global"
instrument = "btc/usdt"
alias = "btcusd"
data_kind = "lob"

[source.okx.fallback.btcusd]
base_mappings = ["BTC"]
quote_mappings = ["USDT"]
separator_mappings = ["-"]
case_fallback = "upper"

[nng]
port = 14242
"#;
        let config = parse_config(toml).unwrap();
        let source = config
            .exchange_sources()
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .1;
        let data_source = source.to_data_source("okx").unwrap();
        assert_eq!(data_source.alias.as_deref(), Some("btcusd"));
        let mapping = data_source
            .fallback
            .get("okx")
            .and_then(|a| a.get("btcusd"))
            .expect("fallback should be forwarded");
        assert_eq!(mapping.base_mappings, vec!["BTC"]);
    }

    #[test]
    fn exchange_sources_returns_all_exchanges_sorted_alphabetically() {
        let config = parse_config(MULTI_EXCHANGE_TOML).unwrap();
        let sources = config.exchange_sources().unwrap();
        let exchanges: Vec<&str> = sources.iter().map(|(e, _)| e.as_str()).collect();
        assert_eq!(exchanges, vec!["kraken", "okx"]);
        assert_eq!(sources.len(), 2);
    }

    #[test]
    fn exchange_sources_errors_when_no_exchanges() {
        let mut config = parse_config(VALID_TOML).unwrap();
        config.source.clear();
        let result = config.exchange_sources();
        assert!(result.is_err());
    }

    #[test]
    fn validated_sources_builds_one_data_source_per_exchange() {
        let config = parse_config(MULTI_EXCHANGE_TOML).unwrap();
        let sources = config.validated_sources().unwrap();
        assert_eq!(sources.len(), 2);
        let exchanges: Vec<&str> = sources.iter().map(|(e, _, _, _)| e.as_str()).collect();
        assert_eq!(exchanges, vec!["kraken", "okx"]);
        let kraken = sources
            .iter()
            .find(|(e, _, _, _)| *e == "kraken")
            .expect("kraken should be present");
        assert_eq!(kraken.1, "XBT/USD");
        let okx = sources
            .iter()
            .find(|(e, _, _, _)| *e == "okx")
            .expect("okx should be present");
        assert_eq!(okx.1, "BTC-USDT");
    }

    #[test]
    fn validated_sources_errors_on_unknown_exchange() {
        let toml = r#"
[source.binance]
region = "global"
instrument = "BTCUSDT"
data_kind = "lob"

[nng]
port = 14242
"#;
        let config = parse_config(toml).unwrap();
        let result = config.validated_sources();
        assert!(result.is_err());
    }

    #[test]
    fn override_silence_timeout_secs_sets_value_on_all_sources() {
        let toml = r#"
[source.okx]
region = "global"
instrument = "BTC-USDT"
data_kind = "lob"

[source.kraken]
region = "global"
instrument = "XBT/USD"
data_kind = "trade"

[nng]
port = 14242
"#;
        let mut config = parse_config(toml).unwrap();
        config.override_silence_timeout_secs(Some(45));
        for source in config.source.values() {
            assert_eq!(source.resilience.silence_timeout_secs, Some(45));
        }
    }

    #[test]
    fn override_silence_timeout_secs_none_leaves_existing_values() {
        let toml = r#"
[source.okx]
region = "global"
instrument = "BTC-USDT"
data_kind = "lob"

[nng]
port = 14242
"#;
        let mut config = parse_config(toml).unwrap();
        let okx = config.source.get("okx").unwrap();
        let original = okx.resilience.silence_timeout_secs;
        config.override_silence_timeout_secs(None);
        let okx_after = config.source.get("okx").unwrap();
        assert_eq!(okx_after.resilience.silence_timeout_secs, original);
    }
}
