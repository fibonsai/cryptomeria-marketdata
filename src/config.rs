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

    /// Build a validated `DataSourceConfig` for every configured instrument
    /// across all exchanges.
    ///
    /// Returns `ValidatedSource` tuples sorted by `(exchange, alias)`. The
    /// suffix is `None` when not configured for a given instrument. Validation
    /// is local (exchange/region/kind checks only); instrument resolution
    /// against each exchange still happens inside `ingest::stream`. This lets
    /// the caller fail fast on bad configs before binding the broker.
    ///
    /// Each instrument under `[source.<exchange>.instrument.<alias>]` produces
    /// one `ValidatedSource`, enabling multiple instruments per exchange.
    pub fn validated_sources(&self) -> Result<Vec<ValidatedSource>, ConfigError> {
        let mut results = Vec::new();
        for (exchange, source) in self.exchange_sources()? {
            let mut instrument_entries: Vec<(&String, &InstrumentConfig)> =
                source.instruments.iter().collect();
            instrument_entries.sort_by_key(|(alias, _)| *alias);
            for (alias, instrument_cfg) in instrument_entries {
                let data_source = source.to_data_source(exchange, alias, instrument_cfg)?;
                results.push((
                    exchange.clone(),
                    instrument_cfg.instrument.clone(),
                    data_source,
                    instrument_cfg.suffix_topic.clone(),
                ));
            }
        }
        Ok(results)
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

/// Per-instrument configuration within an exchange source.
///
/// Each instrument is configured under
/// `[source.<exchange>.instrument.<alias>]`. The `<alias>` key (the
/// `HashMap` key, not a field) doubles as the `DataSourceConfig.alias` value
/// and as the lookup key into the sibling
/// `[source.<exchange>.fallback.<alias>]` section.
///
/// Using an empty-string alias key (`[source.<exchange>.instrument.""]`)
/// selects the exchange-only fallback rule (i.e. `alias = None` in the
/// underlying `DataSourceConfig`).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct InstrumentConfig {
    /// Instrument symbol in exchange-native format.
    pub instrument: String,
    /// Optional suffix to override the normalized instrument name in NNG
    /// topic names. When `Some(value)`, topics use `{kind}__{value}`
    /// verbatim (no normalization); when `None`/absent, topics use the
    /// normalized instrument `{kind}__{normalized}` as before.
    #[serde(default)]
    pub suffix_topic: Option<String>,
    /// Maximum number of price levels per side (None = no limit).
    #[serde(default)]
    pub max_level: Option<usize>,
    /// Maximum percentage from best price (0.0 or 100.0 = no limit; all
    /// levels kept).
    #[serde(default)]
    pub max_level_pct: f64,
}

/// Exchange WebSocket subscription settings.
///
/// Exchange-level fields (region, data kind, credentials, log gating,
/// resilience, fallback mappings) are shared by all instruments configured
/// under `[source.<exchange>.instrument.<alias>]`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SourceConfig {
    pub region: String,
    /// One of "lob", "trade", "both", "lob|trade".
    pub data_kind: String,
    /// When `true`, emit a warning log on Kraken CRC32 checksum mismatch
    /// (in addition to the always-set `checksum_failed` flag). When `false`
    /// (the default), a mismatch is only logged at the runtime `DEBUG` level.
    /// Gating this prevents an exchange feed from injecting log lines that
    /// interpolate an exchange-controlled checksum value.
    #[serde(default)]
    pub checksum_log: bool,
    /// When `true`, emit a warning log on Kraken crossing-guard rejection
    /// (an update whose price would cross the book: ask ≤ best bid or
    /// bid ≥ best ask, Kraken only). When `false` (the default), such
    /// rejections are only logged at the runtime `DEBUG` level. The
    /// crossing guard **always** drops the crossed level regardless of
    /// this setting — only the diagnostic `warn!` is gated. Gating prevents
    /// an exchange feed from generating noisy/spoofed log lines via the
    /// exchange-controlled update price; see
    /// [ADR-022](https://github.com/fibonsai/cryptomeria-ingest/blob/main/docs/adr/Operations/ADR-022-20260812-gate-crossing-guard-logging-prevent-log-spoofing.md).
    #[serde(default)]
    pub crossguard_log: bool,
    #[serde(default)]
    pub resilience: ResilienceConfig,
    /// Per-alias fallback mappings for this exchange, keyed by instrument
    /// alias. The alias key matches the key under
    /// `[source.<exchange>.instrument.<alias>]`. See `cryptomeria-ingest`
    /// README for details.
    #[serde(default)]
    pub fallback: HashMap<String, ExchangeFallbackMapping>,
    /// Optional API key for exchanges that require WebSocket authentication
    /// (e.g. Bitvavo). Ignored by exchanges that do not require credentials.
    #[serde(default)]
    pub api_key: Option<String>,
    /// Optional API secret for exchanges that require WebSocket authentication
    /// (e.g. Bitvavo). Ignored by exchanges that do not require credentials.
    #[serde(default)]
    pub api_secret: Option<String>,
    /// Per-instrument configs, keyed by alias. Each instrument is subscribed
    /// independently and gets its own `ValidatedSource`. The alias key also
    /// selects the matching `[source.<exchange>.fallback.<alias>]` mapping.
    ///
    /// In TOML this is `[source.<exchange>.instrument.<alias>]`.
    #[serde(default, rename = "instrument")]
    pub instruments: HashMap<String, InstrumentConfig>,
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

    /// Resolve API credentials for an exchange.
    ///
    /// Config values take precedence over environment variables. For
    /// `bitvavo`, when the config value is absent the method falls back to
    /// `BITVAVO_API_KEY` / `BITVAVO_API_SECRET` environment variables. For other
    /// exchanges, environment variables are not consulted and the config value
    /// (or `None`) is returned as-is.
    pub fn resolve_credentials(&self, exchange: &str) -> (Option<String>, Option<String>) {
        let key = self
            .api_key
            .clone()
            .or_else(|| env_credential(exchange, "BITVAVO_API_KEY"));
        let secret = self
            .api_secret
            .clone()
            .or_else(|| env_credential(exchange, "BITVAVO_API_SECRET"));
        (key, secret)
    }

    /// Convert to the ingest `DataSourceConfig`, validating exchange/region.
    ///
    /// `exchange` is the key from the parent `[source.<exchange>]` section.
    /// `alias` is the instrument's HashMap key (used for fallback lookup).
    /// `instrument_cfg` provides the per-instrument fields (`instrument`,
    /// `suffix_topic`, `max_level`, `max_level_pct`).
    ///
    /// When `alias` is an empty string, it is passed as `None` to the
    /// underlying `DataSourceConfig`, selecting the exchange-only fallback
    /// rule (`fallback[exchange][""]`).
    pub fn to_data_source(
        &self,
        exchange: &str,
        alias: &str,
        instrument_cfg: &InstrumentConfig,
    ) -> Result<DataSourceConfig, ConfigError> {
        let (api_key, api_secret) = self.resolve_credentials(exchange);
        let data_source = DataSourceConfig {
            exchange: exchange.to_string(),
            region: self.region.clone(),
            instrument: instrument_cfg.instrument.clone(),
            data_kind: self.data_kind()?,
            alias: if alias.is_empty() {
                None
            } else {
                Some(alias.to_string())
            },
            max_level: instrument_cfg.max_level,
            max_level_pct: instrument_cfg.max_level_pct,
            checksum_log: self.checksum_log,
            crossguard_log: self.crossguard_log,
            resilience: self.resilience.clone(),
            fallback: HashMap::from([(exchange.to_string(), self.fallback.clone())]),
            api_key,
            api_secret,
        };
        data_source
            .validate()
            .map_err(|e| ConfigError::InvalidSource(e.to_string()))?;
        Ok(data_source)
    }
}

/// Read a credential from an environment variable, but only for exchanges
/// that are known to require it (currently `bitvavo`). Empty strings are
/// treated as absent.
fn env_credential(exchange: &str, var: &str) -> Option<String> {
    if exchange == "bitvavo" {
        std::env::var(var).ok().filter(|s| !s.is_empty())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cryptomeria_ingest::CaseFallback;
    use std::sync::Mutex;

    // Serializes env-var-dependent tests so parallel runners don't collide.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    // Rust 2024 makes `env::set_var` / `env::remove_var` unsafe. These wrappers
    // keep the test bodies readable; callers must hold `ENV_LOCK`.
    fn set_env(name: &str, val: &str) {
        unsafe { std::env::set_var(name, val) }
    }

    fn remove_env(name: &str) {
        unsafe { std::env::remove_var(name) }
    }

    /// Helper: build a `SourceConfig` with a single instrument keyed by `alias`.
    fn source_with_instrument(alias: &str, instrument: &str) -> SourceConfig {
        SourceConfig {
            region: "global".into(),
            data_kind: "both".into(),
            instruments: HashMap::from([(
                alias.to_string(),
                InstrumentConfig {
                    instrument: instrument.to_string(),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        }
    }

    const VALID_TOML: &str = r#"
[source.okx]
region = "global"
data_kind = "both"

[source.okx.instrument.btcusd]
instrument = "BTC-USDT"

[nng]
port = 14242
"#;

    const MULTI_EXCHANGE_TOML: &str = r#"
[source.okx]
region = "global"
data_kind = "lob"

[source.okx.instrument.btcusd]
instrument = "BTC-USDT"

[source.kraken]
region = "global"
data_kind = "trade"

[source.kraken.instrument.btcusd]
instrument = "XBT/USD"

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
        let inst = source
            .instruments
            .get("btcusd")
            .expect("btcusd instrument should be present");
        assert_eq!(inst.instrument, "BTC-USDT");
        assert_eq!(source.data_kind, "both");
        assert_eq!(config.nng.port, 14242);
    }

    #[test]
    fn applies_defaults_for_optional_fields() {
        let config = parse_config(VALID_TOML).unwrap();
        let source = config.source.get("okx").unwrap();
        let inst = source.instruments.get("btcusd").unwrap();
        assert_eq!(inst.max_level, None);
        assert_eq!(inst.max_level_pct, 0.0);
        assert!(inst.suffix_topic.is_none());
        assert_eq!(config.nng.port, DEFAULT_NNG_PORT);
    }

    #[test]
    fn nng_port_defaults_to_14242_when_omitted() {
        let toml = r#"
[source.okx]
region = "global"
data_kind = "lob"

[source.okx.instrument.btcusd]
instrument = "BTC-USDT"

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
data_kind = "lob"

[source.okx.instrument.btcusd]
instrument = "BTC-USDT"

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
data_kind = "lob"

[source.okx.instrument.btcusd]
instrument = "BTC-USDT"

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
data_kind = "lob"

[source.okx.instrument.btcusd]
instrument = "BTC-USDT"

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
data_kind = "lob"

[source.okx.instrument.btcusd]
instrument = "BTC-USDT"

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
        let inst = source.instruments.get("btcusd").unwrap();
        let data_source = source.to_data_source("okx", "btcusd", inst).unwrap();
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
        let inst = source.instruments.get("btcusd").unwrap();
        let data_source = source.to_data_source("okx", "btcusd", inst).unwrap();
        assert_eq!(data_source.exchange, "okx");
        assert!(data_source.data_kind.contains(DataKind::LOB));
        assert!(data_source.data_kind.contains(DataKind::TRADE));
    }

    #[test]
    fn rejects_unknown_exchange_on_conversion() {
        let toml = r#"
[source.binance]
region = "global"
data_kind = "lob"

[source.binance.instrument.btcusd]
instrument = "BTCUSDT"

[nng]
port = 14242
"#;
        let config = parse_config(toml).unwrap();
        let source = config.source.get("binance").unwrap();
        let inst = source.instruments.get("btcusd").unwrap();
        let err = source
            .to_data_source("binance", "btcusd", inst)
            .unwrap_err();
        assert!(matches!(err, ConfigError::InvalidSource(_)));
    }

    #[test]
    fn parses_instrument_sections_keyed_by_alias() {
        let toml = r#"
[source.okx]
region = "global"
data_kind = "lob"

[source.okx.instrument.btcusd]
instrument = "btc/usdt"

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
        let inst = source
            .instruments
            .get("btcusd")
            .expect("instrument section should be present");
        assert_eq!(inst.instrument, "btc/usdt");
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
    fn applies_defaults_for_instruments_when_omitted() {
        let config = parse_config(VALID_TOML).unwrap();
        let source = config.source.get("okx").unwrap();
        // instruments has one entry (btcusd) from VALID_TOML
        assert_eq!(source.instruments.len(), 1);
        assert!(source.fallback.is_empty());
    }

    #[test]
    fn suffix_topic_defaults_to_none_when_omitted() {
        let config = parse_config(VALID_TOML).unwrap();
        let source = config.source.get("okx").unwrap();
        let inst = source.instruments.get("btcusd").unwrap();
        assert!(inst.suffix_topic.is_none());
    }

    #[test]
    fn parses_suffix_topic_when_present() {
        let toml = r#"
[source.okx]
region = "global"
data_kind = "both"

[source.okx.instrument.btcusd]
instrument = "BTC-USDT"
suffix_topic = "mytopic"

[nng]
port = 14242
"#;
        let config = parse_config(toml).unwrap();
        let source = config.source.get("okx").unwrap();
        let inst = source.instruments.get("btcusd").unwrap();
        assert_eq!(inst.suffix_topic.as_deref(), Some("mytopic"));
    }

    #[test]
    fn validated_sources_includes_suffix_topic() {
        let toml = r#"
[source.okx]
region = "global"
data_kind = "both"

[source.okx.instrument.btcusd]
instrument = "BTC-USDT"
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
data_kind = "lob"

[source.okx.instrument.btcusd]
instrument = "btc/usdt"

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
        let inst = source.instruments.get("btcusd").unwrap();
        let data_source = source.to_data_source("okx", "btcusd", inst).unwrap();
        assert_eq!(data_source.alias.as_deref(), Some("btcusd"));
        let mapping = data_source
            .fallback
            .get("okx")
            .and_then(|a| a.get("btcusd"))
            .expect("fallback should be forwarded");
        assert_eq!(mapping.base_mappings, vec!["BTC"]);
    }

    #[test]
    fn to_data_source_uses_empty_alias_as_none() {
        let source = source_with_instrument("", "BTC-USDT");
        let inst = source.instruments.get("").unwrap();
        let data_source = source.to_data_source("okx", "", inst).unwrap();
        assert_eq!(data_source.alias, None);
    }

    #[test]
    fn validated_sources_builds_one_source_per_instrument() {
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
    fn validated_sources_builds_multiple_instruments_per_exchange() {
        let toml = r#"
[source.okx]
region = "global"
data_kind = "both"

[source.okx.instrument.btcusd]
instrument = "BTC-USDT"

[source.okx.instrument.ethusd]
instrument = "ETH-USDT"

[nng]
port = 14242
"#;
        let config = parse_config(toml).unwrap();
        let sources = config.validated_sources().unwrap();
        assert_eq!(sources.len(), 2);
        // Sorted by alias: "btcusd" < "ethusd"
        let instruments: Vec<&str> = sources.iter().map(|(_, i, _, _)| i.as_str()).collect();
        assert_eq!(instruments, vec!["BTC-USDT", "ETH-USDT"]);
    }

    #[test]
    fn validated_sources_errors_on_unknown_exchange() {
        let toml = r#"
[source.binance]
region = "global"
data_kind = "lob"

[source.binance.instrument.btcusd]
instrument = "BTCUSDT"

[nng]
port = 14242
"#;
        let config = parse_config(toml).unwrap();
        let result = config.validated_sources();
        assert!(result.is_err());
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
    fn override_silence_timeout_secs_sets_value_on_all_sources() {
        let toml = r#"
[source.okx]
region = "global"
data_kind = "lob"

[source.okx.instrument.btcusd]
instrument = "BTC-USDT"

[source.kraken]
region = "global"
data_kind = "trade"

[source.kraken.instrument.btcusd]
instrument = "XBT/USD"

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
data_kind = "lob"

[source.okx.instrument.btcusd]
instrument = "BTC-USDT"

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

    #[test]
    fn parses_api_key_and_api_secret_from_toml() {
        let toml = r#"
[source.bitvavo]
region = "global"
data_kind = "both"
api_key = "my-key"
api_secret = "my-secret"

[source.bitvavo.instrument.btcusd]
instrument = "BTC-EUR"

[nng]
port = 14242
"#;
        let config = parse_config(toml).unwrap();
        let source = config.source.get("bitvavo").unwrap();
        assert_eq!(source.api_key.as_deref(), Some("my-key"));
        assert_eq!(source.api_secret.as_deref(), Some("my-secret"));
    }

    #[test]
    fn api_key_and_api_secret_default_to_none_when_omitted() {
        let config = parse_config(VALID_TOML).unwrap();
        let source = config.source.get("okx").unwrap();
        assert_eq!(source.api_key, None);
        assert_eq!(source.api_secret, None);
    }

    #[test]
    fn resolve_credentials_returns_config_value_when_present() {
        let source = SourceConfig {
            region: "global".into(),
            data_kind: "both".into(),
            api_key: Some("config-key".into()),
            api_secret: Some("config-secret".into()),
            ..Default::default()
        };
        let (key, secret) = source.resolve_credentials("bitvavo");
        assert_eq!(key, Some("config-key".into()));
        assert_eq!(secret, Some("config-secret".into()));
    }

    #[test]
    fn resolve_credentials_falls_back_to_env_for_bitvavo() {
        let _guard = ENV_LOCK.lock().unwrap();
        let source = SourceConfig {
            region: "global".into(),
            data_kind: "both".into(),
            ..Default::default()
        };
        set_env("BITVAVO_API_KEY", "env-key");
        set_env("BITVAVO_API_SECRET", "env-secret");
        let (key, secret) = source.resolve_credentials("bitvavo");
        remove_env("BITVAVO_API_KEY");
        remove_env("BITVAVO_API_SECRET");
        assert_eq!(key, Some("env-key".into()));
        assert_eq!(secret, Some("env-secret".into()));
    }

    #[test]
    fn resolve_credentials_returns_none_when_no_config_or_env() {
        let _guard = ENV_LOCK.lock().unwrap();
        remove_env("BITVAVO_API_KEY");
        remove_env("BITVAVO_API_SECRET");
        let source = SourceConfig {
            region: "global".into(),
            data_kind: "both".into(),
            ..Default::default()
        };
        let (key, secret) = source.resolve_credentials("bitvavo");
        assert_eq!(key, None);
        assert_eq!(secret, None);
    }

    #[test]
    fn resolve_credentials_ignores_env_for_non_bitvavo() {
        let _guard = ENV_LOCK.lock().unwrap();
        set_env("BITVAVO_API_KEY", "should-not-apply");
        set_env("BITVAVO_API_SECRET", "should-not-apply");
        let source = SourceConfig {
            region: "global".into(),
            data_kind: "both".into(),
            ..Default::default()
        };
        let (key, secret) = source.resolve_credentials("okx");
        remove_env("BITVAVO_API_KEY");
        remove_env("BITVAVO_API_SECRET");
        assert_eq!(key, None);
        assert_eq!(secret, None);
    }

    #[test]
    fn resolve_credentials_config_takes_precedence_over_env() {
        let _guard = ENV_LOCK.lock().unwrap();
        set_env("BITVAVO_API_KEY", "env-key");
        set_env("BITVAVO_API_SECRET", "env-secret");
        let source = SourceConfig {
            region: "global".into(),
            data_kind: "both".into(),
            api_key: Some("config-key".into()),
            api_secret: Some("config-secret".into()),
            ..Default::default()
        };
        let (key, secret) = source.resolve_credentials("bitvavo");
        remove_env("BITVAVO_API_KEY");
        remove_env("BITVAVO_API_SECRET");
        assert_eq!(key, Some("config-key".into()));
        assert_eq!(secret, Some("config-secret".into()));
    }

    #[test]
    fn to_data_source_forwards_api_credentials() {
        let mut source = source_with_instrument("btcusd", "BTC-EUR");
        source.api_key = Some("my-key".into());
        source.api_secret = Some("my-secret".into());
        let inst = source.instruments.get("btcusd").unwrap();
        let data_source = source.to_data_source("bitvavo", "btcusd", inst).unwrap();
        assert_eq!(data_source.api_key, Some("my-key".into()));
        assert_eq!(data_source.api_secret, Some("my-secret".into()));
    }

    #[test]
    fn to_data_source_sets_credentials_none_for_okx() {
        let source = source_with_instrument("btcusd", "BTC-USDT");
        let source = SourceConfig {
            region: "global".into(),
            data_kind: "lob".into(),
            ..source
        };
        let inst = source.instruments.get("btcusd").unwrap();
        let data_source = source.to_data_source("okx", "btcusd", inst).unwrap();
        assert_eq!(data_source.api_key, None);
        assert_eq!(data_source.api_secret, None);
    }

    #[test]
    fn bitvavo_without_credentials_fails_validation() {
        let _guard = ENV_LOCK.lock().unwrap();
        remove_env("BITVAVO_API_KEY");
        remove_env("BITVAVO_API_SECRET");
        let source = source_with_instrument("btcusd", "BTC-EUR");
        let inst = source.instruments.get("btcusd").unwrap();
        let result = source.to_data_source("bitvavo", "btcusd", inst);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("bitvavo requires api_key and api_secret")
        );
    }

    #[test]
    fn bitvavo_with_credentials_passes_validation() {
        let mut source = source_with_instrument("btcusd", "BTC-EUR");
        source.api_key = Some("my-key".into());
        source.api_secret = Some("my-secret".into());
        let inst = source.instruments.get("btcusd").unwrap();
        let result = source.to_data_source("bitvavo", "btcusd", inst);
        assert!(result.is_ok());
    }

    #[test]
    fn bitvavo_with_env_credentials_passes_validation() {
        let _guard = ENV_LOCK.lock().unwrap();
        set_env("BITVAVO_API_KEY", "env-key");
        set_env("BITVAVO_API_SECRET", "env-secret");
        let source = source_with_instrument("btcusd", "BTC-EUR");
        let inst = source.instruments.get("btcusd").unwrap();
        let result = source.to_data_source("bitvavo", "btcusd", inst);
        remove_env("BITVAVO_API_KEY");
        remove_env("BITVAVO_API_SECRET");
        assert!(result.is_ok());
        let ds = result.unwrap();
        assert_eq!(ds.api_key, Some("env-key".into()));
        assert_eq!(ds.api_secret, Some("env-secret".into()));
    }

    #[test]
    fn parses_bitvavo_config_from_toml() {
        let toml = r#"
[source.bitvavo]
region = "global"
data_kind = "both"
api_key = "toml-key"
api_secret = "toml-secret"

[source.bitvavo.instrument.btcusd]
instrument = "BTC-EUR"
suffix_topic = "btceur"

[nng]
port = 14242
"#;
        let config = parse_config(toml).unwrap();
        let sources = config.validated_sources().unwrap();
        assert_eq!(sources.len(), 1);
        let (_, _, data_source, _) = &sources[0];
        assert_eq!(data_source.exchange, "bitvavo");
        assert_eq!(data_source.api_key, Some("toml-key".into()));
        assert_eq!(data_source.api_secret, Some("toml-secret".into()));
    }

    #[test]
    fn checksum_log_defaults_to_false_when_omitted() {
        let config = parse_config(VALID_TOML).unwrap();
        let source = config.source.get("okx").unwrap();
        assert!(!source.checksum_log, "checksum_log must default to false");
    }

    #[test]
    fn parses_checksum_log_when_present() {
        let toml = r#"
[source.kraken]
region = "global"
data_kind = "both"
checksum_log = true

[source.kraken.instrument.btcusd]
instrument = "btcusd"
suffix_topic = "btcusd"
max_level = 3

[nng]
port = 14242
"#;
        let config = parse_config(toml).unwrap();
        let source = config.source.get("kraken").unwrap();
        assert!(
            source.checksum_log,
            "checksum_log must parse as true from TOML"
        );
    }

    #[test]
    fn to_data_source_forwards_checksum_log() {
        let toml = r#"
[source.kraken]
region = "global"
data_kind = "both"
checksum_log = true

[source.kraken.instrument.btcusd]
instrument = "btcusd"
suffix_topic = "btcusd"
max_level = 3

[nng]
port = 14242
"#;
        let config = parse_config(toml).unwrap();
        let sources = config.validated_sources().unwrap();
        let kraken = sources
            .iter()
            .find(|(e, _, _, _)| *e == "kraken")
            .expect("kraken should be present");
        assert!(
            kraken.2.checksum_log,
            "checksum_log must be forwarded to DataSourceConfig"
        );
    }

    #[test]
    fn to_data_source_checksum_log_defaults_to_false_when_omitted() {
        let config = parse_config(VALID_TOML).unwrap();
        let sources = config.validated_sources().unwrap();
        let okx = sources
            .iter()
            .find(|(e, _, _, _)| *e == "okx")
            .expect("okx should be present");
        assert!(
            !okx.2.checksum_log,
            "checksum_log must default to false in DataSourceConfig when omitted"
        );
    }

    #[test]
    fn crossguard_log_defaults_to_false_when_omitted() {
        let config = parse_config(VALID_TOML).unwrap();
        let source = config.source.get("okx").unwrap();
        assert!(
            !source.crossguard_log,
            "crossguard_log must default to false"
        );
    }

    #[test]
    fn parses_crossguard_log_when_present() {
        let toml = r#"
[source.kraken]
region = "global"
data_kind = "both"
crossguard_log = true

[source.kraken.instrument.btcusd]
instrument = "btcusd"
suffix_topic = "btcusd"
max_level = 3

[nng]
port = 14242
"#;
        let config = parse_config(toml).unwrap();
        let source = config.source.get("kraken").unwrap();
        assert!(
            source.crossguard_log,
            "crossguard_log must parse as true from TOML"
        );
    }

    #[test]
    fn to_data_source_forwards_crossguard_log() {
        let toml = r#"
[source.kraken]
region = "global"
data_kind = "both"
crossguard_log = true

[source.kraken.instrument.btcusd]
instrument = "btcusd"
suffix_topic = "btcusd"
max_level = 3

[nng]
port = 14242
"#;
        let config = parse_config(toml).unwrap();
        let sources = config.validated_sources().unwrap();
        let kraken = sources
            .iter()
            .find(|(e, _, _, _)| *e == "kraken")
            .expect("kraken should be present");
        assert!(
            kraken.2.crossguard_log,
            "crossguard_log must be forwarded to DataSourceConfig"
        );
    }

    #[test]
    fn to_data_source_crossguard_log_defaults_to_false_when_omitted() {
        let config = parse_config(VALID_TOML).unwrap();
        let sources = config.validated_sources().unwrap();
        let okx = sources
            .iter()
            .find(|(e, _, _, _)| *e == "okx")
            .expect("okx should be present");
        assert!(
            !okx.2.crossguard_log,
            "crossguard_log must default to false in DataSourceConfig when omitted"
        );
    }

    #[test]
    fn multiple_instruments_share_exchange_level_fallback() {
        let toml = r#"
[source.okx]
region = "global"
data_kind = "lob"

[source.okx.fallback.btcusd]
base_mappings = ["BTC", "XBT"]
quote_mappings = ["USDT", "USD"]
separator_mappings = ["-", "/"]
case_fallback = "upper"

[source.okx.fallback.ethusd]
base_mappings = ["ETH"]
quote_mappings = ["USDT"]
separator_mappings = ["-"]
case_fallback = "upper"

[source.okx.instrument.btcusd]
instrument = "btc/usdt"

[source.okx.instrument.ethusd]
instrument = "eth/usdt"

[nng]
port = 14242
"#;
        let config = parse_config(toml).unwrap();
        let sources = config.validated_sources().unwrap();
        assert_eq!(sources.len(), 2);

        // BTC instrument uses the btcusd fallback
        let btc = sources
            .iter()
            .find(|(_, i, _, _)| *i == "btc/usdt")
            .expect("btc instrument should be present");
        assert_eq!(btc.2.alias.as_deref(), Some("btcusd"));
        let btc_fallback = btc
            .2
            .fallback
            .get("okx")
            .and_then(|a| a.get("btcusd"))
            .expect("btcusd fallback should be forwarded");
        assert_eq!(btc_fallback.base_mappings, vec!["BTC", "XBT"]);

        // ETH instrument uses the ethusd fallback
        let eth = sources
            .iter()
            .find(|(_, i, _, _)| *i == "eth/usdt")
            .expect("eth instrument should be present");
        assert_eq!(eth.2.alias.as_deref(), Some("ethusd"));
        let eth_fallback = eth
            .2
            .fallback
            .get("okx")
            .and_then(|a| a.get("ethusd"))
            .expect("ethusd fallback should be forwarded");
        assert_eq!(eth_fallback.base_mappings, vec!["ETH"]);
    }

    #[test]
    fn to_data_source_forward_max_level_and_max_level_pct() {
        let toml = r#"
[source.okx]
region = "global"
data_kind = "both"

[source.okx.instrument.btcusd]
instrument = "BTC-USDT"
max_level = 5
max_level_pct = 50.0

[nng]
port = 14242
"#;
        let config = parse_config(toml).unwrap();
        let source = config.source.get("okx").unwrap();
        let inst = source.instruments.get("btcusd").unwrap();
        let data_source = source.to_data_source("okx", "btcusd", inst).unwrap();
        assert_eq!(data_source.max_level, Some(5));
        assert_eq!(data_source.max_level_pct, 50.0);
    }
}
