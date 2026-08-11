//! Environment file loading.
//!
//! Loads variables from a `.env` file at application startup so that secrets
//! and runtime configuration can be provided without hardcoding them in
//! `config.toml` or requiring explicit shell exports.
//!
//! Loading is best-effort: a missing `.env` file is logged at `debug` level
//! and treated as a no-op.

use std::path::Path;

/// Load environment variables from a `.env` file in the current directory.
///
/// Best-effort: a missing file is logged at `debug` level and treated as a
/// no-op. A present file logs at `info` level. Never panics.
pub fn load_env() {
    load_env_from(Path::new(".env"));
}

/// Load environment variables from the given file path.
///
/// Best-effort: a missing file is logged at `debug` level and treated as a
/// no-op. A present file logs at `info` level. Never panics.
pub fn load_env_from<P: AsRef<Path>>(path: P) {
    let path = path.as_ref();
    match dotenvy::from_path(path) {
        Ok(()) => log::info!("Loaded environment variables from {}", path.display()),
        Err(e) if e.not_found() => {
            log::debug!(
                "No {} file found; relying on existing environment variables",
                path.display()
            )
        }
        Err(e) => {
            log::debug!("Failed to load {}: {}", path.display(), e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn load_env_from_populates_env_vars_from_present_file() {
        let _guard = ENV_LOCK.lock().unwrap();

        let dir = tempfile::tempdir().expect("create temp dir");
        let env_path = dir.path().join(".env");
        std::fs::write(&env_path, "TEST_LOAD_ENV_FROM=success").expect("write .env");

        load_env_from(&env_path);

        assert_eq!(
            std::env::var("TEST_LOAD_ENV_FROM").as_deref(),
            Ok("success")
        );

        unsafe { std::env::remove_var("TEST_LOAD_ENV_FROM") };
    }

    #[test]
    fn load_env_from_does_not_panic_when_file_missing() {
        let _guard = ENV_LOCK.lock().unwrap();

        let dir = tempfile::tempdir().expect("create temp dir");
        let missing_path = dir.path().join("nonexistent.env");

        load_env_from(&missing_path);
    }
}
