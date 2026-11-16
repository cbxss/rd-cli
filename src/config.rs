use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Config {
    pub api_token: Option<String>,
}

fn project_dirs() -> Result<ProjectDirs> {
    ProjectDirs::from("", "", "rd").context("could not determine home directory")
}

/// `RD_CONFIG_DIR` overrides where `config.toml` lives.
fn config_path() -> Result<PathBuf> {
    if let Some(dir) = std::env::var_os("RD_CONFIG_DIR").filter(|v| !v.is_empty()) {
        return Ok(PathBuf::from(dir).join("config.toml"));
    }
    let dirs = project_dirs()?;
    Ok(dirs.config_dir().join("config.toml"))
}

fn env_token() -> Option<String> {
    std::env::var("RD_TOKEN")
        .ok()
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
}

impl Config {
    /// Loads the saved config. A non-empty `RD_TOKEN` environment variable
    /// takes precedence over the saved token — and then a damaged config file
    /// doesn't stand in the way.
    pub fn load() -> Result<Self> {
        if let Some(token) = env_token() {
            let mut config = Self::load_file().unwrap_or_default();
            config.api_token = Some(token);
            return Ok(config);
        }
        Self::load_file()
    }

    /// Loads only what's saved on disk, ignoring `RD_TOKEN` (for login/logout,
    /// which edit the saved file).
    pub fn load_file() -> Result<Self> {
        let path = config_path()?;
        if !path.exists() {
            return Ok(Config::default());
        }
        let contents = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let config: Config = toml::from_str(&contents).with_context(|| {
            format!(
                "failed to parse {} (fix or delete it, or run `rd auth login` to replace it)",
                path.display()
            )
        })?;
        Ok(config)
    }

    /// Writes the config atomically. On Unix the file is created `0600` (and
    /// its directory `0700`), so the token is never readable by others, not
    /// even briefly.
    pub fn save(&self) -> Result<()> {
        let path = config_path()?;
        let dir = path
            .parent()
            .context("config path has no parent directory")?;
        create_private_dir(dir)?;
        let contents = toml::to_string_pretty(self)?;

        let tmp = path.with_extension("toml.tmp");
        {
            let mut opts = fs::OpenOptions::new();
            opts.write(true).create(true).truncate(true);
            #[cfg(unix)]
            std::os::unix::fs::OpenOptionsExt::mode(&mut opts, 0o600);
            let mut file = opts
                .open(&tmp)
                .with_context(|| format!("failed to write {}", tmp.display()))?;
            std::io::Write::write_all(&mut file, contents.as_bytes())?;
            file.sync_all()?;
        }
        fs::rename(&tmp, &path).with_context(|| format!("failed to write {}", path.display()))?;
        Ok(())
    }

    /// Removes the saved token. A config file that can't be parsed is
    /// removed outright, since it's no use to anyone.
    pub fn clear_saved_token() -> Result<bool> {
        match Self::load_file() {
            Ok(mut config) => {
                let had = config.api_token.take().is_some();
                if had {
                    config.save()?;
                }
                Ok(had)
            }
            Err(_) => {
                let path = config_path()?;
                fs::remove_file(&path)
                    .with_context(|| format!("failed to remove {}", path.display()))?;
                Ok(true)
            }
        }
    }
}

fn create_private_dir(dir: &std::path::Path) -> Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder
        .create(dir)
        .with_context(|| format!("failed to create {}", dir.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    // One test touches the process environment, so keep it in one function.
    #[test]
    fn save_load_and_damaged_files() {
        let dir = std::env::temp_dir().join(format!("rd-config-test-{}", std::process::id()));
        // SAFETY: no other test in this crate reads these variables.
        unsafe {
            std::env::set_var("RD_CONFIG_DIR", &dir);
            std::env::remove_var("RD_TOKEN");
        }

        Config {
            api_token: Some("abc".into()),
        }
        .save()
        .unwrap();
        assert_eq!(Config::load().unwrap().api_token.as_deref(), Some("abc"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(dir.join("config.toml"))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }

        fs::write(dir.join("config.toml"), "not = [valid").unwrap();
        assert!(Config::load().is_err());
        unsafe { std::env::set_var("RD_TOKEN", " from-env \n") };
        assert_eq!(
            Config::load().unwrap().api_token.as_deref(),
            Some("from-env")
        );
        assert!(Config::clear_saved_token().unwrap());
        assert!(!dir.join("config.toml").exists());

        unsafe {
            std::env::remove_var("RD_TOKEN");
            std::env::remove_var("RD_CONFIG_DIR");
        }
        fs::remove_dir_all(&dir).unwrap();
    }
}
