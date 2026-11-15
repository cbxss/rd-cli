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

impl Config {
    /// Loads the saved config. A non-empty `RD_TOKEN` environment variable
    /// takes precedence over the saved token.
    pub fn load() -> Result<Self> {
        let mut config = Self::load_file()?;
        if let Ok(token) = std::env::var("RD_TOKEN") {
            if !token.is_empty() {
                config.api_token = Some(token);
            }
        }
        Ok(config)
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
        let config: Config = toml::from_str(&contents)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        Ok(config)
    }

    pub fn save(&self) -> Result<()> {
        let path = config_path()?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let contents = toml::to_string_pretty(self)?;
        fs::write(&path, contents)
            .with_context(|| format!("failed to write {}", path.display()))?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                .with_context(|| format!("failed to set permissions on {}", path.display()))?;
        }

        Ok(())
    }

    pub fn clear(&mut self) -> Result<()> {
        self.api_token = None;
        self.save()
    }
}
