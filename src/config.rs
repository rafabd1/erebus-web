use std::fs::{self, OpenOptions};
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, bail};
use clap::Parser;
use rand::RngCore;

#[derive(Clone, Debug, Parser)]
#[command(name = "erebus-webd")]
pub struct Config {
    #[arg(long, env = "EREBUS_WEB_DATA_DIR", default_value = "data")]
    pub data_dir: PathBuf,

    #[arg(long, env = "EREBUS_WEB_BIND", default_value = "127.0.0.1:0")]
    pub bind: SocketAddr,

    #[arg(long, env = "EREBUS_WEB_TOKEN")]
    pub token: Option<String>,
}

impl Config {
    pub fn prepare(mut self) -> anyhow::Result<Self> {
        if !self.bind.ip().is_loopback() {
            bail!("the local API may only bind to a loopback address");
        }
        fs::create_dir_all(&self.data_dir)
            .with_context(|| format!("failed to create {}", self.data_dir.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(&self.data_dir, fs::Permissions::from_mode(0o700))
                .with_context(|| format!("failed to protect {}", self.data_dir.display()))?;
        }
        self.data_dir = self
            .data_dir
            .canonicalize()
            .context("failed to resolve data directory")?;

        if self.token.as_deref().is_none_or(str::is_empty) {
            self.token = Some(load_or_create_token(&self.data_dir)?);
        }
        Ok(self)
    }

    pub fn token(&self) -> &str {
        self.token
            .as_deref()
            .expect("prepared config always has a token")
    }
}

fn load_or_create_token(data_dir: &std::path::Path) -> anyhow::Result<String> {
    let path = data_dir.join("api-token");
    match fs::read_to_string(&path) {
        Ok(value) if !value.trim().is_empty() => return Ok(value.trim().to_owned()),
        Ok(_) => bail!("{} contains an empty API token", path.display()),
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
            return Err(error).with_context(|| format!("failed to read {}", path.display()));
        }
        Err(_) => {}
    }

    let mut bytes = [0_u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    let token = blake3::hash(&bytes).to_hex().to_string();
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .with_context(|| format!("failed to create {}", path.display()))?;
    file.write_all(token.as_bytes())?;
    file.sync_all()?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(token)
}
