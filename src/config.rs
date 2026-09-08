use std::{net::SocketAddr, path::PathBuf};

use anyhow::{Context, Result, bail};
use url::Url;

#[derive(Debug, Clone)]
pub struct Config {
    pub addr: SocketAddr,
    pub data_dir: PathBuf,
    pub base_url: Url,
    pub s3_url: Option<String>,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let config = Self {
            addr: std::env::var("EVENTGLASS_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:8080".into())
                .parse()
                .context("EVENTGLASS_ADDR must be a socket address")?,
            data_dir: std::env::var_os("EVENTGLASS_DATA_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("data")),
            base_url: Url::parse(
                &std::env::var("EVENTGLASS_BASE_URL")
                    .unwrap_or_else(|_| "http://127.0.0.1:8080".into()),
            )?,
            s3_url: std::env::var("EVENTGLASS_S3_URL").ok(),
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if !matches!(self.base_url.scheme(), "http" | "https")
            || self.base_url.host_str().is_none()
            || !self.base_url.username().is_empty()
            || self.base_url.password().is_some()
            || self.base_url.query().is_some()
            || self.base_url.fragment().is_some()
            || self.base_url.path() != "/"
        {
            bail!("EVENTGLASS_BASE_URL must be an HTTP(S) origin without credentials or path");
        }
        if self.base_url.scheme() == "http" {
            let local = match self.base_url.host() {
                Some(url::Host::Domain(name)) => name == "localhost",
                Some(url::Host::Ipv4(address)) => address.is_loopback(),
                Some(url::Host::Ipv6(address)) => address.is_loopback(),
                None => false,
            };
            if !local {
                bail!(
                    "HTTP is allowed only for a loopback base URL; configure HTTPS for remote access"
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insecure_session_origins_are_limited_to_loopback() {
        for (origin, accepted) in [
            ("http://localhost:8080", true),
            ("http://127.0.0.2", true),
            ("http://[::1]", true),
            ("http://localhost.example.test", false),
            ("http://192.168.1.1", false),
            ("https://eventglass.example.test", true),
            ("https://user@example.test", false),
            ("https://example.test/path", false),
        ] {
            let config = Config {
                addr: "127.0.0.1:0".parse().unwrap(),
                data_dir: PathBuf::from("unused"),
                base_url: origin.parse().unwrap(),
                s3_url: None,
            };
            assert_eq!(config.validate().is_ok(), accepted, "{origin}");
        }
    }
}

#[derive(Debug, Clone)]
pub struct Limits {
    pub wire_bytes: usize,
    pub decoded_bytes: usize,
    pub record_bytes: usize,
    pub request_records: usize,
    pub ingress_bytes: usize,
    pub chunk_bytes: usize,
    pub chunk_records: usize,
    pub batch_bytes: usize,
    pub batch_records: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            wire_bytes: 20 * 1024 * 1024,
            decoded_bytes: 20 * 1024 * 1024,
            record_bytes: 1024 * 1024,
            request_records: 10_000,
            ingress_bytes: 64 * 1024 * 1024,
            chunk_bytes: 512 * 1024,
            chunk_records: 128,
            batch_bytes: 4 * 1024 * 1024,
            batch_records: 1_000,
        }
    }
}
