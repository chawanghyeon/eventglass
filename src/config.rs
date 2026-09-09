use std::{net::SocketAddr, path::PathBuf};

use anyhow::{Context, Result, bail};
use url::Url;

#[derive(Debug, Clone)]
pub struct Config {
    pub addr: SocketAddr,
    pub data_dir: PathBuf,
    pub base_url: Url,
    pub s3_url: Option<String>,
    pub s3_endpoint: Option<Url>,
    pub s3_initialize: bool,
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
            s3_endpoint: std::env::var("EVENTGLASS_S3_ENDPOINT")
                .ok()
                .map(|value| Url::parse(&value))
                .transpose()
                .context("EVENTGLASS_S3_ENDPOINT must be a URL")?,
            s3_initialize: match std::env::var("EVENTGLASS_S3_INITIALIZE").as_deref() {
                Err(std::env::VarError::NotPresent) | Ok("0" | "false") => false,
                Ok("1" | "true") => true,
                Ok(_) | Err(std::env::VarError::NotUnicode(_)) => {
                    bail!("EVENTGLASS_S3_INITIALIZE must be true, false, 1, or 0")
                }
            },
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if let Some(location) = &self.s3_url {
            crate::storage::s3::S3Location::parse(location)?;
        }
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
        if let Some(endpoint) = &self.s3_endpoint {
            if !matches!(endpoint.scheme(), "http" | "https")
                || endpoint.host_str().is_none()
                || !endpoint.username().is_empty()
                || endpoint.password().is_some()
                || endpoint.query().is_some()
                || endpoint.fragment().is_some()
            {
                bail!("EVENTGLASS_S3_ENDPOINT must be an HTTP(S) URL without credentials");
            }
            if self.s3_url.is_none() {
                bail!("EVENTGLASS_S3_ENDPOINT requires EVENTGLASS_S3_URL");
            }
            if endpoint.scheme() == "http" {
                let local = match endpoint.host() {
                    Some(url::Host::Domain(name)) => name == "localhost",
                    Some(url::Host::Ipv4(address)) => address.is_loopback(),
                    Some(url::Host::Ipv6(address)) => address.is_loopback(),
                    None => false,
                };
                if !local {
                    bail!("HTTP S3 endpoints are limited to loopback compatibility tests");
                }
            }
        }
        if self.s3_initialize && self.s3_url.is_none() {
            bail!("EVENTGLASS_S3_INITIALIZE requires EVENTGLASS_S3_URL");
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
            ("https://example.test/?query=value", false),
            ("https://example.test/#fragment", false),
        ] {
            let config = Config {
                addr: "127.0.0.1:0".parse().unwrap(),
                data_dir: PathBuf::from("unused"),
                base_url: origin.parse().unwrap(),
                s3_url: None,
                s3_endpoint: None,
                s3_initialize: false,
            };
            assert_eq!(config.validate().is_ok(), accepted, "{origin}");
        }
        let credentialed = format!("https://user:{}@example.test", "password");
        let config = Config {
            addr: "127.0.0.1:0".parse().unwrap(),
            data_dir: PathBuf::from("unused"),
            base_url: credentialed.parse().unwrap(),
            s3_url: None,
            s3_endpoint: None,
            s3_initialize: false,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn s3_namespace_and_compatibility_endpoint_are_validated_separately() {
        let base = Config {
            addr: "127.0.0.1:0".parse().unwrap(),
            data_dir: PathBuf::from("unused"),
            base_url: "https://eventglass.example.test".parse().unwrap(),
            s3_url: Some("s3://eventglass/tenant".into()),
            s3_endpoint: None,
            s3_initialize: false,
        };
        assert!(base.validate().is_ok());
        assert!(
            Config {
                s3_url: Some("https://eventglass/tenant".into()),
                ..base.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            Config {
                s3_endpoint: Some("http://127.0.0.1:9000".parse().unwrap()),
                ..base.clone()
            }
            .validate()
            .is_ok()
        );
        assert!(
            Config {
                s3_endpoint: Some("http://minio.example.test".parse().unwrap()),
                ..base
            }
            .validate()
            .is_err()
        );

        for endpoint in [
            "ftp://127.0.0.1",
            "https://user@example.test",
            "https://example.test/?query=value",
            "https://example.test/#fragment",
        ] {
            assert!(
                Config {
                    addr: "127.0.0.1:0".parse().unwrap(),
                    data_dir: PathBuf::from("unused"),
                    base_url: "https://eventglass.example.test".parse().unwrap(),
                    s3_url: Some("s3://eventglass/tenant".into()),
                    s3_endpoint: Some(endpoint.parse().unwrap()),
                    s3_initialize: false,
                }
                .validate()
                .is_err(),
                "{endpoint}"
            );
        }

        let credentialed = format!("https://user:{}@example.test", "password");
        assert!(
            Config {
                addr: "127.0.0.1:0".parse().unwrap(),
                data_dir: PathBuf::from("unused"),
                base_url: "https://eventglass.example.test".parse().unwrap(),
                s3_url: Some("s3://eventglass/tenant".into()),
                s3_endpoint: Some(credentialed.parse().unwrap()),
                s3_initialize: false,
            }
            .validate()
            .is_err()
        );

        assert!(
            Config {
                addr: "127.0.0.1:0".parse().unwrap(),
                data_dir: PathBuf::from("unused"),
                base_url: "https://eventglass.example.test".parse().unwrap(),
                s3_url: None,
                s3_endpoint: Some("https://example.test".parse().unwrap()),
                s3_initialize: false,
            }
            .validate()
            .is_err()
        );
        assert!(
            Config {
                addr: "127.0.0.1:0".parse().unwrap(),
                data_dir: PathBuf::from("unused"),
                base_url: "https://eventglass.example.test".parse().unwrap(),
                s3_url: None,
                s3_endpoint: None,
                s3_initialize: true,
            }
            .validate()
            .is_err()
        );
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
