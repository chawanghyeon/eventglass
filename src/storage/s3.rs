//! Validated S3 namespace and the narrow object operations used by archive recovery.

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
#[cfg(feature = "s3")]
use aws_sdk_s3::{Client, primitives::ByteStream};
#[cfg(feature = "s3")]
use base64::Engine;
#[cfg(feature = "s3")]
use sha2::{Digest, Sha256};
use std::path::{Component, Path};
#[cfg(feature = "s3")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Location {
    bucket: String,
    prefix: String,
}

impl S3Location {
    pub fn parse(value: &str) -> Result<Self> {
        let url = Url::parse(value).context("EVENTGLASS_S3_URL must be a URL")?;
        ensure!(
            url.scheme() == "s3"
                && url.host_str().is_some()
                && url.port().is_none()
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none(),
            "EVENTGLASS_S3_URL must be s3://bucket/prefix"
        );
        ensure!(
            !url.path().contains('%') && !url.path().contains('\\') && !url.path().contains("//"),
            "encoded or unsafe S3 prefix"
        );
        let prefix = url.path().trim_matches('/');
        ensure!(
            Path::new(prefix)
                .components()
                .all(|component| matches!(component, Component::Normal(_))),
            "unsafe S3 prefix"
        );
        Ok(Self {
            bucket: url.host_str().unwrap().to_owned(),
            prefix: prefix.to_owned(),
        })
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn key(&self, relative: &str) -> Result<String> {
        ensure!(
            !relative.is_empty()
                && !relative.contains('%')
                && !relative.contains('\\')
                && !relative.contains("//")
                && Path::new(relative)
                    .components()
                    .all(|component| matches!(component, Component::Normal(_))),
            "unsafe S3 object key"
        );
        Ok(if self.prefix.is_empty() {
            relative.to_owned()
        } else {
            format!("{}/{relative}", self.prefix)
        })
    }

    pub fn relative_key(&self, key: &str) -> Result<String> {
        let relative = if self.prefix.is_empty() {
            key
        } else {
            key.strip_prefix(&format!("{}/", self.prefix))
                .context("S3 listed an object outside the configured prefix")?
        };
        ensure!(self.key(relative)? == key, "invalid listed S3 object key");
        Ok(relative.to_owned())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMetadata {
    pub key: String,
    pub size: u64,
    pub etag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectPage {
    pub objects: Vec<ObjectMetadata>,
    pub continuation: Option<String>,
}

#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn list(&self, continuation: Option<String>) -> Result<ObjectPage>;
    async fn get_small(&self, relative: &str, max_bytes: u64) -> Result<Vec<u8>>;
    async fn put_if_absent(&self, relative: &str, bytes: Vec<u8>) -> Result<()>;
    async fn put_bytes(&self, relative: &str, bytes: Vec<u8>) -> Result<()>;
    async fn put_file(&self, relative: &str, path: &Path, checksum_sha256_hex: &str) -> Result<()>;
    async fn download(
        &self,
        relative: &str,
        destination: &Path,
        max_bytes: u64,
    ) -> Result<ObjectMetadata>;
}

#[cfg(feature = "s3")]
pub struct AwsObjectStore {
    client: Client,
    location: S3Location,
}

#[cfg(feature = "s3")]
impl AwsObjectStore {
    pub async fn load(location: S3Location, endpoint: Option<&Url>) -> Result<Self> {
        let shared = aws_config::defaults(aws_config::BehaviorVersion::v2026_01_12())
            .load()
            .await;
        let mut builder = aws_sdk_s3::config::Builder::from(&shared)
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::v2026_01_12());
        if let Some(endpoint) = endpoint {
            builder = builder
                .endpoint_url(endpoint.as_str())
                .force_path_style(true);
        }
        Ok(Self {
            client: Client::from_conf(builder.build()),
            location,
        })
    }

    async fn put_multipart(
        &self,
        key: String,
        path: &Path,
        expected_sha256: &[u8],
        checksum_sha256: &str,
    ) -> Result<()> {
        const PART_BYTES: usize = 8 * 1024 * 1024;
        let created = self
            .client
            .create_multipart_upload()
            .bucket(self.location.bucket())
            .key(&key)
            .metadata("sha256", checksum_sha256)
            .send()
            .await?;
        let upload_id = created
            .upload_id()
            .context("S3 multipart response missing upload ID")?
            .to_owned();
        let result: Result<()> = async {
            let mut file = tokio::fs::File::open(path).await?;
            let mut whole = Sha256::new();
            let mut parts = Vec::new();
            let mut part_number = 1i32;
            loop {
                let mut bytes = vec![0u8; PART_BYTES];
                let mut filled = 0usize;
                while filled < bytes.len() {
                    let count = file.read(&mut bytes[filled..]).await?;
                    if count == 0 {
                        break;
                    }
                    filled += count;
                }
                if filled == 0 {
                    break;
                }
                bytes.truncate(filled);
                whole.update(&bytes);
                let part_checksum =
                    base64::engine::general_purpose::STANDARD.encode(Sha256::digest(&bytes));
                let uploaded = self
                    .client
                    .upload_part()
                    .bucket(self.location.bucket())
                    .key(&key)
                    .upload_id(&upload_id)
                    .part_number(part_number)
                    .checksum_sha256(part_checksum)
                    .body(ByteStream::from(bytes))
                    .send()
                    .await?;
                parts.push(
                    aws_sdk_s3::types::CompletedPart::builder()
                        .part_number(part_number)
                        .set_e_tag(uploaded.e_tag().map(ToOwned::to_owned))
                        .set_checksum_sha256(uploaded.checksum_sha256().map(ToOwned::to_owned))
                        .build(),
                );
                part_number = part_number.checked_add(1).context("too many S3 parts")?;
            }
            ensure!(
                whole.finalize().as_slice() == expected_sha256,
                "upload source checksum changed"
            );
            self.client
                .complete_multipart_upload()
                .bucket(self.location.bucket())
                .key(&key)
                .upload_id(&upload_id)
                .multipart_upload(
                    aws_sdk_s3::types::CompletedMultipartUpload::builder()
                        .set_parts(Some(parts))
                        .build(),
                )
                .send()
                .await?;
            Ok(())
        }
        .await;
        if result.is_err() {
            let _ = self
                .client
                .abort_multipart_upload()
                .bucket(self.location.bucket())
                .key(&key)
                .upload_id(&upload_id)
                .send()
                .await;
        }
        result
    }
}

#[cfg(feature = "s3")]
#[async_trait]
impl ObjectStore for AwsObjectStore {
    async fn list(&self, continuation: Option<String>) -> Result<ObjectPage> {
        let prefix = if self.location.prefix.is_empty() {
            None
        } else {
            Some(format!("{}/", self.location.prefix))
        };
        let output = self
            .client
            .list_objects_v2()
            .bucket(self.location.bucket())
            .set_prefix(prefix)
            .set_continuation_token(continuation)
            .send()
            .await?;
        let objects = output
            .contents()
            .iter()
            .map(|object| {
                Ok(ObjectMetadata {
                    key: self
                        .location
                        .relative_key(object.key().context("S3 list object missing key")?)?,
                    size: u64::try_from(object.size().unwrap_or_default())?,
                    etag: object.e_tag().map(ToOwned::to_owned),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(ObjectPage {
            objects,
            continuation: output.next_continuation_token().map(ToOwned::to_owned),
        })
    }

    async fn get_small(&self, relative: &str, max_bytes: u64) -> Result<Vec<u8>> {
        let output = self
            .client
            .get_object()
            .bucket(self.location.bucket())
            .key(self.location.key(relative)?)
            .send()
            .await?;
        let declared = u64::try_from(output.content_length().unwrap_or_default())?;
        ensure!(declared <= max_bytes, "S3 object exceeds read limit");
        let mut input = output.body.into_async_read().take(max_bytes + 1);
        let mut bytes = Vec::with_capacity(usize::try_from(declared)?);
        input.read_to_end(&mut bytes).await?;
        ensure!(
            bytes.len() as u64 <= max_bytes,
            "S3 object exceeds read limit"
        );
        Ok(bytes)
    }

    async fn put_if_absent(&self, relative: &str, bytes: Vec<u8>) -> Result<()> {
        self.client
            .put_object()
            .bucket(self.location.bucket())
            .key(self.location.key(relative)?)
            .if_none_match("*")
            .body(ByteStream::from(bytes))
            .send()
            .await?;
        Ok(())
    }

    async fn put_bytes(&self, relative: &str, bytes: Vec<u8>) -> Result<()> {
        self.client
            .put_object()
            .bucket(self.location.bucket())
            .key(self.location.key(relative)?)
            .body(ByteStream::from(bytes))
            .send()
            .await?;
        Ok(())
    }

    async fn put_file(&self, relative: &str, path: &Path, checksum_sha256_hex: &str) -> Result<()> {
        let checksum = decode_sha256(checksum_sha256_hex)?;
        let checksum_sha256 = base64::engine::general_purpose::STANDARD.encode(checksum);
        let key = self.location.key(relative)?;
        if tokio::fs::metadata(path).await?.len() >= 16 * 1024 * 1024 {
            return self
                .put_multipart(key, path, &checksum, &checksum_sha256)
                .await;
        }
        self.client
            .put_object()
            .bucket(self.location.bucket())
            .key(key)
            .checksum_sha256(checksum_sha256)
            .body(ByteStream::from_path(path).await?)
            .send()
            .await?;
        Ok(())
    }

    async fn download(
        &self,
        relative: &str,
        destination: &Path,
        max_bytes: u64,
    ) -> Result<ObjectMetadata> {
        ensure!(!destination.exists(), "download destination already exists");
        let key = self.location.key(relative)?;
        let output = self
            .client
            .get_object()
            .bucket(self.location.bucket())
            .key(&key)
            .send()
            .await?;
        let declared = u64::try_from(output.content_length().unwrap_or_default())?;
        ensure!(declared <= max_bytes, "S3 object exceeds download limit");
        let etag = output.e_tag().map(ToOwned::to_owned);
        let transfer: Result<u64> = async {
            let mut input = output.body.into_async_read().take(max_bytes + 1);
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(destination)
                .await?;
            let copied = tokio::io::copy(&mut input, &mut file).await?;
            ensure!(copied <= max_bytes, "S3 object exceeds download limit");
            file.flush().await?;
            file.sync_all().await?;
            Ok(copied)
        }
        .await;
        let copied = match transfer {
            Ok(copied) => copied,
            Err(error) => {
                let _ = tokio::fs::remove_file(destination).await;
                return Err(error);
            }
        };
        Ok(ObjectMetadata {
            key: relative.to_owned(),
            size: copied,
            etag,
        })
    }
}

#[cfg(feature = "s3")]
fn decode_sha256(value: &str) -> Result<[u8; 32]> {
    ensure!(value.len() == 64, "S3 checksum must be hex SHA-256");
    let mut decoded = [0u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = (pair[0] as char)
            .to_digit(16)
            .context("S3 checksum must be hex SHA-256")?;
        let low = (pair[1] as char)
            .to_digit(16)
            .context("S3 checksum must be hex SHA-256")?;
        decoded[index] = ((high << 4) | low) as u8;
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn location_separates_bucket_prefix_and_rejects_unsafe_keys() -> Result<()> {
        let location = S3Location::parse("s3://eventglass-prod/tenant/primary/")?;
        assert_eq!(location.bucket(), "eventglass-prod");
        assert_eq!(
            location.key("checkpoints/1.json")?,
            "tenant/primary/checkpoints/1.json"
        );
        assert_eq!(
            location.relative_key("tenant/primary/checkpoints/1.json")?,
            "checkpoints/1.json"
        );
        assert!(location.relative_key("other/checkpoints/1.json").is_err());
        for invalid in [
            "../escape",
            "/absolute",
            "double//slash",
            "encoded%2fslash",
            "back\\slash",
        ] {
            assert!(location.key(invalid).is_err(), "{invalid}");
        }
        for invalid in [
            "https://bucket/prefix",
            "s3:///missing",
            "s3://bucket/a%2fb",
            "s3://user@bucket/prefix",
        ] {
            assert!(S3Location::parse(invalid).is_err(), "{invalid}");
        }
        Ok(())
    }
}
