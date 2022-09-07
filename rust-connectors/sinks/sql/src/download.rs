use anyhow::{anyhow, Context};
use flate2::read::GzDecoder;
use fluvio_future::tracing::info;
use futures::io::AsyncReadExt;
use std::io::prelude::*;
use std::io::Cursor;
use tar::Archive;
use url::Url;

const MANIFEST_FILENAME: &str = "manifest.tar.gz";
const DEFAULT_VERSION: &str = "latest";

pub(crate) struct Downloader {
    base_url: Url,
}

impl Downloader {
    pub(crate) fn from_url(url: Url) -> anyhow::Result<Self> {
        let base_url = url.join("/hub/v0/pkg/pub/")?;
        info!("base_url={}", base_url);

        Ok(Self { base_url })
    }

    pub(crate) async fn download_binary(&self, package: &str) -> anyhow::Result<Vec<u8>> {
        let (group, name, version) = into_version_parts(package)?;
        info!(group, name, version, "downloading module from hub");

        let response = isahc::get_async(
            self.base_url
                .join(format!("{}/{}/{}", group, name, version).as_str())?
                .as_str(),
        )
        .await?;
        if response.status().as_u16() != 200 {
            return Err(anyhow!(
                "unsuccessful response from Hub: {}",
                response.status()
            ));
        }
        let body_len = response.body().len().unwrap_or_default() as usize;
        info!(body_len, "response body read");

        let mut body = Vec::with_capacity(body_len);
        response.into_body().read_to_end(&mut body).await?;

        let manifest_tar_gz = extract(Cursor::new(body), MANIFEST_FILENAME)
            .context("unable to extract archive from body response")?;
        let manifest_tar = decompress(Cursor::new(manifest_tar_gz))
            .context("unable to decompress manifest archive entry")?;
        let binary = extract(Cursor::new(manifest_tar), ".wasm")
            .context("unable to extract module binary from manifest archive")?;

        info!(
            "module binary pulled successfully. Len={} bytes",
            binary.len()
        );
        Ok(binary)
    }
}

fn into_version_parts(input: &str) -> anyhow::Result<(&str, &str, &str)> {
    let parts: Vec<&str> = input.split('/').collect();
    let group = parts
        .first()
        .filter(|p| !p.trim().is_empty())
        .ok_or_else(|| {
            anyhow!("missing `group` part in package name. Expected format `group/name/version`")
        })?;
    let name = parts
        .get(1)
        .filter(|p| !p.trim().is_empty())
        .ok_or_else(|| {
            anyhow!("missing `name` part in package name. Expected format `group/name/version`")
        })?;
    let version = parts
        .get(2)
        .filter(|p| !p.trim().is_empty())
        .unwrap_or(&DEFAULT_VERSION);
    Ok((group, name, version))
}

fn extract<R: Read>(reader: R, file_suffix: &str) -> anyhow::Result<Vec<u8>> {
    let mut archive = Archive::new(reader);
    for entry in archive.entries()? {
        let mut file = entry?;
        if let Ok(path) = file.path() {
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .filter(|file| file.ends_with(file_suffix))
                .is_some()
            {
                let mut result = Vec::new();
                file.read_to_end(&mut result)?;
                return Ok(result);
            }
        }
    }
    Err(anyhow!("file {} not found inside the archive", file_suffix))
}

fn decompress<R: Read>(reader: R) -> anyhow::Result<Vec<u8>> {
    let mut decoder = GzDecoder::new(reader);
    let mut result = Vec::new();
    decoder.read_to_end(&mut result)?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use httpmock::Method::GET;
    use httpmock::MockServer;
    use tar::Header;

    #[test]
    fn test_into_versions_part() {
        assert_eq!(
            into_version_parts("infinyon/sql/latest").expect("valid package name"),
            ("infinyon", "sql", "latest")
        );
        assert_eq!(
            into_version_parts("infinyon/sql").expect("valid package name"),
            ("infinyon", "sql", DEFAULT_VERSION)
        );
        assert_eq!(
            into_version_parts("infinyon/sql/").expect("valid package name"),
            ("infinyon", "sql", DEFAULT_VERSION)
        );
        assert_eq!(
            into_version_parts("infinyon/avro/1.0.0").expect("valid package name"),
            ("infinyon", "avro", "1.0.0")
        );
        assert!(into_version_parts("infinyon").is_err());
        assert!(into_version_parts("infinyon/").is_err());
        assert!(into_version_parts("/").is_err());
        assert!(into_version_parts("").is_err());
    }

    #[async_std::test]
    async fn test_non_ok_response() {
        //given
        let (server, downloader) = test_downloader();
        let mock = server.mock(|when, then| {
            when.method(GET).path("/hub/v0/pkg/pub/group/name/latest");
            then.status(500);
        });

        //when
        let result = downloader.download_binary("group/name").await;

        //then
        mock.assert();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("unsuccessful response from Hub"))
    }

    #[async_std::test]
    async fn test_download() {
        //given
        let (server, downloader) = test_downloader();
        let mock = server.mock(|when, then| {
            when.method(GET).path("/hub/v0/pkg/pub/group/name/latest");
            then.status(200)
                .header("content-type", "application/octet-stream")
                .body(pack_data(b"hello_world"));
        });

        //when
        let result = downloader
            .download_binary("group/name")
            .await
            .expect("downloaded file");

        //then
        mock.assert();
        assert_eq!(result, b"hello_world");
    }

    fn test_downloader() -> (MockServer, Downloader) {
        fluvio_future::subscriber::init_logger();
        let server = MockServer::start();
        let downloader =
            Downloader::from_url(Url::parse(&server.base_url()).expect("valid server url"))
                .expect("valid url");
        (server, downloader)
    }

    fn archive(filename: &str, payload: &[u8]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        let mut header = Header::new_gnu();
        header.set_size(payload.len() as u64);
        header.set_cksum();
        builder
            .append_data(&mut header, filename, payload)
            .expect("data appended");
        builder.into_inner().expect("archived data")
    }

    fn compress(payload: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(payload).expect("data written");
        encoder.finish().expect("encoded")
    }

    fn pack_data(payload: &[u8]) -> Vec<u8> {
        let wasm = archive("binary.wasm", payload);
        let gzipped_wasm = compress(&wasm);
        archive("manifest.tar.gz", &gzipped_wasm)
    }
}
