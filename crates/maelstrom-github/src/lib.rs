use anyhow::{anyhow, bail, Result};
use azure_storage_blobs::prelude::BlobClient;
use futures::{stream::TryStreamExt as _, StreamExt as _};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::io::AsyncRead;
use tokio_util::compat::FuturesAsyncReadCompatExt as _;
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackendIds {
    pub workflow_run_backend_id: String,
    pub workflow_job_run_backend_id: String,
}

fn decode_backend_ids(token: &str) -> BackendIds {
    use base64::Engine as _;

    let mut token_parts = token.split(".").skip(1);
    let b64_part = token_parts.next().unwrap();
    let decoded = base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(b64_part)
        .unwrap();
    let v = serde_json::from_slice::<serde_json::Value>(&decoded).unwrap();

    let scp = v.get("scp").unwrap().as_str().unwrap();

    let scope_parts = scp
        .split(" ")
        .map(|p| p.split(":").collect::<Vec<_>>())
        .find(|p| p[0] == "Actions.Results")
        .unwrap();

    BackendIds {
        workflow_run_backend_id: scope_parts[1].into(),
        workflow_job_run_backend_id: scope_parts[2].into(),
    }
}

struct TwirpClient {
    client: reqwest::Client,
    token: String,
    base_url: Url,
    backend_ids: BackendIds,
}

impl TwirpClient {
    fn new(token: &str, base_url: Url) -> Self {
        let client = reqwest::Client::new();

        let backend_ids = decode_backend_ids(token);

        Self {
            client,
            token: token.into(),
            base_url,
            backend_ids,
        }
    }

    async fn request<BodyT: Serialize, RespT: DeserializeOwned>(
        &self,
        service: &str,
        method: &str,
        body: &BodyT,
    ) -> Result<RespT> {
        let req = self
            .client
            .post(
                self.base_url
                    .join(&format!("twirp/{service}/{method}"))
                    .unwrap(),
            )
            .header("Content-Type", "application/json")
            .header("User-Agent", "@actions/artifact-2.1.11")
            .header(
                "Authorization",
                &format!("Bearer {token}", token = &self.token),
            )
            .json(body);

        let resp = req.send().await?;
        if !resp.status().is_success() {
            bail!("{}", resp.text().await.unwrap());
        }

        Ok(resp.json().await?)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateArtifactRequest {
    #[serde(flatten)]
    backend_ids: BackendIds,
    name: String,
    version: u32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FinalizeArtifactRequest {
    #[serde(flatten)]
    backend_ids: BackendIds,
    name: String,
    size: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ListArtifactsRequest {
    #[serde(flatten)]
    backend_ids: BackendIds,
}

#[derive(Debug, Deserialize)]
pub struct Artifact {
    #[serde(flatten, with = "BackendIdsSnakeCase")]
    pub backend_ids: BackendIds,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(remote = "BackendIds")]
struct BackendIdsSnakeCase {
    workflow_run_backend_id: String,
    workflow_job_run_backend_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListArtifactsResponse {
    artifacts: Vec<Artifact>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GetSignedArtifactUrlRequest {
    #[serde(flatten)]
    backend_ids: BackendIds,
    name: String,
}

#[derive(Debug, Deserialize)]
struct CreateArtifactResponse {
    signed_upload_url: String,
}

#[derive(Debug, Deserialize)]
struct GetSignedArtifactUrlResponse {
    signed_url: String,
}

pub struct GitHubClient {
    client: TwirpClient,
}

impl GitHubClient {
    pub fn new(token: &str, base_url: Url) -> Self {
        Self {
            client: TwirpClient::new(token, base_url),
        }
    }

    pub async fn start_upload(&self, name: &str) -> Result<BlobClient> {
        let req = CreateArtifactRequest {
            backend_ids: self.client.backend_ids.clone(),
            name: name.into(),
            version: 4,
        };
        let resp: CreateArtifactResponse = self
            .client
            .request(
                "github.actions.results.api.v1.ArtifactService",
                "CreateArtifact",
                &req,
            )
            .await?;

        let upload_url = url::Url::parse(&resp.signed_upload_url)?;
        Ok(BlobClient::from_sas_url(&upload_url)?)
    }

    pub async fn finish_upload(&self, name: &str, content_length: usize) -> Result<()> {
        let req = FinalizeArtifactRequest {
            backend_ids: self.client.backend_ids.clone(),
            name: name.into(),
            size: content_length,
        };
        self.client
            .request::<_, serde_json::Value>(
                "github.actions.results.api.v1.ArtifactService",
                "FinalizeArtifact",
                &req,
            )
            .await?;
        Ok(())
    }

    pub async fn upload(&self, name: &str, content: &[u8]) -> Result<()> {
        let blob_client = self.start_upload(name).await?;
        blob_client
            .put_block_blob(content.to_owned())
            .content_type("application/octet-stream")
            .await?;
        self.finish_upload(name, content.len()).await?;
        Ok(())
    }

    pub async fn list(&self) -> Result<Vec<Artifact>> {
        let req = ListArtifactsRequest {
            backend_ids: self.client.backend_ids.clone(),
        };
        let resp: ListArtifactsResponse = self
            .client
            .request(
                "github.actions.results.api.v1.ArtifactService",
                "ListArtifacts",
                &req,
            )
            .await?;
        Ok(resp.artifacts)
    }

    pub async fn start_download(&self, backend_ids: BackendIds, name: &str) -> Result<BlobClient> {
        let req = GetSignedArtifactUrlRequest {
            backend_ids,
            name: name.into(),
        };
        let resp: GetSignedArtifactUrlResponse = self
            .client
            .request(
                "github.actions.results.api.v1.ArtifactService",
                "GetSignedArtifactURL",
                &req,
            )
            .await?;
        let url = Url::parse(&resp.signed_url)?;
        Ok(BlobClient::from_sas_url(&url)?)
    }

    pub async fn download(
        &self,
        backend_ids: BackendIds,
        name: &str,
    ) -> Result<impl AsyncRead + Unpin> {
        let blob_client = self.start_download(backend_ids, name).await?;
        let mut page_stream = blob_client.get().chunk_size(u64::MAX).into_stream();
        let single_page = page_stream
            .next()
            .await
            .ok_or_else(|| anyhow!("missing response"))??;
        Ok(single_page
            .data
            .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
            .into_async_read()
            .compat())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_DATA: &[u8] = include_bytes!("lib.rs");

    fn client_factory() -> Option<GitHubClient> {
        let token = std::env::var("ACTIONS_RUNTIME_TOKEN").ok()?;
        let base_url = Url::parse(&std::env::var("ACTIONS_RESULTS_URL").ok()?).unwrap();
        Some(GitHubClient::new(&token, base_url))
    }

    #[tokio::test]
    async fn upload_download() {
        let Some(client) = client_factory() else {
            println!("skipping due to missing GitHub credentials");
            return;
        };
        println!("test found GitHub credentials");

        client.upload("test_data", &TEST_DATA).await.unwrap();

        let listing = client.list().await.unwrap();
        println!("got artifact listing {listing:?}");

        let artifact = listing.iter().find(|a| a.name == "test_data").unwrap();

        let backend_ids = &artifact.backend_ids;
        let mut download_stream = client
            .download(backend_ids.clone(), "test_data")
            .await
            .unwrap();

        let mut downloaded = vec![];
        tokio::io::copy(&mut download_stream, &mut downloaded)
            .await
            .unwrap();

        assert_eq!(downloaded, TEST_DATA);
    }
}
