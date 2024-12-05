//! This crate contains code that can communicate with GitHub's artifact API.
//!
//! This API works by doing HTTP requests to GitHub to manage the artifacts, but the actual data is
//! stored in Azure, and the API returns signed-URLs to upload or download the data.
//!
//! The GitHub API is using the TWIRP RPC framework <https://github.com/twitchtv/twirp>.
//!
//! It seems that it uses protobuf definitions to define the body of requests. I'm not sure where
//! to find these protobuf definitions, but we do have access to typescript that seems to be
//! generated from them, which can be found here:
//! <https://github.com/actions/toolkit/blob/main/packages/artifact/src/generated/results/api/v1/artifact.ts>

pub use azure_core::{
    error::Result as AzureResult,
    tokio::fs::{FileStream, FileStreamBuilder},
    Body, SeekableStream,
};

use anyhow::{anyhow, bail, Result};
use azure_storage_blobs::prelude::BlobClient;
use futures::{stream::TryStreamExt as _, StreamExt as _};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use std::str::FromStr;
use tokio::io::AsyncRead;
use tokio_util::compat::FuturesAsyncReadCompatExt as _;
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BackendIds {
    pub workflow_run_backend_id: String,
    pub workflow_job_run_backend_id: String,
}

impl FromStr for BackendIds {
    type Err = anyhow::Error;

    fn from_str(token: &str) -> Result<BackendIds> {
        use base64::Engine as _;

        let mut token_parts = token.split(".").skip(1);
        let b64_part = token_parts
            .next()
            .ok_or_else(|| anyhow!("missing period"))?;
        let decoded = base64::engine::general_purpose::STANDARD_NO_PAD
            .decode(b64_part)
            .map_err(|e| anyhow!("base64 invalid: {e}"))?;
        let v = serde_json::from_slice::<serde_json::Value>(&decoded)?;

        let scp = v
            .get("scp")
            .ok_or_else(|| anyhow!("missing 'scp' field"))?
            .as_str()
            .ok_or_else(|| anyhow!("'scp' field not a string"))?;

        let scope_parts = scp
            .split(" ")
            .map(|p| p.split(":").collect::<Vec<_>>())
            .find(|p| p[0] == "Actions.Results")
            .ok_or_else(|| anyhow!("'Actions.Results' missing from 'scp' field"))?;

        Ok(Self {
            workflow_run_backend_id: scope_parts[1].into(),
            workflow_job_run_backend_id: scope_parts[2].into(),
        })
    }
}

struct TwirpClient {
    client: reqwest::Client,
    token: String,
    base_url: Url,
    backend_ids: BackendIds,
}

impl TwirpClient {
    fn new(token: &str, base_url: Url) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::new(),
            token: token.into(),
            base_url,
            backend_ids: token.parse()?,
        })
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
    #[serde(skip_serializing_if = "Option::is_none")]
    name_filter: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    id_filter: Option<DatabaseId>,
}

#[serde_as]
#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct DatabaseId(#[serde_as(as = "DisplayFromStr")] i64);

#[serde_as]
#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct Artifact {
    #[serde(flatten, with = "BackendIdsSnakeCase")]
    pub backend_ids: BackendIds,
    pub name: String,
    #[serde_as(as = "DisplayFromStr")]
    pub size: i64,
    pub database_id: DatabaseId,
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
    pub fn new(token: &str, base_url: Url) -> Result<Self> {
        Ok(Self {
            client: TwirpClient::new(token, base_url)?,
        })
    }

    /// Start an upload of an artifact. It returns a [`BlobClient`] which should be used to upload
    /// your data. Once all the data has been written, [`Self::finish_upload`] must be called to
    /// finalize the upload.
    ///
    /// The given name needs to be something unique, an error should be returned on collision.
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

    /// Meant to be called on an upload which was started via [`Self::start_upload`] which has had
    /// all its data uploaded with the returned [`BlobClient`]. Once it returns success, the
    /// artifact should be immediately available to be downloaded. If called on an artifact not in
    /// this state, an error should be returned.
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

    /// Upload the given content as an artifact. Once it returns success, the artifact should be
    /// immediately available for download. The given content can be an in-memory buffer or a
    /// [`FileStream`] created using [`FileStreamBuilder`].
    pub async fn upload(&self, name: &str, content: impl Into<Body>) -> Result<()> {
        let blob_client = self.start_upload(name).await?;
        let body: Body = content.into();
        let size = body.len();
        blob_client
            .put_block_blob(body)
            .content_type("application/octet-stream")
            .await?;
        self.finish_upload(name, size).await?;
        Ok(())
    }

    async fn list_internal(
        &self,
        name_filter: Option<String>,
        id_filter: Option<DatabaseId>,
    ) -> Result<Vec<Artifact>> {
        let req = ListArtifactsRequest {
            backend_ids: self.client.backend_ids.clone(),
            name_filter,
            id_filter,
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

    /// List all the given artifacts accessible to the current workflow run.
    pub async fn list(&self) -> Result<Vec<Artifact>> {
        self.list_internal(None, None).await
    }

    /// Get the artifact represented by the given name if it exists.
    pub async fn get(&self, name: &str) -> Result<Option<Artifact>> {
        let mut artifacts = self.list_internal(Some(name.into()), None).await?;
        if artifacts.is_empty() {
            return Ok(None);
        }
        if artifacts.len() > 1 {
            bail!("invalid filtered list response");
        }
        Ok(Some(artifacts.remove(0)))
    }

    /// Get the artifact represented by the given id if it exists.
    pub async fn get_by_id(&self, id: DatabaseId) -> Result<Option<Artifact>> {
        let mut artifacts = self.list_internal(None, Some(id)).await?;
        if artifacts.is_empty() {
            return Ok(None);
        }
        if artifacts.len() > 1 {
            bail!("invalid filtered list response");
        }
        Ok(Some(artifacts.remove(0)))
    }

    /// Start a download of an artifact identified by the given name. The returned [`BlobClient`]
    /// should be used to download all or part of the data.
    ///
    /// The `backend_ids` must be the ones for the artifact obtained from [`Self::list`]. An
    /// individual uploader should end up with the same `backend_ids` for all artifacts it uploads.
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

    /// Return a stream that downloads all the contents of the artifacts represented by the given
    /// name.
    ///
    /// The `backend_ids` must be the ones for the artifact obtained from [`Self::list`]. An
    /// individual uploader should end up with the same `backend_ids` for all artifacts it uploads.
    pub async fn download(
        &self,
        backend_ids: BackendIds,
        name: &str,
    ) -> Result<impl AsyncRead + Unpin + Send + Sync + 'static> {
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

    const TEST_TOKEN: &str = include_str!("test_token.b64");

    #[test]
    fn backend_ids_from_str_canned_example() {
        let ids = BackendIds::from_str(TEST_TOKEN).unwrap();
        assert_eq!(
            ids,
            BackendIds {
                workflow_run_backend_id: "a4c8893f-39a2-4108-b278-a7d0fb589276".into(),
                workflow_job_run_backend_id: "5264e576-3c6f-51f6-f055-fab409685f20".into()
            }
        );
    }

    #[test]
    fn backend_ids_errors() {
        fn test_error(s: &str, expected_error: &str) {
            let actual_error = BackendIds::from_str(s).unwrap_err().to_string();
            assert!(actual_error.contains(expected_error), "{actual_error}");
        }
        test_error("foobar", "missing period");
        test_error("foo.bar", "base64 invalid");
        test_error("foo.e30=", "base64 invalid: Invalid padding");
        test_error("foo.e30", "missing 'scp' field");
        test_error("foo.eyJzY3AiOjEyfQ", "'scp' field not a string");
        test_error(
            "foo.eyJzY3AiOiJmb28ifQ",
            "'Actions.Results' missing from 'scp' field",
        );
    }

    const TEST_DATA: &[u8] = include_bytes!("lib.rs");

    fn client_factory() -> Option<GitHubClient> {
        let token = std::env::var("ACTIONS_RUNTIME_TOKEN").ok()?;
        let base_url = Url::parse(&std::env::var("ACTIONS_RESULTS_URL").ok()?).unwrap();
        Some(GitHubClient::new(&token, base_url).unwrap())
    }

    #[tokio::test]
    async fn real_github_integration_test() {
        let Some(client) = client_factory() else {
            println!("skipping due to missing GitHub credentials");
            return;
        };
        println!("test found GitHub credentials");

        client.upload("test_data", TEST_DATA).await.unwrap();

        let listing = client.list().await.unwrap();
        println!("got artifact listing {listing:?}");
        assert!(listing.iter().find(|a| a.name == "test_data").is_some());

        let artifact = client.get("test_data").await.unwrap().unwrap();

        let artifact2 = client
            .get_by_id(artifact.database_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&artifact, &artifact2);

        assert_eq!(client.get("this_does_not_exist").await.unwrap(), None);

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
