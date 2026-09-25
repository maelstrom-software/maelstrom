//! A small client for the parts of the Azure Blob Storage REST API that we use.
//!
//! GitHub's artifact API stores artifact data in Azure, and hands out SAS ("shared access
//! signature") URLs for uploading and downloading it. All of the authorization is carried in the
//! URL's query string, so there's no request signing to do: we just make plain HTTP requests.
//!
//! See <https://learn.microsoft.com/en-us/rest/api/storageservices/blob-service-rest-api>.

use anyhow::{anyhow, Result};
use bytes::Bytes;
use derive_more::{Display, Error, From};
use futures::TryStreamExt as _;
use reqwest::{
    header::{CONTENT_LENGTH, CONTENT_TYPE, ETAG, IF_NONE_MATCH, RETRY_AFTER},
    Body, Method, RequestBuilder, Response, StatusCode,
};
use std::{
    fmt,
    future::Future,
    io,
    time::{Duration, Instant},
};
use tokio::io::AsyncRead;
use tokio_util::compat::FuturesAsyncReadCompatExt as _;
use url::Url;

/// The version of the REST API we use. This is the version the Azure SDK we used to use sent.
const API_VERSION: &str = "2022-11-02";

/// Retry policy. These match the defaults of the Azure SDK we used to use.
const RETRY_STATUSES: &[StatusCode] = &[
    StatusCode::REQUEST_TIMEOUT,
    StatusCode::TOO_MANY_REQUESTS,
    StatusCode::INTERNAL_SERVER_ERROR,
    StatusCode::BAD_GATEWAY,
    StatusCode::SERVICE_UNAVAILABLE,
    StatusCode::GATEWAY_TIMEOUT,
];
const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(200);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);
const MAX_RETRIES: u32 = 8;
const MAX_TOTAL_RETRY_TIME: Duration = Duration::from_secs(60);

/// An opaque identifier for a version of a blob.
#[derive(Clone, Debug, Display, From, PartialEq, Eq)]
pub struct Etag(String);

/// An error response from Azure.
#[derive(Debug, Error)]
pub struct BlobError {
    pub status: StatusCode,
    /// The value of the `x-ms-error-code` header, if there was one.
    pub error_code: Option<String>,
}

impl fmt::Display for BlobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Azure blob request failed with status {}", self.status)?;
        if let Some(error_code) = &self.error_code {
            write!(f, " ({error_code})")?;
        }
        Ok(())
    }
}

/// A client for a single blob, addressed by a SAS URL.
#[derive(Clone)]
pub struct BlobClient {
    client: reqwest::Client,
    url: Url,
}

impl BlobClient {
    pub(crate) fn new(client: reqwest::Client, url: Url) -> Self {
        Self { client, url }
    }

    fn request(&self, method: Method, url: Url) -> RequestBuilder {
        self.client
            .request(method, url)
            .header("x-ms-version", API_VERSION)
    }

    /// Create (or replace) a block blob with the given contents. The body is obtained by calling
    /// `body`, which may be called again if the request needs to be retried.
    pub async fn put_block_blob<BodyFnT, BodyFutT>(
        &self,
        content_length: u64,
        mut body: BodyFnT,
    ) -> Result<()>
    where
        BodyFnT: FnMut() -> BodyFutT,
        BodyFutT: Future<Output = Result<Body>>,
    {
        self.send(|| {
            let request = self
                .request(Method::PUT, self.url.clone())
                .header("x-ms-blob-type", "BlockBlob")
                .header(CONTENT_TYPE, "application/octet-stream")
                .header(CONTENT_LENGTH, content_length);
            let body = body();
            async move { Ok(request.body(body.await?)) }
        })
        .await?;
        Ok(())
    }

    /// Create (or replace) an empty append blob.
    pub async fn put_append_blob(&self) -> Result<()> {
        self.send(|| {
            let request = self
                .request(Method::PUT, self.url.clone())
                .header("x-ms-blob-type", "AppendBlob")
                .header(CONTENT_LENGTH, 0);
            async move { Ok(request) }
        })
        .await?;
        Ok(())
    }

    /// Append a block of data to an append blob.
    pub async fn append_block(&self, data: impl Into<Bytes>) -> Result<()> {
        let data = data.into();
        let mut url = self.url.clone();
        url.query_pairs_mut().append_pair("comp", "appendblock");
        self.send(|| {
            let request = self
                .request(Method::PUT, url.clone())
                .header(CONTENT_LENGTH, data.len())
                .body(data.clone());
            async move { Ok(request) }
        })
        .await?;
        Ok(())
    }

    /// Get the contents of the blob starting at byte `start`, along with the blob's current
    /// [`Etag`]. If `if_none_match` is given and the blob's etag matches it, this fails with a
    /// [`BlobError`] with status [`StatusCode::NOT_MODIFIED`].
    pub async fn get_from(
        &self,
        start: usize,
        if_none_match: Option<&Etag>,
    ) -> Result<(Vec<u8>, Etag)> {
        let response = self
            .send(|| {
                let mut request = self
                    .request(Method::GET, self.url.clone())
                    .header("x-ms-range", format!("bytes={start}-"));
                if let Some(Etag(etag)) = if_none_match {
                    request = request.header(IF_NONE_MATCH, etag);
                }
                async move { Ok(request) }
            })
            .await?;
        let etag = response
            .headers()
            .get(ETAG)
            .ok_or_else(|| anyhow!("Azure blob response missing ETag header"))?
            .to_str()?
            .to_owned()
            .into();
        let data = response.bytes().await?.to_vec();
        Ok((data, etag))
    }

    /// Get a stream of the whole contents of the blob.
    pub async fn get(&self) -> Result<impl AsyncRead + Unpin + Send + 'static> {
        let response = self
            .send(|| {
                let request = self.request(Method::GET, self.url.clone());
                async move { Ok(request) }
            })
            .await?;
        Ok(response
            .bytes_stream()
            .map_err(io::Error::other)
            .into_async_read()
            .compat())
    }

    /// Send a request, retrying it if it fails in a way that may be transient. The request is
    /// built by calling `make_request` for each attempt. Returns an error for non-2xx responses.
    async fn send<MakeRequestFnT, MakeRequestFutT>(
        &self,
        mut make_request: MakeRequestFnT,
    ) -> Result<Response>
    where
        MakeRequestFnT: FnMut() -> MakeRequestFutT,
        MakeRequestFutT: Future<Output = Result<RequestBuilder>>,
    {
        let start = Instant::now();
        let mut delay = INITIAL_RETRY_DELAY;
        let mut retries = 0;
        loop {
            let result = make_request().await?.send().await;
            let retry_after = match &result {
                Ok(response) if RETRY_STATUSES.contains(&response.status()) => {
                    Some(retry_after(response).unwrap_or(delay))
                }
                Ok(_) => None,
                Err(err) if err.is_builder() => None,
                Err(_) => Some(delay),
            };
            if let Some(retry_after) = retry_after {
                if retries < MAX_RETRIES && start.elapsed() + retry_after <= MAX_TOTAL_RETRY_TIME {
                    tokio::time::sleep(retry_after).await;
                    delay = (delay * 2).min(MAX_RETRY_DELAY);
                    retries += 1;
                    continue;
                }
            }
            let response = result?;
            let status = response.status();
            if status.is_success() {
                return Ok(response);
            }
            let error_code = response
                .headers()
                .get("x-ms-error-code")
                .and_then(|v| v.to_str().ok())
                .map(ToOwned::to_owned);
            return Err(BlobError { status, error_code }.into());
        }
    }
}

/// The delay requested by a `Retry-After` header, if the response has one in seconds.
fn retry_after(response: &Response) -> Option<Duration> {
    let seconds = response.headers().get(RETRY_AFTER)?.to_str().ok()?;
    Some(Duration::from_secs(seconds.parse().ok()?))
}
