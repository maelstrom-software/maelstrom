//! A fake Azure Blob Storage server for tests. It implements just the operations in
//! [`crate::blob`], and mimics how Azure responds to them: status codes, `ETag` and
//! `x-ms-error-code` headers, and so on. Blobs are addressed by path, and a request is only
//! authorized if its URL has the query parameter `sig=valid`, like a (fake) SAS URL.

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::{
    body::Incoming, server::conn::http1, service::service_fn, Method, Request, Response, StatusCode,
};
use hyper_util::rt::TokioIo;
use std::{
    collections::HashMap,
    convert::Infallible,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::net::TcpListener;
use url::Url;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BlobType {
    Block,
    Append,
}

struct Blob {
    blob_type: BlobType,
    data: Vec<u8>,
    version: u64,
}

impl Blob {
    fn etag(&self) -> String {
        format!("\"0x{:016X}\"", self.version)
    }
}

/// A request the server received, for making assertions about what the client sent.
#[derive(Clone, Debug)]
pub struct RecordedRequest {
    pub method: Method,
    pub path: String,
    pub query: Option<String>,
    pub ms_version: Option<String>,
}

#[derive(Default)]
struct State {
    blobs: HashMap<String, Blob>,
    next_version: u64,
    /// The number of upcoming requests to fail with 503 (Server Busy).
    fail_next: usize,
    requests: Vec<RecordedRequest>,
}

#[derive(Clone)]
pub struct FakeAzure {
    addr: SocketAddr,
    state: Arc<Mutex<State>>,
}

impl FakeAzure {
    /// Start a server on an ephemeral localhost port. It runs until the test's runtime shuts down.
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(State::default()));
        let server_state = state.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let state = server_state.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |request| handle(state.clone(), request));
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        Self { addr, state }
    }

    /// An authorized ("SAS") URL for the blob with the given name.
    pub fn url(&self, name: &str) -> Url {
        Url::parse(&format!(
            "http://{}/container/{name}?sv=2022-11-02&sig=valid",
            self.addr
        ))
        .unwrap()
    }

    /// A URL for the blob with the given name whose signature won't be accepted.
    pub fn unauthorized_url(&self, name: &str) -> Url {
        Url::parse(&format!(
            "http://{}/container/{name}?sv=2022-11-02&sig=expired",
            self.addr
        ))
        .unwrap()
    }

    pub fn fail_next(&self, count: usize) {
        self.state.lock().unwrap().fail_next = count;
    }

    pub fn blob_names(&self) -> Vec<String> {
        let state = self.state.lock().unwrap();
        state
            .blobs
            .keys()
            .map(|path| path.strip_prefix("/container/").unwrap().to_owned())
            .collect()
    }

    pub fn blob_len(&self, name: &str) -> usize {
        let state = self.state.lock().unwrap();
        state.blobs[&format!("/container/{name}")].data.len()
    }

    pub fn requests(&self) -> Vec<RecordedRequest> {
        self.state.lock().unwrap().requests.clone()
    }
}

fn response(status: StatusCode) -> hyper::http::response::Builder {
    Response::builder().status(status)
}

fn error(status: StatusCode, error_code: &str) -> Response<Full<Bytes>> {
    response(status)
        .header("x-ms-error-code", error_code)
        .body(Full::default())
        .unwrap()
}

fn header<'a>(request: &'a Request<Incoming>, name: &str) -> Option<&'a str> {
    request.headers().get(name).and_then(|v| v.to_str().ok())
}

async fn handle(
    state: Arc<Mutex<State>>,
    request: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let query = request.uri().query().map(ToOwned::to_owned);
    let ms_version = header(&request, "x-ms-version").map(ToOwned::to_owned);
    let blob_type = header(&request, "x-ms-blob-type").map(ToOwned::to_owned);
    let range = header(&request, "x-ms-range").map(ToOwned::to_owned);
    let if_none_match = header(&request, "if-none-match").map(ToOwned::to_owned);
    let body = request.into_body().collect().await.unwrap().to_bytes();

    let mut state = state.lock().unwrap();
    state.requests.push(RecordedRequest {
        method: method.clone(),
        path: path.clone(),
        query: query.clone(),
        ms_version,
    });

    if state.fail_next > 0 {
        state.fail_next -= 1;
        return Ok(error(StatusCode::SERVICE_UNAVAILABLE, "ServerBusy"));
    }

    let query = query.unwrap_or_default();
    let params: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();
    if params.get("sig").map(|s| s.as_ref()) != Some("valid") {
        return Ok(error(StatusCode::FORBIDDEN, "AuthenticationFailed"));
    }

    let version = state.next_version;
    state.next_version += 1;

    Ok(match (method, params.get("comp").map(|s| s.as_ref())) {
        (Method::PUT, None) => {
            let blob_type = match blob_type.as_deref() {
                Some("BlockBlob") => BlobType::Block,
                Some("AppendBlob") => BlobType::Append,
                _ => return Ok(error(StatusCode::BAD_REQUEST, "MissingRequiredHeader")),
            };
            if blob_type == BlobType::Append && !body.is_empty() {
                return Ok(error(StatusCode::BAD_REQUEST, "InvalidHeaderValue"));
            }
            let blob = Blob {
                blob_type,
                data: body.to_vec(),
                version,
            };
            let etag = blob.etag();
            state.blobs.insert(path, blob);
            response(StatusCode::CREATED)
                .header("etag", etag)
                .body(Full::default())
                .unwrap()
        }
        (Method::PUT, Some("appendblock")) => {
            let Some(blob) = state.blobs.get_mut(&path) else {
                return Ok(error(StatusCode::NOT_FOUND, "BlobNotFound"));
            };
            if blob.blob_type != BlobType::Append {
                return Ok(error(StatusCode::CONFLICT, "InvalidBlobType"));
            }
            blob.data.extend_from_slice(&body);
            blob.version = version;
            response(StatusCode::CREATED)
                .header("etag", blob.etag())
                .body(Full::default())
                .unwrap()
        }
        (Method::GET, None) => {
            let Some(blob) = state.blobs.get(&path) else {
                return Ok(error(StatusCode::NOT_FOUND, "BlobNotFound"));
            };
            let etag = blob.etag();
            if if_none_match.as_deref() == Some(etag.as_str()) {
                return Ok(error(StatusCode::NOT_MODIFIED, "ConditionNotMet"));
            }
            match range {
                None => response(StatusCode::OK)
                    .header("etag", etag)
                    .body(Full::new(Bytes::from(blob.data.clone())))
                    .unwrap(),
                Some(range) => {
                    let start: usize = range
                        .strip_prefix("bytes=")
                        .and_then(|r| r.strip_suffix('-'))
                        .and_then(|s| s.parse().ok())
                        .expect("only open-ended ranges are supported");
                    if start >= blob.data.len() {
                        return Ok(error(StatusCode::RANGE_NOT_SATISFIABLE, "InvalidRange"));
                    }
                    response(StatusCode::PARTIAL_CONTENT)
                        .header("etag", etag)
                        .body(Full::new(Bytes::from(blob.data[start..].to_vec())))
                        .unwrap()
                }
            }
        }
        _ => error(StatusCode::BAD_REQUEST, "UnsupportedHttpVerb"),
    })
}
