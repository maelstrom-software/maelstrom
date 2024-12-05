mod github;
mod tcp;

pub use github::{github_client_factory, GitHubArtifactFetcher};
pub use tcp::TcpArtifactFetcher;
