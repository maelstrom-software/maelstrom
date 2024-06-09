use crate::parse_str;
use anyhow::anyhow;
use combine::{
    any, attempt, choice, count_min_max, many, many1, not_followed_by, optional,
    parser::char::{alpha_num, digit, string},
    satisfy, token, Parser, Stream,
};
use maelstrom_base::Utf8PathBuf;
use std::{fmt, str::FromStr};

fn component<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = String> {
    many1(satisfy(|c| c != '.' && c != '@' && c != ':' && c != '/'))
}

fn not_hostname<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = String> {
    not_followed_by(string("localhost/")).with(component())
}

fn hostname<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = String> {
    many1(satisfy(|c| c != '/' && c != '@' && c != ':'))
}

fn tag_or_name<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = String> {
    many1(alpha_num().or(token('_')).or(token('.')).or(token('-')))
}

fn digest<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = String> {
    many1(alpha_num().or(token(':')))
}

fn port<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = u16> {
    count_min_max(1, 5, digit()).map(|s: String| s.parse::<u16>().unwrap())
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum Host {
    DockerIo {
        path: Option<String>,
    },
    Other {
        name: String,
        port: Option<u16>,
        path: Option<String>,
    },
}

impl Default for Host {
    fn default() -> Self {
        Self::DockerIo { path: None }
    }
}

impl fmt::Display for Host {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DockerIo { path: None } => write!(f, ""),
            Self::DockerIo { path: Some(path) } => write!(f, "{path}/"),
            Self::Other { name, port, path } => {
                write!(f, "{name}")?;
                if let Some(port) = port {
                    write!(f, ":{port}")?;
                }
                if let Some(path) = path {
                    write!(f, "/{path}")?;
                }
                write!(f, "/")
            }
        }
    }
}

impl Host {
    pub fn base_url(&self) -> String {
        match self {
            Self::DockerIo { path } => {
                let path = path.as_ref().map(|s| s.as_str()).unwrap_or("library");
                format!("https://registry-1.docker.io/v2/{path}")
            }
            Self::Other { name, port, path } => {
                let port_str = port.map(|p| format!(":{p}")).unwrap_or("".into());
                let path_str = path.as_ref().map(|p| format!("/{p}")).unwrap_or("".into());
                format!("https://{name}{port_str}/v2{path_str}")
            }
        }
    }

    pub fn parser<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = Self> {
        optional(attempt(choice((
            attempt((
                not_hostname().skip(token('/')),
                many(attempt(component().skip(token('/')))),
            ))
            .map(|(first, rest): (_, Vec<_>)| {
                let mut path = first;
                if !rest.is_empty() {
                    path += "/";
                    path += &rest.join("/");
                }
                Self::DockerIo { path: Some(path) }
            }),
            attempt((
                (hostname(), optional(token(':').with(port()))).skip(token('/')),
                many(attempt(component().skip(token('/')))),
            ))
            .map(|((name, port), path): (_, Vec<_>)| Self::Other {
                name,
                port,
                path: (!path.is_empty()).then(|| path.join("/")),
            }),
        ))))
        .map(|loc| loc.unwrap_or_default())
    }
}

#[cfg(test)]
fn test_host_roundtrip(i: &str, e: Host) {
    assert_eq!(&parse_str!(Host, i).unwrap(), &e);
    assert_eq!(e.to_string(), i);
}

#[test]
fn parse_host_docker_io() {
    test_host_roundtrip(
        "foo/",
        Host::DockerIo {
            path: Some("foo".into()),
        },
    );

    test_host_roundtrip(
        "foo/bar/",
        Host::DockerIo {
            path: Some("foo/bar".into()),
        },
    );
}

#[test]
fn parse_host_other() {
    test_host_roundtrip(
        "localhost/",
        Host::Other {
            name: "localhost".into(),
            port: None,
            path: None,
        },
    );

    test_host_roundtrip(
        "localhost/foo/",
        Host::Other {
            name: "localhost".into(),
            port: None,
            path: Some("foo".into()),
        },
    );

    test_host_roundtrip(
        "localhost:8080/foo/",
        Host::Other {
            name: "localhost".into(),
            port: Some(8080),
            path: Some("foo".into()),
        },
    );

    test_host_roundtrip(
        "foo.com/bar/",
        Host::Other {
            name: "foo.com".into(),
            port: None,
            path: Some("bar".into()),
        },
    );

    test_host_roundtrip(
        "foo.com:70/bar/",
        Host::Other {
            name: "foo.com".into(),
            port: Some(70),
            path: Some("bar".into()),
        },
    );

    test_host_roundtrip(
        "foo.com/bar/baz/",
        Host::Other {
            name: "foo.com".into(),
            port: None,
            path: Some("bar/baz".into()),
        },
    );

    test_host_roundtrip(
        "foo.com:70/bar/baz/",
        Host::Other {
            name: "foo.com".into(),
            port: Some(70),
            path: Some("bar/baz".into()),
        },
    );
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct DockerReference {
    pub host: Host,
    pub name: String,
    pub tag: Option<String>,
    pub digest: Option<String>,
}

impl fmt::Display for DockerReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", &self.host, &self.name)?;
        if let Some(tag) = &self.tag {
            write!(f, ":{tag}")?
        }
        if let Some(digest) = &self.digest {
            write!(f, "@{digest}")?
        }
        Ok(())
    }
}

impl DockerReference {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn tag(&self) -> &str {
        self.tag.as_deref().unwrap_or("latest")
    }

    pub fn digest(&self) -> Option<&str> {
        self.digest.as_deref()
    }

    pub fn digest_or_tag(&self) -> &str {
        self.digest().unwrap_or(self.tag())
    }

    pub fn parser<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = Self> {
        (
            Host::parser(),
            many1(tag_or_name()),
            optional(token(':').with(tag_or_name())),
            optional(token('@').with(digest())),
        )
            .map(|(host, name, tag, digest)| Self {
                host,
                name,
                tag,
                digest,
            })
    }
}

#[cfg(test)]
fn test_docker_reference_roundtrip(i: &str, e: DockerReference) {
    assert_eq!(&parse_str!(DockerReference, i).unwrap(), &e);
    assert_eq!(e.to_string(), i);
}

#[test]
fn parse_docker_reference_host_default() {
    test_docker_reference_roundtrip(
        "foobar",
        DockerReference {
            host: Host::default(),
            name: "foobar".into(),
            tag: None,
            digest: None,
        },
    );

    test_docker_reference_roundtrip(
        "foo-bar1",
        DockerReference {
            host: Host::default(),
            name: "foo-bar1".into(),
            tag: None,
            digest: None,
        },
    );

    test_docker_reference_roundtrip(
        "foo_bar2",
        DockerReference {
            host: Host::default(),
            name: "foo_bar2".into(),
            tag: None,
            digest: None,
        },
    );

    test_docker_reference_roundtrip(
        "foobar@sha256:abc123",
        DockerReference {
            host: Host::default(),
            name: "foobar".into(),
            tag: None,
            digest: Some("sha256:abc123".into()),
        },
    );

    test_docker_reference_roundtrip(
        "foobar:latest",
        DockerReference {
            host: Host::default(),
            name: "foobar".into(),
            tag: Some("latest".into()),
            digest: None,
        },
    );

    test_docker_reference_roundtrip(
        "foo.b_ar-2:a1_b.c-d",
        DockerReference {
            host: Host::default(),
            name: "foo.b_ar-2".into(),
            tag: Some("a1_b.c-d".into()),
            digest: None,
        },
    );

    test_docker_reference_roundtrip(
        "foobar:latest@sha256:abc123",
        DockerReference {
            host: Host::default(),
            name: "foobar".into(),
            tag: Some("latest".into()),
            digest: Some("sha256:abc123".into()),
        },
    );
}

#[test]
fn parse_docker_reference_host_specified() {
    test_docker_reference_roundtrip(
        "foo/bar",
        DockerReference {
            host: Host::DockerIo {
                path: Some("foo".into()),
            },
            name: "bar".into(),
            tag: None,
            digest: None,
        },
    );

    test_docker_reference_roundtrip(
        "foo/bar/baz",
        DockerReference {
            host: Host::DockerIo {
                path: Some("foo/bar".into()),
            },
            name: "baz".into(),
            tag: None,
            digest: None,
        },
    );

    test_docker_reference_roundtrip(
        "foo/bar@sha256:abc123",
        DockerReference {
            host: Host::DockerIo {
                path: Some("foo".into()),
            },
            name: "bar".into(),
            tag: None,
            digest: Some("sha256:abc123".into()),
        },
    );

    test_docker_reference_roundtrip(
        "foo/bar:latest",
        DockerReference {
            host: Host::DockerIo {
                path: Some("foo".into()),
            },
            name: "bar".into(),
            tag: Some("latest".into()),
            digest: None,
        },
    );

    test_docker_reference_roundtrip(
        "foo/bar:latest@sha256:abc123",
        DockerReference {
            host: Host::DockerIo {
                path: Some("foo".into()),
            },
            name: "bar".into(),
            tag: Some("latest".into()),
            digest: Some("sha256:abc123".into()),
        },
    );
}

#[test]
fn parse_docker_reference_host_other() {
    test_docker_reference_roundtrip(
        "foo.co.uk/bar",
        DockerReference {
            host: Host::Other {
                name: "foo.co.uk".into(),
                port: None,
                path: None,
            },
            name: "bar".into(),
            tag: None,
            digest: None,
        },
    );

    test_docker_reference_roundtrip(
        "foo.com/bar",
        DockerReference {
            host: Host::Other {
                name: "foo.com".into(),
                port: None,
                path: None,
            },
            name: "bar".into(),
            tag: None,
            digest: None,
        },
    );

    test_docker_reference_roundtrip(
        "foo.com/bar/baz",
        DockerReference {
            host: Host::Other {
                name: "foo.com".into(),
                port: None,
                path: Some("bar".into()),
            },
            name: "baz".into(),
            tag: None,
            digest: None,
        },
    );

    test_docker_reference_roundtrip(
        "localhost/bar",
        DockerReference {
            host: Host::Other {
                name: "localhost".into(),
                port: None,
                path: None,
            },
            name: "bar".into(),
            tag: None,
            digest: None,
        },
    );

    test_docker_reference_roundtrip(
        "foo.com/bar@sha256:abc123",
        DockerReference {
            host: Host::Other {
                name: "foo.com".into(),
                port: None,
                path: None,
            },
            name: "bar".into(),
            tag: None,
            digest: Some("sha256:abc123".into()),
        },
    );

    test_docker_reference_roundtrip(
        "foo.com/bar:latest",
        DockerReference {
            host: Host::Other {
                name: "foo.com".into(),
                port: None,
                path: None,
            },
            name: "bar".into(),
            tag: Some("latest".into()),
            digest: None,
        },
    );

    test_docker_reference_roundtrip(
        "foo.com/bar:latest@sha256:abc123",
        DockerReference {
            host: Host::Other {
                name: "foo.com".into(),
                port: None,
                path: None,
            },
            name: "bar".into(),
            tag: Some("latest".into()),
            digest: Some("sha256:abc123".into()),
        },
    );

    test_docker_reference_roundtrip(
        "foo.com:1234/bar",
        DockerReference {
            host: Host::Other {
                name: "foo.com".into(),
                port: Some(1234),
                path: None,
            },
            name: "bar".into(),
            tag: None,
            digest: None,
        },
    );

    test_docker_reference_roundtrip(
        "foo.com:1234/bar@sha256:abc123",
        DockerReference {
            host: Host::Other {
                name: "foo.com".into(),
                port: Some(1234),
                path: None,
            },
            name: "bar".into(),
            tag: None,
            digest: Some("sha256:abc123".into()),
        },
    );

    test_docker_reference_roundtrip(
        "foo.com:1234/bar:latest",
        DockerReference {
            host: Host::Other {
                name: "foo.com".into(),
                port: Some(1234),
                path: None,
            },
            name: "bar".into(),
            tag: Some("latest".into()),
            digest: None,
        },
    );

    test_docker_reference_roundtrip(
        "foo.com:1234/bar:latest@sha256:abc123",
        DockerReference {
            host: Host::Other {
                name: "foo.com".into(),
                port: Some(1234),
                path: None,
            },
            name: "bar".into(),
            tag: Some("latest".into()),
            digest: Some("sha256:abc123".into()),
        },
    );

    test_docker_reference_roundtrip(
        "foo.com:1234/bar/baz/bin:latest@sha256:abc123",
        DockerReference {
            host: Host::Other {
                name: "foo.com".into(),
                port: Some(1234),
                path: Some("bar/baz".into()),
            },
            name: "bin".into(),
            tag: Some("latest".into()),
            digest: Some("sha256:abc123".into()),
        },
    );
}

#[test]
fn parse_docker_reference_error() {
    parse_str!(DockerReference, "").unwrap_err();
    parse_str!(DockerReference, "foo/").unwrap_err();
    parse_str!(DockerReference, "foo*bar").unwrap_err();
    parse_str!(DockerReference, "foo:").unwrap_err();
    parse_str!(DockerReference, "foo@").unwrap_err();

    parse_str!(DockerReference, "foo@a.b").unwrap_err();
    parse_str!(DockerReference, "foo@a/b").unwrap_err();
    parse_str!(DockerReference, "foo@a@b").unwrap_err();
    parse_str!(DockerReference, "foo.com:/bar").unwrap_err();

    parse_str!(DockerReference, "foo:bar:baz").unwrap_err();
    parse_str!(DockerReference, "foo@abc123@abc345").unwrap_err();
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct LocalPath {
    pub path: Utf8PathBuf,
    pub reference: Option<String>,
}

impl fmt::Display for LocalPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.path)?;
        if let Some(ref_) = &self.reference {
            write!(f, ":{ref_}")?;
        }
        Ok(())
    }
}

impl LocalPath {
    pub fn parser<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = Self> {
        (
            many1(satisfy(|c| c != ':')),
            optional(token(':').with(many1(any()))),
        )
            .map(|(path, reference): (String, _)| Self {
                path: path.into(),
                reference,
            })
    }
}

#[cfg(test)]
fn test_local_path_roundtrip(i: &str, e: LocalPath) {
    assert_eq!(&parse_str!(LocalPath, i).unwrap(), &e);
    assert_eq!(e.to_string(), i);
}

#[test]
fn parse_local_path() {
    test_local_path_roundtrip(
        "foo/bar/baz",
        LocalPath {
            path: "foo/bar/baz".into(),
            reference: None,
        },
    );
    test_local_path_roundtrip(
        "foo/bar/baz:abc",
        LocalPath {
            path: "foo/bar/baz".into(),
            reference: Some("abc".into()),
        },
    );
    test_local_path_roundtrip(
        "foo/bar/baz:abc:def",
        LocalPath {
            path: "foo/bar/baz".into(),
            reference: Some("abc:def".into()),
        },
    );
}

#[test]
fn parse_local_path_err() {
    parse_str!(LocalPath, "").unwrap_err();
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum ImageName {
    Docker(DockerReference),
    Oci(LocalPath),
    OciArchive(LocalPath),
}

impl fmt::Display for ImageName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Docker(r) => write!(f, "docker://{r}"),
            Self::Oci(p) => write!(f, "oci:{p}"),
            Self::OciArchive(p) => write!(f, "oci-archive:{p}"),
        }
    }
}

impl ImageName {
    pub fn parser<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = Self> {
        choice((
            attempt(string("docker://").with(DockerReference::parser().map(Self::Docker))),
            attempt(string("oci:").with(LocalPath::parser().map(Self::Oci))),
            string("oci-archive:").with(LocalPath::parser().map(Self::OciArchive)),
        ))
    }
}

impl FromStr for ImageName {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        parse_str!(Self, s).map_err(|e| anyhow!("failed to parse image name: {e}"))
    }
}

#[cfg(test)]
fn test_image_name_roundtrip(i: &str, e: ImageName) {
    assert_eq!(&parse_str!(ImageName, i).unwrap(), &e);
    assert_eq!(e.to_string(), i);
}

#[test]
fn parse_image_name() {
    test_image_name_roundtrip(
        "docker://foo.com:124/bar:baz@sha256:abc123",
        ImageName::Docker(DockerReference {
            host: Host::Other {
                name: "foo.com".into(),
                port: Some(124),
                path: None,
            },
            name: "bar".into(),
            tag: Some("baz".into()),
            digest: Some("sha256:abc123".into()),
        }),
    );

    test_image_name_roundtrip(
        "oci:/foo/bar:r:ef1",
        ImageName::Oci(LocalPath {
            path: "/foo/bar".into(),
            reference: Some("r:ef1".into()),
        }),
    );

    test_image_name_roundtrip(
        "oci-archive:/foo/bar:r:ef1",
        ImageName::OciArchive(LocalPath {
            path: "/foo/bar".into(),
            reference: Some("r:ef1".into()),
        }),
    );
}

#[test]
fn parse_image_name_err() {
    parse_str!(ImageName, "").unwrap_err();
    parse_str!(ImageName, "poci:foo").unwrap_err();
    parse_str!(ImageName, "docker://").unwrap_err();
}
