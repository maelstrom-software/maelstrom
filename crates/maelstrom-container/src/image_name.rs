use anyhow::anyhow;
use combine::{
    any, attempt, choice, count_min_max, many1, optional,
    parser::char::{digit, string},
    satisfy, token, Parser, Stream,
};
use maelstrom_base::Utf8PathBuf;
use std::str::FromStr;

pub fn non_special<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = char> {
    satisfy(|c| c != '.' && c != '/' && c != '@' && c != ':')
}

pub fn hostname_char<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = char> {
    satisfy(|c| c != '/' && c != '@' && c != ':')
}

pub fn port<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = u16> {
    count_min_max(1, 5, digit()).map(|s: String| s.parse::<u16>().unwrap())
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum Host {
    DockerIo { library: Option<String> },
    Other { name: String, port: Option<u16> },
}

impl Default for Host {
    fn default() -> Self {
        Self::DockerIo { library: None }
    }
}

impl Host {
    pub fn base_url(&self) -> String {
        match self {
            Self::DockerIo { library } => {
                let library = library.as_ref().map(|s| s.as_str()).unwrap_or("library");
                format!("https://registry-1.docker.io/v2/{library}")
            }
            Self::Other { name, port } => {
                let port_str = port.map(|p| format!(":{p}")).unwrap_or("".into());
                format!("http://{name}{port_str}")
            }
        }
    }

    pub fn auth_url(&self, name: &str) -> String {
        match self {
            Self::DockerIo { library } => {
                let library = library.as_ref().map(|s| s.as_str()).unwrap_or("library");
                format!(
                    "https://auth.docker.io/\
                    token?service=registry.docker.io&scope=repository:{library}/{name}:pull"
                )
            }
            Self::Other { name, port } => {
                let port_str = port.map(|p| format!(":{p}")).unwrap_or("".into());
                format!(
                    "http://{name}{port_str}/\
                    token?service=registry.docker.io&scope=repository:{name}:pull"
                )
            }
        }
    }

    pub fn parser<InputT: Stream<Token = char>>() -> impl Parser<InputT, Output = Self> {
        optional(attempt(choice((
            attempt(many1(non_special()).skip(token('/')))
                .map(|loc| Self::DockerIo { library: Some(loc) }),
            attempt(many1(hostname_char()).skip(token('/')))
                .map(|name| Self::Other { name, port: None }),
            attempt((many1(hostname_char()).skip(token(':')), port()).skip(token('/'))).map(
                |(name, port)| Self::Other {
                    name,
                    port: Some(port),
                },
            ),
        ))))
        .map(|loc| loc.unwrap_or_default())
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct DockerReference {
    pub host: Host,
    pub name: String,
    pub tag: Option<String>,
    pub digest: Option<String>,
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
            many1(non_special()),
            optional(token(':').with(many1(non_special()))),
            optional(token('@').with(many1(non_special()))),
        )
            .map(|(host, name, tag, digest)| Self {
                host,
                name,
                tag,
                digest,
            })
    }
}

macro_rules! parse_str {
    ($ty:ty, $input:expr) => {{
        use combine::{EasyParser as _, Parser as _};
        <$ty>::parser()
            .skip(combine::eof())
            .easy_parse(combine::stream::position::Stream::new($input))
            .map(|x| x.0)
    }};
}

#[test]
fn parse_docker_reference() {
    assert_eq!(
        parse_str!(DockerReference, "foobar").unwrap(),
        DockerReference {
            host: Host::default(),
            name: "foobar".into(),
            tag: None,
            digest: None,
        }
    );

    assert_eq!(
        parse_str!(DockerReference, "foo-bar1").unwrap(),
        DockerReference {
            host: Host::default(),
            name: "foo-bar1".into(),
            tag: None,
            digest: None,
        }
    );

    assert_eq!(
        parse_str!(DockerReference, "foo_bar2").unwrap(),
        DockerReference {
            host: Host::default(),
            name: "foo_bar2".into(),
            tag: None,
            digest: None,
        }
    );

    assert_eq!(
        parse_str!(DockerReference, "foobar@abc123").unwrap(),
        DockerReference {
            host: Host::default(),
            name: "foobar".into(),
            tag: None,
            digest: Some("abc123".into()),
        }
    );

    assert_eq!(
        parse_str!(DockerReference, "foobar:latest").unwrap(),
        DockerReference {
            host: Host::default(),
            name: "foobar".into(),
            tag: Some("latest".into()),
            digest: None,
        }
    );

    assert_eq!(
        parse_str!(DockerReference, "foobar:latest@abc123").unwrap(),
        DockerReference {
            host: Host::default(),
            name: "foobar".into(),
            tag: Some("latest".into()),
            digest: Some("abc123".into()),
        }
    );

    assert_eq!(
        parse_str!(DockerReference, "foo/bar").unwrap(),
        DockerReference {
            host: Host::DockerIo {
                library: Some("foo".into()),
            },
            name: "bar".into(),
            tag: None,
            digest: None,
        }
    );

    assert_eq!(
        parse_str!(DockerReference, "foo/bar@abc123").unwrap(),
        DockerReference {
            host: Host::DockerIo {
                library: Some("foo".into()),
            },
            name: "bar".into(),
            tag: None,
            digest: Some("abc123".into()),
        }
    );

    assert_eq!(
        parse_str!(DockerReference, "foo/bar:latest").unwrap(),
        DockerReference {
            host: Host::DockerIo {
                library: Some("foo".into()),
            },
            name: "bar".into(),
            tag: Some("latest".into()),
            digest: None,
        }
    );

    assert_eq!(
        parse_str!(DockerReference, "foo/bar:latest@abc123").unwrap(),
        DockerReference {
            host: Host::DockerIo {
                library: Some("foo".into()),
            },
            name: "bar".into(),
            tag: Some("latest".into()),
            digest: Some("abc123".into()),
        }
    );

    assert_eq!(
        parse_str!(DockerReference, "foo.co.uk/bar").unwrap(),
        DockerReference {
            host: Host::Other {
                name: "foo.co.uk".into(),
                port: None
            },
            name: "bar".into(),
            tag: None,
            digest: None,
        }
    );

    assert_eq!(
        parse_str!(DockerReference, "foo.com/bar").unwrap(),
        DockerReference {
            host: Host::Other {
                name: "foo.com".into(),
                port: None
            },
            name: "bar".into(),
            tag: None,
            digest: None,
        }
    );

    assert_eq!(
        parse_str!(DockerReference, "foo.com/bar@abc123").unwrap(),
        DockerReference {
            host: Host::Other {
                name: "foo.com".into(),
                port: None
            },
            name: "bar".into(),
            tag: None,
            digest: Some("abc123".into()),
        }
    );

    assert_eq!(
        parse_str!(DockerReference, "foo.com/bar:latest").unwrap(),
        DockerReference {
            host: Host::Other {
                name: "foo.com".into(),
                port: None
            },
            name: "bar".into(),
            tag: Some("latest".into()),
            digest: None,
        }
    );

    assert_eq!(
        parse_str!(DockerReference, "foo.com/bar:latest@abc123").unwrap(),
        DockerReference {
            host: Host::Other {
                name: "foo.com".into(),
                port: None
            },
            name: "bar".into(),
            tag: Some("latest".into()),
            digest: Some("abc123".into()),
        }
    );

    assert_eq!(
        parse_str!(DockerReference, "foo.com:1234/bar").unwrap(),
        DockerReference {
            host: Host::Other {
                name: "foo.com".into(),
                port: Some(1234)
            },
            name: "bar".into(),
            tag: None,
            digest: None,
        }
    );

    assert_eq!(
        parse_str!(DockerReference, "foo.com:1234/bar@abc123").unwrap(),
        DockerReference {
            host: Host::Other {
                name: "foo.com".into(),
                port: Some(1234)
            },
            name: "bar".into(),
            tag: None,
            digest: Some("abc123".into()),
        }
    );

    assert_eq!(
        parse_str!(DockerReference, "foo.com:1234/bar:latest").unwrap(),
        DockerReference {
            host: Host::Other {
                name: "foo.com".into(),
                port: Some(1234)
            },
            name: "bar".into(),
            tag: Some("latest".into()),
            digest: None,
        }
    );

    assert_eq!(
        parse_str!(DockerReference, "foo.com:1234/bar:latest@abc123").unwrap(),
        DockerReference {
            host: Host::Other {
                name: "foo.com".into(),
                port: Some(1234)
            },
            name: "bar".into(),
            tag: Some("latest".into()),
            digest: Some("abc123".into()),
        }
    );
}

#[test]
fn parse_docker_reference_error() {
    parse_str!(DockerReference, "").unwrap_err();
    parse_str!(DockerReference, "foo/").unwrap_err();
    parse_str!(DockerReference, "foo.").unwrap_err();
    parse_str!(DockerReference, "foo:").unwrap_err();
    parse_str!(DockerReference, "foo@").unwrap_err();

    parse_str!(DockerReference, "foo@a.b").unwrap_err();
    parse_str!(DockerReference, "foo@a:b").unwrap_err();
    parse_str!(DockerReference, "foo@a/b").unwrap_err();
    parse_str!(DockerReference, "foo@a@b").unwrap_err();
    parse_str!(DockerReference, "foo/a.b").unwrap_err();
    parse_str!(DockerReference, "foo/a/b").unwrap_err();
    parse_str!(DockerReference, "foo.com:/bar").unwrap_err();

    parse_str!(DockerReference, "foo/bar/baz").unwrap_err();
    parse_str!(DockerReference, "foo:bar:baz").unwrap_err();
    parse_str!(DockerReference, "foo@abc123@abc345").unwrap_err();
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct LocalPath {
    pub path: Utf8PathBuf,
    pub reference: Option<String>,
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

#[test]
fn parse_local_path() {
    assert_eq!(
        parse_str!(LocalPath, "foo/bar/baz/").unwrap(),
        LocalPath {
            path: "foo/bar/baz".into(),
            reference: None
        }
    );
    assert_eq!(
        parse_str!(LocalPath, "foo/bar/baz/:abc").unwrap(),
        LocalPath {
            path: "foo/bar/baz".into(),
            reference: Some("abc".into())
        }
    );
    assert_eq!(
        parse_str!(LocalPath, "foo/bar/baz/:abc:def").unwrap(),
        LocalPath {
            path: "foo/bar/baz".into(),
            reference: Some("abc:def".into())
        }
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

#[test]
fn parse_image_name() {
    assert_eq!(
        parse_str!(ImageName, "docker://foo.com:124/bar:baz@abc123").unwrap(),
        ImageName::Docker(DockerReference {
            host: Host::Other {
                name: "foo.com".into(),
                port: Some(124)
            },
            name: "bar".into(),
            tag: Some("baz".into()),
            digest: Some("abc123".into()),
        })
    );

    assert_eq!(
        parse_str!(ImageName, "oci:/foo/bar:r:ef1").unwrap(),
        ImageName::Oci(LocalPath {
            path: "/foo/bar".into(),
            reference: Some("r:ef1".into())
        })
    );

    assert_eq!(
        parse_str!(ImageName, "oci-archive:/foo/bar:r:ef1").unwrap(),
        ImageName::OciArchive(LocalPath {
            path: "/foo/bar".into(),
            reference: Some("r:ef1".into())
        })
    );
}

#[test]
fn parse_image_name_err() {
    parse_str!(ImageName, "").unwrap_err();
    parse_str!(ImageName, "poci:foo").unwrap_err();
    parse_str!(ImageName, "docker://").unwrap_err();
}
