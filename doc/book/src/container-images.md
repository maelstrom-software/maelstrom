# Container Images

Maelstrom runs each job in its own container. By default, these containers are
minimal and are built entirely from files copied from the local machine. This
works well for compiled languages (like Rust), where job don't depend on
executing other programs. However, for interpreted languages (like Python), or
in situations where the job needs to execute other programs, Maelstrom provides a way to
create jobs' containers based off of a standard, OCI container images. These
container images can be retrieved from a container registry like [Docker
Hub](https://hub.docker.com/), or provided from the local machine.

## Container Image URIs

A container image is specified using the URI format defined by the
[Containers](https://github.com/containers/) project. In particular,
[here](https://github.com/containers/image/blob/main/docs/containers-transports.5.md)
is their specification of the URI format. We currently support the following
URI schemes.

### **docker://**_docker_reference_

This scheme indicates that the container image should be retrieved from a container
registry using the [Docker Registry HTTP API
V2](https://docker-docs.uclv.cu/registry/spec/api/). The container registry,
container name, and optional tags are specified in _docker_reference_.

_docker-reference_ has the format: _name_[`:`_tag_ | `@`_digest_]. If _name_
does not contain a slash, it is treated as `docker.io/library/`_name_.
Otherwise, the component before the first slash is checked to see if it is
recognized as a _hostname_[`:`_port_] (i.e., it contains either a `.` or a `:`,
or the component is exactly `localhost`). If the first component of name is not
recognized as a _hostname_[`:`_port_], _name_ is treated as `docker.io/`_name_.

[Here](https://github.com/containers/image/blob/main/docs/containers-transports.5.md#dockerdocker-reference)
is how the Containers project specifies this scheme.

### **oci:/**_path_[`:`_reference_]

This scheme indicates that the container images should be retrieved from a
local directory at _path_ in the format specified by the [Open Container Image
Layout
Specification](https://specs.opencontainers.org/image-spec/image-layout/).

Any characters after the first `:` are considered to be part of _reference_,
which is used to match an `org.opencontainers.image.ref.name` annotation in the
top-level index. If _reference_ is not specified when reading an archive, the
archive must contain exactly one image.

[Here](https://github.com/containers/image/blob/main/docs/containers-transports.5.md#ocipathreference)
is how the Containers project specifies this scheme.

### **oci-archive:/**_path_[`:`_reference_]

This scheme indicates that the container images should be retrieved from a tar
file at _path_ with contents in the format specified by the [Open Container
Image Layout
Specification](https://specs.opencontainers.org/image-spec/image-layout/).

Any characters after the first `:` are considered to be part of _reference_,
which is used to match an `org.opencontainers.image.ref.name` annotation in the
top-level index. If _reference_ is not specified when reading an archive, the
archive must contain exactly one image.

[Here](https://github.com/containers/image/blob/main/docs/containers-transports.5.md#oci-archivepathreference)
is how the Containers project specifies this scheme.

## Cached Container Images {#container-image-depot-root}

When a container image is specified, the client will first download or copy the
image into its cache directory. It will then use the internal bits of the [OCI
image](https://github.com/opencontainers/image-spec) &mdash; most importantly
the file-system layers &mdash; to create the container for the job.

The cache directory can be set with the `container-image-depot-root`
[configuration value](config.md). If this value isn't set, but `XDG_CACHE_HOME`
is set and non-empty, then `$XDG_CACHE_HOME/maelstrom/containers` will be used.
Otherwise, `~/.cache/maelstrom/containers` will be used. See the [XDG Base
Directories
specification](https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html)
for more information.

## Lock File

When a client first resolves a container registry tag, it stores the result in
a local lock file. Subsequently, it will use the exact image specified in the
lock file instead of resolving the tag again. This guarantees that subsequent
runs use the same images as previous runs.

The lock file is `maelstrom-container-tags.lock`, stored in the [project
directory](project-dir.md). It is recommended that this file be committed to
revision control, so that others in the project, and CI, use the same images
when running tests.

To update a tag to the latest version, remove the corresponding line from the
lock file and then run the client. This will force it to re-evaluate the tag
and store the new results.
