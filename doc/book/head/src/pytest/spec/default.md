# Default Configuration

If there is no `maelstrom-pytest.toml` in the project directory, then
`maelstrom-pytest.toml` will run with the following defaults:

```toml
# Because it has no `filter` field, this directive applies to all tests.
[[directives]]

image = "docker://python:3.12.3-slim"

# Use `added_layers` here since we want to add to what are provided by the image.
added_layers = [
    # This layer includes all the Python files from our project.
    { glob = "**.{py,pyc,pyi}" },
    # Include pyproject.toml if it exists.
    { glob = "pyproject.toml" },
    # This layer just includes files and directories for mounting the following
    # file-systems and devices.
    { stubs = [ "/{proc,sys,tmp}/", "/dev/{full,null,random,urandom,zero}" ] },
]

# Provide /tmp, /proc, /sys, and some devices in /dev/. These are used pretty
# commonly by tests.
mounts = [
    { type = "tmp", mount_point = "/tmp" },
    { type = "proc", mount_point = "/proc" },
    { type = "sys", mount_point = "/sys" },
    { type = "devices", devices = ["full", "null", "random", "urandom", "zero"] },
]

# Later directives can override the `environment` key, but the `added_environment` key is only
# additive. By using it here we ensure it applies to all tests regardless of other directives.
[directives.added_environment]
# If we are using the xdist plugin, we need to tell it we don't want any parallelism since we are
# only running one test per process.
PYTEST_XDIST_AUTO_NUM_WORKERS = "1"
```
