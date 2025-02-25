# Default Configuration

If there is no `cargo-maelstrom.toml` in the workspace root, then
`cargo-maelstrom` will run with the following defaults:

```toml
# Because it has no `filter` field, this directive applies to all tests.
[[directives]]

# This layer just includes files and directories for mounting the following
# file-systems and devices.
layers = [
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

# Forward the RUST_BACKTRACE and RUST_LIB_BACKTRACE environment variables.
# Later directives can override the `environment` key, but the `added_environment` key is only
# additive. By using it here we ensure it applies to all tests regardless of other directives.
[directives.added_environment]
RUST_BACKTRACE = "$env{RUST_BACKTRACE:-0}"
RUST_LIB_BACKTRACE = "$env{RUST_LIB_BACKTRACE:-0}"
```
