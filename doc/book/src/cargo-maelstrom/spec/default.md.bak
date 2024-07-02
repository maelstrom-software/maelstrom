# Default Configuration

If there is no `maelstrom-test.toml` in the workspace root, then
`cargo-maelstrom` will run with the following defaults:

```toml
# Because it has no `filter` field, this directive applies to all tests.
[[directives]]

# Copy any shared libraries the test depends on along with the binary.
include_shared_libraries = true

# This layer just includes files and directories for mounting the following
# file-systems and devices.
layers = [
    { stubs = [ "/{proc,sys,tmp}/", "/dev/{full,null,random,urandom,zero}" ] },
]

# Provide /tmp, /proc, /sys. These are used pretty commonly by tests.
mounts = [
    { type = "tmp", mount_point = "/tmp" },
    { type = "proc", mount_point = "/proc" },
    { type = "sys", mount_point = "/sys" },
]

# Mount these devices in /dev/. These are used pretty commonly by tests.
devices = ["full", "null", "random", "urandom", "zero"]
```
