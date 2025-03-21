# Default Configuration

If there is no `maelstrom-go-test.toml` in the current directory, then
`maelstrom-go-test` will run with the following defaults:

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
```
