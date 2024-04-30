# Files in Target Directory

`cargo-maelstrom` stores a number of files in the workspace's target directory.
This chapter lists them and explains what they're for.

Except in the case of the [local worker](#local-worker), `cargo-maelstrom`
doesn't currently make any effort to clean up these files.

`cargo-maelstrom` also uses the [container-images
cache](../container-images.html). That cache is not stored in the target
directory, as it can be reused by different Maelstrom clients.

## Local Worker

When run in [standalone mode](../local-worker.md), the local worker stores its
files in `maelstrom-local-worker/` in the target directory. The
[`cache-size`](config.md#cache-size) configuration value indicates the target
size of this cache directory.

## Manifest Files

`cargo-maelstrom` uses "manifest files" for non-tar layers. These are like tar
files, but without the actual data contents. These files are stored in `maelstrom-manifests/` in the target directory.

## Client Log File

The local client process &mdash; the one that `cargo-maelstrom` talks to, and
that contains the local worker &mdash; has a log file that is stored at
`maelstrom-client-process.log` in the target directory.

## Test Listing

When `cargo-maelstrom` finishes, it updates a list of all of the tests in the
workspace. This is used to predict the amount of tests will be run in
subsequent invocations. This is stored in the `maelstrom-test-listing.toml`
file in the target directory.

## File Digests

Files uploaded to the broker are identified by a hash of their file contents.
Calculating these hashes can be time consuming so `cargo-maelstrom` caches this
information. This is stored in `maelstrom-cached-digests.toml` in the target directory.
