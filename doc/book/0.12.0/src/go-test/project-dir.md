# Files in Project Directory

<span style="white-space: nowrap;">`maelstrom-go-test`</span> stores a number of
files in the [project directory](../../dirs.md#project-directory), under the
`.maelstrom-go-test` subdirectory. This chapter lists them and explains what
they're for.

It is safe to remove this directory whenever <span style="white-space:
nowrap;">`maelstrom-go-test`</span> isn't running.

Except in the case of the [local worker](#local-worker), <span
style="white-space: nowrap;">`maelstrom-go-test`</span> doesn't currently make
any effort to clean up these files. However, the total space consumed by these
files should be pretty small.

<span style="white-space: nowrap;">`maelstrom-go-test`</span> also uses the
[container-images cache](../container-images.html). That cache is not stored in
the target directory, as it can be shared by different Maelstrom clients.

## Local Worker

The [local worker](../local-worker.md) stores its cache in <span
style="white-space: nowrap;">`.maelstrom-go-test/cache/local-worker/`</span> in the
project directory. The [<span style="white-space:
nowrap;">`cache-size`</span>](config.md#cache-size) configuration value
indicates the target size of this cache directory.

## Manifest Files

<span style="white-space: nowrap;">`maelstrom-go-test`</span> uses "manifest
files" for non-tar layers. These are like tar files, but without the actual
data contents. These files are stored in `.maelstrom-go-test/cache/manifests/` in the
project directory.

## File Digests

Files uploaded to the broker are identified by a hash of their file contents.
Calculating these hashes can be time consuming so <span style="white-space:
nowrap;">`maelstrom-go-test`</span> caches this information. This cache is stored
in <span style="white-space:
nowrap;">`.maelstrom-go-test/cache/cached-digests.toml`</span> in the project directory.

## Client Log File

The local client process &mdash; the one that <span style="white-space:
nowrap;">`maelstrom-go-test`</span> talks to, and that contains the local worker
&mdash; has a log file that is stored at <span style="white-space:
nowrap;">`.maelstrom-go-test/state/client-process.log`</span> in the project directory.

## Test Listing

When <span style="white-space: nowrap;">`maelstrom-go-test`</span> finishes, it
updates a list of all of the tests in the workspace, and how long they took to
run. This is used to predict the number of tests that will be run in subsequent
invocations, as well as how long they will take. This is stored in the <span
style="white-space: nowrap;">`.maelstrom-go-test/state/test-listing.toml`</span> file in
the project directory.

## Test Binaries

<span style="white-space: nowrap;">`maelstrom-go-test`</span> builds go binaries and puts them in
<span style="white-space: nowrap;">`.maelstrom-go-test/cache/test-binaries`</span>. These need to
exist here so we can either run them or upload them. They are organized in a directory structure
that mirrors your project directory.
