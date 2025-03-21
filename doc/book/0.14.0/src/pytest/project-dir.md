# Files in Project Directory

<span style="white-space: nowrap;">`maelstrom-pytest`</span> stores a number of
files in the [project directory](../../dirs.md#project-directory), under the
`.maelstrom-pytest` subdirectory. This chapter lists them and explains what
they're for.

It is safe to remove this directory whenever <span style="white-space:
nowrap;">`maelstrom-pytest`</span> isn't running.

Except in the case of the [local worker](#local-worker), <span
style="white-space: nowrap;">`maelstrom-pytest`</span> doesn't currently make
any effort to clean up these files. However, the total space consumed by these
files should be pretty small.

<span style="white-space: nowrap;">`maelstrom-pytest`</span> also uses the
[container-images cache](../container-images.html). That cache is not stored in
the target directory, as it can be shared by different Maelstrom clients.

## Local Worker

The [local worker](../local-worker.md) stores its cache in <span
style="white-space: nowrap;">`.maelstrom-pytest/cache/local-worker/`</span> in the
project directory. The [<span style="white-space:
nowrap;">`cache-size`</span>](config.md#cache-size) configuration value
indicates the target size of this cache directory.

## Manifest Files

<span style="white-space: nowrap;">`maelstrom-pytest`</span> uses "manifest
files" for non-tar layers. These are like tar files, but without the actual
data contents. These files are stored in `.maelstrom-pytest/cache/manifests/` in the
project directory.

## File Digests

Files uploaded to the broker are identified by a hash of their file contents.
Calculating these hashes can be time consuming so <span style="white-space:
nowrap;">`maelstrom-pytest`</span> caches this information. This cache is stored
in <span style="white-space:
nowrap;">`.maelstrom-pytest/cache/cached-digests.toml`</span> in the project directory.

## Test Listing

When <span style="white-space: nowrap;">`maelstrom-pytest`</span> finishes, it
updates a list of all of the tests in the workspace, and how long they took to
run. This is used to predict the number of tests that will be run in subsequent
invocations, as well as how long they will take. This is stored in the <span
style="white-space: nowrap;">`.maelstrom-pytest/state/test-listing.toml`</span> file in
the project directory.
