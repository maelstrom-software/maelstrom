# Introduction

The intent of this document is to elaborate on a proposed set of syntax changes to
support command layers. This document will only look at the proposed changes to
the various test-runner TOML files. It won't discuss `maelstrom-run`'s file
format, the client protocol or any implementation details.

# Goals

We want users to be able to simply specify the simple things, but also somehow
be able to specify the complex things. In other words: make the simple things
simple and the complex things possible.

At the simplest end of the spectrum, we want a user to be able to run a command
to be run in every container before tests are run. An example of this is
running `pip install`.

At the complex end of the spectrum, we want a user to be able to create layers
using certain underlying layers, but not have those underlying layers available
in the test containers. For example, a user may want to use `pip` to create
some layers, but not have `pip` itself available in tests' containers.

# Command Layer Type

We propose introducing a new layer type to the TOML files for test runners: the
command layer. In its simplest form it would look like this:
```toml
layers = [
    { command = "a-command-with-no-arguments" },
    { command = ["a-command-with-arguments", "first-argument", "second argument"]},
]
```

In its more complex form, each command layer could have these optional fields:
  - `network`
  - `mounts` or `added_mounts`, but not both
  - `environment` or `added_environment`, but not both
  - `working_directory`

Command layers can be thought of as being evaluated when a container
specification is created for a test. When the container specification is built
for a test, the test runner starts at the top of the toml file and looks for
directives that match the test. If a matching directive is found, its changes
are applied to the container specification. This is the time the command layer
will be executed. In particular:

  1. The rest of the directive will be applied to the container being built.
     This includes `mounts`, `environment`, etc.
  2. Any layers underneath the command layer will be applied.
  3. Any "local" changes from the command layer will be applied to the
     container. These come from the `network`, `mounts`, etc. fields that may
     be specified with the command.
  4. The command will be run with a writable root file system. Any changes to
     the root file system will be captured in a layer. This is the command layer.

Command layers will be cached, so that the command layer that has the same
inputs (i.e. container specification it is run in) as a previous one won't be
re-evaluated. We will discuss this more layer.

Note that a single command layer in a directive may be re-evaluated many times
in a single test run if it is applied to different lower containers.

# Container Specifications

We proposing adding a new specification type. In addition to `directives`,
which is a list of directives, we propose adding a top-level `containers`
field. This would be a table instead of a list. Each individual container
specification would be just like the current directive, except it wouldn't
allow the following fields:
  - `filter`
  - `include_shared_libraries`
  - `timeout`
  - `ignore`
  - `container` (see below)

A container would have an additional optional `base` field, which would be
mutually exclusive with `image`. If `base` is specified, it must be the name of
another container specified in the `conatainers` table. Containers in the
`containers` table must form a directed acyclic graph, though they can be
specified in the TOML file in any order.

If `base` is specified, then the container specification is built by starting
with `base`, then adding the fields specified for the container. Fields that
start with `added_` will append to the container in the same way they do for
directives. Fields that don't will override whatever is specified in the `base`
container.

# Container Directive Field

We propose adding a new field to directives: `container`. This field is
mutually exclusive with `image`. It must be a valid name from the `containers`
table. When provided, the directive specifies that the container for the test
should be built by starting with the named container and then applying the
fields from the directive, in the same way as is done with images.

This field will use everything from the provided container, unless explicitly
overwritten in the directive. In the future, we may add support for a `use`
subfield, like we do for `image`, to selectively inherit only parts of the
container.

If a directive with the `image` field is applied to a test's metadata that
already has a `container` field, it replaces the `container` field. Likewise,
if a `container` field is applied to a test's metadata fhat already has an
`image` field, it replaces the `image` field.

# Container Layer Type

We propose adding another new layer type: `container`. This is really a
"pseudo-layer", as it may expand to multiple layers. It looks like this:
```toml
layers = [
    { container = "container-1" },
    { container = "container-2", recursive = true },
]
```

In the first use, with out the `recursive` field set to `true`, it expands to
only those layers explicitly defined in the named container. In the second
form, with `recursive` set to `true`, it includes all layers from the
container, including inherited layers from `base` and `image` fields.

If the `container` layer type is used in a container directive, it must not
result in a cycle.

# The `include_shared_libraries` Pseudo-Field

We have to define the default value for the `include_shared_libraries`
pseudo-field in the face of the `container` directive field.

We will say a container specified in the `containers` field is "based on an
image" if it's `image` field is set, or if it has a `base` image that is based
on an image.

We will say a test's container is "based on an image" if it has an `image`
field, or if it has a `container` field, and the referred-to container is based
on an image.

Then, we say that the default value for `include_shared_libraries` is `true` if
an only if the test's container is not based on a image.

# Caching

We want to avoid re-evaluating command layers both within a single execution of
a test runner, as well as between executions. To do so, we need to come up with
a caching scheme that will let us reuse already-built command layers.

To that end, we introduce the concepts of "expressionization" and "input
caching", and show how they will be used to implement caching.

Named containers, and more specifically the "pointer concept they introduce,
means that a container specification is no longer just a simple tree, but
instead is a directed, acyclic graph (DAG). We can imagine creating an
expression language to define a tree, but not a DAG. However, for our purposes,
it will be sufficient to transform any given DAG into a tree by copying the the
referred-to portion. We can then imagine this being a single expression.

For example, imagine that we had the following:
```
A = 2 * 3
B = A + 21
C = A - 10
D = B * C
```

To "expressionize" `D`, we would recursively expand things to get:
```
D = (A + 21) * (A - 10)
D = ((2 * 3) + 21) * ((2 * 3) - 10)
```

At this point, `D` is an expression tree.

We can imagine doing the same thing for any given container specification. We
could expand any container specification to an abstract syntax tree (AST).
Command layers would result in nested container specifications.

We can then define an input hash operation that takes an AST and turns it into
a checksum. For each type of AST node type, we define how the input hash is
computed, assuming all sub-expressions can be recursively turned into input
hashes. The result is that we can come up with a single input hash for a given
container specification.

There are some details about how this input hash is computed. Mostly, it'll be
based on the actual syntax of the specification, but when it comes to layers
that inspect the file system, it will have to check the file system.

So, on to some specifics. For `image` fields, this will be the checksum already
stored in the `maelstrom-container-tags.lock` file.

For command layers, it will be computed by hashing the input parameters to the
command layer along with the input container.

For stubs and symlinks layers, the hash will just be derived from the layer specification.

For tar, paths, and shared-library-dependencies layers, the hash will be
derived from the specification as well as the actual values of the referenced
files.

For glob layers, the hash will be derived from the specifiation's
`PrefixOptions` as well as the actual files' contents and their locations in
the the file system. If two input hashes match for a glob layer, then the two
glob layers must evaluate to the same set of files with the same contents. For
glob layers, it's probably okay to rely on the order provided from the file
system, as long as it's relatively stable. It's okay to get false negatives: we
just will have to recompute the layer. We can't accept false positives.

For any layer that references files in the file system, the we can additionally
cache results for the a single run of the test runner. In other words, we can
cache the input hashes of these layers within the context of a single process.
We don't worry about files changing out from under the test runner.
