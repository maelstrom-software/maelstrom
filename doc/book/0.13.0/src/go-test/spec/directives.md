# Directives

Directives are the other type of elements in `maelstrom-go-test.toml`, besides
[`containers`](containers.md).

The purpose of directives is to map individual tests to their corresponding
settings, that is, [job specifications](../../spec.md#job-specification).
As there may be thousands of tests in a single project, we need a system that
allows for providing settings for a broad swath of tests, while at the same
time allowing for individual configuration when required. Directives is how we
chose to solve the problem.

Every directive has a filter: a pattern that tests either match against or
don't. To arrive at the job spec for a job, `maelstrom-go-test` starts with a
default _candidate_ job spec, then inspects each directive in order. If the
directive's filter matches the test, then the directive is applied to the
candidate job spec. The process does not stop after a single match, but instead
proceeds through all directives, in order.

<a name="post-processing"></a>After `maelstrom-go-test` has determined the final
candidate job spec, it does some post processing to arrive at the final job
spec to be used for the test.

First, it looks at the value of the
[`include_shared_libraries`](#include_shared_libraries) pseudo-field. This is a
field that only exists for the candidate job spec. If it is set to `true`, then
`maelstrom-go-test` will add a
[`shared-library-dependencies`](../../spec-layers.md#sharedlibrarydependencies)
layer for the test binary.

Then, it adds one more [`paths`](../../spec-layers.md#paths) layer with the
test binary placed in the root directory.

## `filter`

```toml
[[directives]]
filter = "package.equals(maelstrom-software.com/maelstrom/go-test)"
image.name = "docker://golang"
image.use = ["layers", "environment"]
```

This field must be a string, which is interpreted as a [test filter
pattern](../filter.md). The directive only applies to tests that match the
filter. If there is no `filter` field, the directive applies to all tests.

Sometimes it is useful to use multi-line strings for long patterns:

```toml
[[directives]]
filter = """
package.equals(maelstrom-client) ||
package.equals(maelstrom-client-process) ||
package.equals(maelstrom-container) ||
package.equals(maelstrom-fuse) ||
package.equals(maelstrom-util)"""
layers = [{ stubs = ["/tmp/"] }]
mounts = [{ type = "tmp", mount_point = "/tmp" }]
```

A directive without an explicit filter matches every test.

## `ignore`

```toml
[[directives]]
filter = "name.contains(broken_test)"
ignore = true
```

This field specifies that any tests matching the directive should not be run.
`maelstrom-go-test` prints ignored tests, with a special "ignored" state. When
tests are listed, ignored tests are listed normally.

## `timeout`

```toml
[[directives]]
timeout = 60
```

This field sets the [`timeout`](../../spec.md#timeout) field of the
job spec. It must be an unsigned, 32-bit integer.

Setting the timeout to `0` results in no timeout.

By default, tests don't have a timeout.

There is also a [`timeout` configuration value](../config.md#timeout). If set
set, the configuration value overrides the directive's field.

## `include_shared_libraries`

```toml
[[directives]]
include_shared_libraries = true
```

This boolean field sets the `include_shared_libraries` pseudo-field in the
candidate job spec. See [above](#post-processing) for more details.

If `include_shared_libraries` is not explicitly set, then a heuristic is used
to determine the default value. The value will be `true` unless the candidate
job spec's ancestor chain ends in an [image](containers.md#image).

The thinking is that if your container is based off of an image, then
the image probably has the necessary shared libraries, and you don't want
`maelstrom-go-test` overwriting them with the shared libraries from the client
machine.

Since go tests are usually statically linked, this flag is unlikely to have any
affect. However, if `include_shared_libraries` is set, `maelstrom-go-test` will
still look for shared library dependencies just in case.

# Container Specification Fields

Each job spec has an embedded container spec. As a result, all of the
[`containers` fields](containers.md) are also valid in directives. However,
things can get tricky with the inheritance of container specs as well as the
application of directives. We've decided on a set of rules that we hope makes
thing powerful but yet understandable.

## Setting `parent` or `image`

When a directive specifies the [`parent`](containers.md#parent) or
[`image`](containers.md#image) field, then **all other container spec fields
are reset**. For example:

```toml
[[directives]]
user = 42
added_layers = [{ stubs = ["/tmp/"] }]
added_mounts = [{ type = "tmp", mount_point = "/tmp" }]
timeout = 10

[[directives]]
filter = "package.equals(maelstrom-software.com/maelstrom/go-test) && test.contains(Integration)"
image = "docker://golang"
```

In this case, any test that matches the second directive will have its
`layers`, `mounts`, and `user` reset. However, it would retain the `timeout`,
since `timeout` isn't a container spec field.

When either `parent` or `image` are provided, then all of the [container spec
fields](containers.md) have their same meanings and restrictions as they do
when specifying a named container. In particular, setting vector fields will
act just as described [here](containers.md#vector-fields).

## Without `parent` or `image`

A directive can still set container spec fields even if it doesn't include the
`parent` or `image` fields. However, in this case, the interpretation of [vector
fields](containers.md#vector-fields) is different.

The [`added_layers`](containers.md#added_layers),
[`added_environment`](containers.md#added_environment), or
[`added_mounts`](containers.md#added_mounts)  are
always allowed in this case. They will append to relevant container spec field,
regardless of whether the container spec inherits the field from a parent.

Similarly, the [`layers`](containers.md#layers),
[`environment`](containers.md#environment), and
[`mounts`](containers.md#mounts) are always allowed as well. They will
overwrite the relevant container spec field, without changing the how the
container spec inherits the field from a parent. **This last part will probably
change.** It's likely that in an upcoming release we will change the behavior
in this edge case so that container spec will also be modified so that it
doesn't inherit the relevant field from its parent.
