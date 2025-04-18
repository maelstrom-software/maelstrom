+++
title = "About Maelstrom"
template = "index.html"
+++

Maelstrom packages your tests into micro-containers, then distributes them to be run on an
arbitrarily large cluster of test-runners, or locally on your machine using a custom-built, super-fast container runtime. 

<a href="https://maelstrom-software.com/doc/book/latest/" class="rightimg wrap"><img src="images/Architecture Small.png" alt="" /></a>

You might use Maelstrom to run your tests because:

- **It's easy.** Maelstrom functions as a drop-in replacement for `cargo-test`, `go test`, and
  `pytest`. In most cases, it just works with your existing tests with minimal configuration.
- **It's reliable.** Maelstrom runs every test isolated in its own lightweight container and runs
  each test independently, eliminating errors caused by inter-test or implicit test-environment
  dependencies.
- **It's scalable.** You can run Maelstrom as a cluster &mdash; add more worker machines to linearly
  increase test throughput.
- **It's clean.** Maelstrom has a from-scratch, rootless container implementation (not relying on
  docker or RunC) written in Rust, optimized to be low-overhead and start quickly.
- **It's fast.** In most cases, Maelstrom is faster than `cargo test` or `go test`, even without
  adding clustering.  Maelstrom’s test-per-process model is inherently slower than `pytest`’s
  shared-process model, but Maelstrom provides test isolation at a low performance cost.

Maelstrom is currently available for Rust, Go, and Pytest on Linux.  C++,
Typescript, and Java are coming soon.

While our focus has been on running tests, Maelstrom's underlying job execution system is
general-purpose. We provide a command line utility to run arbitrary commands, as well a gRPC-based
API and Rust bindings for programmatic access and control.

<p>See the book for more information:</p>
<ul class="actions">
  <li><a href="https://maelstrom-software.com/doc/book/latest/" class="button">Maelstrom Book</a></li>
</ul>
</article>
</div>
</div>

<!-- Promo -->
<div id="promo-wrapper">
<section id="promo">
<a href="https://maelstrom-software.com/doc/book/latest/installation.html" class="image promo"><img src="images/Maelstrom.gif" alt=""></a>
<ul class="actions major">
</section>
</div>

<!-- Overview -->
<div class="wrapper">
<div class="container" id="main">

<!-- Content -->
<article id="content">
<header>
  <h2>Getting Started</h2>
</header>
<p><h3><b>Installing Pre-Built Binaries</b></h3></p>

To run your tests using Maelstrom, you need a test-runner binary.
The easiest way to get it is using [`cargo-binstall`.](https://github.com/cargo-bins/cargo-binstall)

For Rust tests:

```sh
cargo binstall cargo-maelstrom
```

For Go tests:

```bash
cargo binstall maelstrom-go-test
```

For Python tests:

```sh
cargo binstall maelstrom-pytest
```

This will install a pre-built binary from the [github releases
page](https://github.com/maelstrom-software/maelstrom/releases). If you don't have
`cargo-binstall`, you can download the binary manually.

Check out [the book](https://maelstrom-software.com/doc/book/latest/installation.html) for more ways
to install Maelstrom.

<p><h3><b>Running <tt>cargo-maelstrom</tt></b></h3></p>

To run your Rust tests, use `cargo-maelstrom`:

```sh
cargo maelstrom
```

This runs in "standalone" mode, meaning all tests are run locally. Each test is run in its own
container, configured with a few common dependencies. It may work for your project without any
further configuration.

If some tests fail, however, it likely means those tests have dependencies on their execution
environment that aren't packaged in their containers. You can remedy this by adding directives to
the `cargo-maelstrom.toml` file. To do this, run:

```sh
cargo maelstrom --init
```

Then edit the created `cargo-maelstrom.toml` file as described [in the
book](https://maelstrom-software.com/doc/book/latest/cargo-maelstrom/spec.html)

<p><h3><b>Running <tt>maelstrom-go-test</tt></b></h3></p>

To run your Go tests, use `maelstrom-go-test`:

```sh
maelstrom-go-test
```

This runs in "standalone" mode, meaning all tests are run locally. Each test is run in its own
container, configured with a few common dependencies. It may work for your project without any
further configuration.

If some tests fail, however, it likely means those tests have dependencies on their execution
environment that aren't packaged in their containers. You can remedy this by adding directives to
the `maelstrom-go-test.toml` file. To do this, run:

```sh
maelstrom-go-test --init
```

Then edit the created `maelstrom-go-test.toml` file as described [in the
book](https://maelstrom-software.com/doc/book/latest/go-test/spec.html)

<p><h3><b>Running <tt>maelstrom-pytest</tt></b></h3></p>
Before running tests, we need to do a little setup.

<p><h4><b>Choosing a Python Image</b></h4></p>
First generate a `maelstrom-pytest.toml` file

```bash
maelstrom-pytest --init
```

Then update the file to include a python image
```toml
[[directives]]
image = "docker://python:3.11-slim"
```
This example installs an [image from Docker](https://hub.docker.com/_/python)

<p><h4><b>Including Your Project Python Files</b></h4></p>
So that your tests can be run from the container, your project's python must be included.

```toml
added_layers = [ { glob = "**.py" } ]
```

This example just adds all files with a `.py` extension. You may also need to include
`.pyi` files or other files.

<p><h4><b>Including <tt>pip</tt> Packages</b></h4></p>
If you have an image named "python", `maelstrom-pytest` will automatically include pip
packages for you as part of the container. It expects to read these packages from a
test-requirements.txt file in your project directory. This needs to at a minimum include the pytest
package

<p><h4><b>Running Tests</b></h4></p>
Once you have finished the configuration, you only need invoke `maelstrom-pytest` to run all
the tests in your project. It must be run from an environment where `pytest` is in the Python
path. If you are using virtualenv for your project make sure to source that first.

`test-requirements.txt`.

```python
pytest==8.1.1
```

Now we are ready to try to run tests. Just invoke `maelstrom-pytest`:
```sh
maelstrom-pytest
```

This runs in "standalone" mode, meaning all tests are run locally. Each test is run in its own
container.

<p><h4><b>Setting Up a Cluster</b></h4></p>

To get even more out of Maelstrom, you can set up a cluster to run your tests on. You will need to
run one copy of the broker (`maelstrom-broker`) somewhere, and one copy of the worker
(`maelstrom-worker`) on each node of the cluster.

You can install these using [multiple methods](https://maelstrom-software.com/doc/book/latest/installation.html), including `cargo-binstall`:

```sh
cargo binstall maelstrom-worker maelstrom-broker
```

Then you can start the broker:
```sh
maelstrom-broker --port=1234
```

Then a few workers:
```sh
maelstrom-worker --broker=broker-host:1234
```

Then run `cargo-maelstrom` against the cluster:
```sh
cargo maelstrom --broker=broker-host:1234
```

See the book for more information:
<ul class="actions">
  <li><a href="https://maelstrom-software.com/doc/book/latest/" class="button">Maelstrom Book</a></li>
</ul>
