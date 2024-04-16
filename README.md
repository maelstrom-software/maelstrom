![MaelstromLogo](https://github.com/maelstrom-software/maelstrom/assets/146376379/c95eb097-92b2-4895-a87c-c1d6e1ab5f90)

[![CI](https://github.com/maelstrom-software/maelstrom/actions/workflows/ci.yml/badge.svg)](https://github.com/maelstrom-software/maelstrom/actions/workflows/ci.yml)

Maelstrom is an extremely fast Rust test runner built on top of a general-purpose clustered job runner. Maelstrom packages your Rust tests into hermetic micro-containers, then distributes them to be run on an arbitrarily large cluster of test-runners.

* It's easy. Maelstrom functions as a drop-in replacement for cargo-test, so in most cases, it just works.
* It's reliable. Maelstrom runs every test hermetically in its own lightweight container and runs each test independently, eliminating confusing test errors caused by inter-test dependencies or by
hidden test-environment dependencies.
* It's scalable. Add more worker machines to linearly improve test throughput.
* It works everywhere. Maelstrom workers are rootless containers so you can run them securely, anywhere.
* It's clean. Maelstrom has a home-grown container implementation (not relying on docker or RunC), optimized to be low-overhead and start quickly.
* It's Rusty. The whole project is built in Rust.

Maelstrom is currently available for Rust on Linux. C++, Typescript, Python, and Java are coming soon.

See the book for more information:
[Maelstrom Book](https://maelstrom-software.com/book/)

# Design

![Architecture](https://github.com/maelstrom-software/maelstrom/assets/146376379/07209c96-b529-45b6-a215-8c0c1a713795)

Maelstrom is split up into a few different pieces of software.

* The Broker. This is the central brain of the clustered job runner. Clients and Workers connect to it.
* The Worker. There are one or many instances of these. This is what runs the actual job (or test.)
* The Client. There are one or many instances of these. This is what connects to the broker and submits jobs.
* cargo-maelstrom. This is our cargo test replacement which submits tests as jobs by acting as a client.

# Getting Started

## Installing the Broker
We will use cargo install the broker, but first we need to install some dependencies. First make sure you've installed Rust. Then install these other required things.

```
rustup target add wasm32-unknown-unknown
cargo install wasm-opt
```
Now we can install the broker

```
export GITHUB_URL="https://github.com/maelstrom-software/maelstrom.git"
cargo install --git $GITHUB_URL maelstrom-broker
```

It is best to not run the service as root, so we will create a new user to use for this purpose. The broker also uses the local file-system to cache artifacts so this will give us a place to put them.

```
sudo adduser maelstrom-broker
sudo mkdir ~maelstrom-broker/bin
sudo mv ~/.cargo/bin/maelstrom-broker ~maelstrom-broker/bin/
```

We need now to run the broker as a service. This guide will cover using Systemd to do this.

Create a service file at ``/etc/systemd/system/maelstrom-broker.service`` and fill it with the following contents.

```
[Unit]
Description=Maelstrom Broker

[Service]
User=maelstrom-broker
WorkingDirectory=/home/maelstrom-broker
ExecStart=/home/maelstrom-broker/bin/maelstrom-broker --http-port 9000 --port 9001
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

Now we install and start the broker

```
sudo systemctl enable maelstrom-broker
sudo systemctl start maelstrom-broker
```

The broker should be running now and you can confirm by using a web browser and navigating to the web interface running on the HTTP port.

The broker listens on two different ports which we have provided to it via command-line arguments. The HTTP port has a web interface we can use to monitor and interact with the broker. The other port is the port workers and clients will connect to.

The broker can be configured using CLI, environment variables, or configuration file, for more information see Broker Configuration

By default it stores its caches in ``<working-directory>/.cache/maelstrom-broker``. For the given set-up this should be ``/home/maelstrom-broker/.cache/maelstrom-broker``

## Installing the Worker

You are allowed to have as many worker instances as you would like. Typically you should install one per machine you wish to be involved in running jobs.

First make sure you've installed Rust.

Install the worker with

```
export GITHUB_URL="https://github.com/maelstrom-software/maelstrom.git"
cargo install --git $GITHUB_URL maelstrom-worker
```

It is best to not run the service as root, so we will create a new user to use for this purpose. The worker also uses the local file-system to cache artifacts so this will give us a place to put them.

```
sudo adduser maelstrom-worker
sudo mkdir ~maelstrom-worker/bin
sudo mv ~/.cargo/bin/maelstrom-worker ~maelstrom-worker/bin/
```

We need now to run the worker as a service. This guide will cover using Systemd to do this.

Create a service file at ``/etc/systemd/system/maelstrom-worker.service`` and fill it with the following contents.

```
[Unit]
Description=Maelstrom Worker

[Service]
User=maelstrom-worker
WorkingDirectory=/home/maelstrom-worker
ExecStart=/home/maelstrom-worker/bin/maelstrom-worker --broker <broker-machine-address>:9001
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

Replace ``<broker-machine-address>`` with the hostname or IP of the broker machine (or localhost if its running on the same machine). 9001 is the port we chose when setting up the broker.

Now we install and start the worker

```
sudo systemctl enable maelstrom-worker
sudo systemctl start maelstrom-worker
```

The worker should be running now. To make sure you can pull up the broker web UI and it should now show that there is 1 worker.

The worker can be configured using CLI, environment variables, or configuration file, for more information see Worker Configuration

By default it stores its caches in ``<working-directory>/.cache/maelstrom-worker``. For the given set-up this should be ``/home/maelstrom-worker/.cache/maelstrom-worker``

Repeat these steps for every machine you wish to install a worker on to.

# Licensing

This project is available under the terms of either the Apache 2.0 license or the MIT license.
