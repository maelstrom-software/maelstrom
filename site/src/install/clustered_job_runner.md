# Installing Clustered Job Runner

This covers setting up the clustered job runner. This is split into two
different parts.

- **The Broker**. This is the brains of the clustered job runner, clients and
  workers connect to it.
- **The Worker**. There are one or many of these running on the same or different
  machines from each other and the broker.

The broker and the worker only work on Linux. They both can be installed using
cargo.

## Installing the Broker

We will use cargo install the broker, but first we need to install some
dependencies. First make sure you've installed
[Rust](https://www.rust-lang.org/tools/install). Then install these other
required things.

```bash
rustup target add wasm32-unknown-unknown
cargo install wasm-opt
```

Now we can install the broker

```bash
export GITHUB_URL="https://github.com/maelstrom-software/maelstrom.git"
cargo install --git $GITHUB_URL maelstrom-broker
```

It is best to not run the service as root, so we will create a new user to use
for this purpose. The broker also uses the local file-system to cache artifacts
so this will give us a place to put them.

```bash
sudo adduser maelstrom-broker
sudo mkdir ~maelstrom-broker/bin
sudo mv ~/.cargo/bin/maelstrom-broker ~maelstrom-broker/bin/
```

We need now to run the broker as a service. This guide will cover using
[Systemd](https://systemd.io) to do this.

Create a service file at `/etc/systemd/system/maelstrom-broker.service` and
fill it with the following contents.

```language-systemd
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

```bash
sudo systemctl enable maelstrom-broker
sudo systemctl start maelstrom-broker
```

The broker should be hopefully be running now and you can confirm by using a web
browser and navigating to the web interface running on the HTTP port.

The broker listens on two different ports which we have provided to it via
command-line arguments. The HTTP port has a web interface we can use to monitor
and interact with the broker. The other port is the port workers and clients
will connect to.

The broker can be configured using CLI, environment variables, or configuration
file, for more information see [Broker
Configuration](../clustered_job_runner_management/broker.md)

By default it stores its caches in
`<working-directory>/.cache/maelstrom-broker`. For the given set-up this should
be `/home/maelstrom-broker/.cache/maelstrom-broker`

## Installing the Worker

You are allowed to have as many worker instances as you would like. Typically
you should install one per machine you wish to be involved in running jobs.

First make sure you've installed [Rust](https://www.rust-lang.org/tools/install).

Install the worker with

```bash
export GITHUB_URL="https://github.com/maelstrom-software/maelstrom.git"
cargo install --git $GITHUB_URL maelstrom-worker
```

It is best to not run the service as root, so we will create a new user to use
for this purpose. The worker also uses the local file-system to cache artifacts
so this will give us a place to put them.

```bash
sudo adduser maelstrom-worker
sudo mkdir ~maelstrom-worker/bin
sudo mv ~/.cargo/bin/maelstrom-worker ~maelstrom-worker/bin/
```

We need now to run the worker as a service. This guide will cover using
[Systemd](https://systemd.io) to do this.

Create a service file at `/etc/systemd/system/maelstrom-worker.service` and
fill it with the following contents.

```language-systemd
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

Replace `<broker-machine-address>` with the hostname or IP of the broker machine
(or localhost if its running on the same machine). 9001 is the port we chose
when setting up the broker.

Now we install and start the worker

```bash
sudo systemctl enable maelstrom-worker
sudo systemctl start maelstrom-worker
```

The worker should be running now. To make sure you can pull up the broker web UI
and it should now show that there is 1 worker.

The worker can be configured using CLI, environment variables, or configuration
file, for more information see [Worker
Configuration](../clustered_job_runner_management/worker.md)

By default it stores its caches in
`<working-directory>/.cache/maelstrom-worker`. For the given set-up this should
be `/home/maelstrom-worker/.cache/maelstrom-worker`

Repeat these steps for every machine you wish to install a worker on to.
