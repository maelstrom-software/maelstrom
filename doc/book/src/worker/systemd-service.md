# Running as systemd Service

You may choose to run `maelstrom-worker` in the background as a
[systemd](https://systemd.io/) service. This chapter covers one way to do that.

The `maelstrom-worker` does not need to run as root. Given this, we can
create a non-privileged user to run the service:

```bash
sudo adduser --disabled-login --gecos "Maelstrom Worker User" maelstrom-worker
sudo -u maelstrom-worker mkdir ~maelstrom-worker/cache
sudo -u maelstrom-worker touch ~maelstrom-worker/config.toml
sudo cp ~/.cargo/bin/maelstrom-worker ~maelstrom-worker/
```

This assumes the `maelstrom-worker` binary is installed in `~/.cargo/bin/`.

Next, create a service file at `/etc/systemd/system/maelstrom-worker.service`
and fill it with the following contents:

```language-systemd
[Unit]
Description=Maelstrom Worker

[Service]
User=maelstrom-worker
WorkingDirectory=/home/maelstrom-worker
ExecStart=/home/maelstrom-worker/maelstrom-worker \
    --config-file /home/maelstrom-worker/config.toml
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

Next, edit the file at `/home/maelstrom-worker/config.toml` and fill it with
the following contents:

```toml
broker = "<broker-machine-address>:<broker-port>"
cache-root = "/home/maelstrom-worker/cache"
```

The `<broker-machine-address>` and `<broker-port>` need to be substituted with
their actual values. You can add other [configuration values](config.md) as you
please.

Finally, enable and start the worker:

```bash
sudo systemctl enable maelstrom-worker
sudo systemctl start maelstrom-worker
```

The worker should be running now. If you want, you can verify this by pulling
up the broker web UI and checking the worker count, or by looking at the
broker's log messages.
