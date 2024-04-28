# Running as systemd Service

You may choose to run `maelstrom-broker` in the background as a
[systemd](https://systemd.io/) service. This chapter covers one way to do that.

The `maelstrom-broker` does not need to run as root. Given this, we can create
a non-privileged user to run the service:

```bash
sudo adduser --disabled-login --gecos "Maelstrom Broker User" maelstrom-broker
sudo -u maelstrom-broker mkdir ~maelstrom-broker/cache
sudo -u maelstrom-broker touch ~maelstrom-broker/config.toml
sudo cp ~/.cargo/bin/maelstrom-broker ~maelstrom-broker/
```

This assumes the `maelstrom-broker` binary is installed in `~/.cargo/bin/`.

Next, create a service file at `/etc/systemd/system/maelstrom-broker.service`
and fill it with the following contents:

```language-systemd
[Unit]
Description=Maelstrom Broker

[Service]
User=maelstrom-broker
WorkingDirectory=/home/maelstrom-broker
ExecStart=/home/maelstrom-broker/maelstrom-broker \
    --config-file /home/maelstrom-broker/config.toml
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

Next, edit the file at `/home/maelstrom-broker/config.toml` and fill it with
the following contents:

```toml
port = 9000
http-port = 9001
cache-root = "/home/maelstrom-broker/cache"
```

You can add other [configuration values](config.md) as you please.

Finally, enable and start the broker:

```bash
sudo systemctl enable maelstrom-broker
sudo systemctl start maelstrom-broker
```

The broker should be running now. If you want, you can verify this by
attempting to pull up the web UI, or by verifying the logs messages with
`journalctl`.
