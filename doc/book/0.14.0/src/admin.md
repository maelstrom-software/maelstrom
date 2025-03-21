# `maelstrom-admin`

The `maelstrom-admin` program can be used to monitor and control the broker
from the command-line. It can be run on any machine as it communicates with the
broker over the network.

## Command-Line Options

`maelstrom-admin` supports the [common command-line options](common-cli.md), as
well as two [configuration values](#configuration-values), which are covered in
the next section.

## Configuration Values

`maelstrom-admin` supports the following [configuration values](config.md):

Value                                                  | Short Option | Type    | Description                                  | Default
-------------------------------------------------------|--------------|---------|----------------------------------------------|-----------------
`broker`                                               | `-b`         | string  | [address of broker](#broker)                 | must be provided
<span style="white-space: nowrap;">`log-level`</span>  | `-L`         | string  | [minimum log level](#log-level)              | `"info"`

### `broker`

The `broker` configuration value specifies the socket address of the broker.
This configuration value must be provided.

Here are some example value socket addresses:
  - `broker.example.org:1234`
  - `192.0.2.3:1234`
  - `[2001:db8::3]:1234`

### `log-level`

See [here](common-config.md#log-level).

`maelstrom-admin` always prints log messages to stderr.

## Subcommands

`maelstrom-admin` has two subcommands: `status` and `stop`. One of these must be specified.

### `status`

```bash
maelstrom-admin status
```

This subcommand will gather cluster status information from the broker and print it to standard output.

### `stop`

```bash
maelstrom-admin stop
```

This subcommand will tell the broker and all of the connected workers to exit.
This is useful in CI workflows when it's not possible to just send a signal to
the broker.
