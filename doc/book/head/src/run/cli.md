# Command-Line Options

In addition the command-line options for used to specify [configuration
values](../config.md) described in the [previous chapter](config.md),
`maelstrom-run` supports these command-line options:

Option                                                     | Short Alias | Argument    | Description
-----------------------------------------------------------|-------------|-------------|------------
<span style="white-space: nowrap;">`--help`</span>         | `-h`        |             | [print help and exit](../common-cli.md#--help)
<span style="white-space: nowrap;">`--version`</span>      |             |             | [print version and exit](../common-cli.md#--version)
<span style="white-space: nowrap;">`--print-config`</span> | `-P`        |             | [print all configuration values and exit](../common-cli.md#--print-config)
<span style="white-space: nowrap;">`--config-file`</span>  | `-c`        | path or `-` | [file to read configuration values from](../common-cli.md#--config-file)
<span style="white-space: nowrap;">`--file`</span>         | `-f`        | path        | [read job specifications from the provided file](#--file)
<span style="white-space: nowrap;">`--one`</span>          | `-1`        |             | [run in "one" mode](#--one)                                                  
<span style="white-space: nowrap;">`--tty`</span>          | `-t`        |             | [run in "TTY" mode](#--tty)                                                  

## `--file`

Read job specifications from the provided file instead of from standard input.

## `--one`

Run in ["one" mode](../run.md#one-mode).

This flag conflicts with [`--tty`](#--tty).

The job specifications to run can be provided with [`--file`](#--file) or on standard
input. If provided on standard input, `maelstrom-run` will stop reading once it
has read one complete job specification. If multiple job specifications are
provided with `--file`, only the first one is used: the rest are discarded.

If any positional command-line arguments are provided, they will replace the
[`program`](../spec.md#program) and [`arguments`](../spec.md#arguments) fields
of the provided job specification.


## `--tty`

Run in [TTY mode](../run.md#tty-mode).

This flag conflicts with [`--one`](#--one).

The job specifications to run can be provided with [`--file`](#--file) or on standard
input. If provided on standard input, `maelstrom-run` will stop reading once it
has read one complete job specification. If multiple job specifications are
provided with `--file`, only the first one is used: the rest are discarded.

If any positional command-line arguments are provided, they will replace the
[`program`](../spec.md#program) and [`arguments`](../spec.md#arguments) fields
of the provided job specification.

After the job specification has been read, `maelstrom-run` will start the job
and attempt to connect to its TTY. Once that happens, the program will take
over the local terminal in the same way SSH does, and will just forward data
between the local terminal and the job's terminal, and vice versa.
