# Standard Command-Line Options

All Maelstrom programs support a standard base set of command-line options.

## `--help`

The `--help` (or `-h`) command-line option will print out the program's
command-line options, configuration value (including their associated
environment variables)s, and configuration-file search path, then exit.

## `--version`

The `--version` (or `-v`) command-line option will cause the program to print
its software version, then exit.

## `--print-config`

The `--print-config` (or `-P`) command-line option will print out all of the
program's configuration values, then exit. This can be useful for validating
configuration.

## `--config-file`

The `--config-file` (or `-c`) command-line option is used to specify a specific
configuration file, or specify that no configuration file should be used. See
[here](config.md#config-file) for more details.
