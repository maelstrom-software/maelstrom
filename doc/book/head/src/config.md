# Configuration Values

All Maelstrom [programs](programs.md) are configured through "configuration
values". Configuration values can be set through command-line options,
environment variables, or configuration files.

Each configuration value has a type, which is either: string, number, boolean,
or list of strings.

Imagine a hypothetical configuration value named `config-value` in a
hypothetical program called `maelstrom-prog`. This configuration value could be
specified via:
  - The `--config-value` command-line option.
  - The `MAELSTROM_PROG_CONFIG_VALUE` environment variable.
  - The `config-value` key in a configuration file.

## Command-Line Options

Configuration values set on the command line have the highest priority: they
override settings from environment variables or configuration files.

Type             | Example
-----------------|----------------------
string           | `--frob-name=string`
string           | `--frob-name string`
number           | `--frob-size=42`
number           | `--frob-size 42`
boolean          | `--enable-frobs`
list as flags    | `--extra-args=some --extra-args=list --extra-args=elements`
list as var-args | `-- some list elements`

There is currently no way to set a boolean configuration value to `false` from
the command line.

### Lists

There are two types of list configuration values: those specified using flags
like `--foo`, and those specified as "var-args", after a `--`. The most
commonly used list configuration value will be specified using var-args, and the
rest will use flags.

For example, imagine `maelstrom-prog` has two list configuration values:
`args-to-foo` and `args-to-bar` that are used to pass extra arguments to the
commands `foo` and `bar`. Imagine `args-to-foo` is the most common, so it's
specified using var-args, while `args-to-bar` uses flags. We could invoke
`maelstrom-prog` like this:

```
maelstrom-prog --args-to-bar=bar-arg-1 --args-to-bar bar-arg-2 -- foo-arg-1 foo-arg-2
```

There is currently no way to explicitly set an empty list value on the command
line.

## Environment Variables

Configuration values set via environment variables have intermediate priority:
they override settings from configuration files, but are overridden by
command-line options.

The environment variable name is created by converting the configuration value
to ["screaming snake case"](https://en.wikipedia.org/wiki/Snake_case), and
prepending a program-specific prefix. Image that we're evaluating configuration
values for a program called `maelstrom-prog`:

Type    | Example
--------|----------------------
string  | `MAELSTROM_PROG_FROB_NAME=string`
number  | `MAELSTROM_PROG_FROB_SIZE=42`
boolean | `MAELSTROM_PROG_ENABLE_FROBS=true`
boolean | `MAELSTROM_PROG_ENABLE_FROBS=false`
list    | `MAELSTROM_PROG_FROBS="some list elements"`

Note that quotation marks aren't expected for string values. The string value
will be the entirety of the environment variable.

Boolean values can be set to either `true` or `false` using environment variables.

List values are whitespace delimited. Any extra whitespace between entries is
ignored. There is currently no way to include a list element that has internal
whitespace.

Any empty list value can be specified with an empty string, or one containing
only whitespace.

## Configuration Files

Configuration values set in configuration files have the lowest priority: they
are overridden by both command-line options and environment variables.

Configuration files are in [TOML](https://toml.io/en/) format. In configuration
files, configuration values map to keys of the same name. Values types map to
the corresponding TOML types. For example:

```toml
frob-name = "string"
frob-size = 42
enable-frobs = true
enable-qux = false
frobs = ["some", "list", "elements"]
```

Maelstrom programs support the existence of multiple configuration files. In
this case, the program will read each one in preference order, with the
settings from the higher-preference files overriding those from
lower-preference files.

### Configuration File Search

By default, Maelstrom programs will use the [XDG Base Directory
Specification](https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html)
for searching for configuration files.

Specifically, any configuration file found in `XDG_CONFIG_HOME` has the highest
preference, followed by those found in `XDG_CONFIG_DIRS`. If `XDG_CONFIG_HOME` is not
set, or is empty, then `~/.config/` is used. Similarly, if `XDG_CONFIG_DIRS`
is not set, or is empty, then `/etc/xdg/` is used.

Each program has a program-specific suffix that it appends to the directory it
gets from XDG. This has the form `maelstrom/<prog>`, where `<prog>` is
program-specific.

Finally, the program looks for a file named `config.toml` in these directories.

More concretely, these are where Maelstrom programs will look for configuration files:

Program           | Configuration File
------------------|---------------------------------------------------------
cargo-maelstrom   | `<xdg-config-dir>/maelstrom/cargo-maelstrom/config.toml`
maelstrom-go-test | `<xdg-config-dir>/maelstrom/go-test/config.toml`
maelstrom-pytest  | `<xdg-config-dir>/maelstrom/pytest/config.toml`
maelstrom-run     | `<xdg-config-dir>/maelstrom/run/config.toml`
maelstrom-broker  | `<xdg-config-dir>/maelstrom/broker/config.toml`
maelstrom-worker  | `<xdg-config-dir>/maelstrom/worker/config.toml`

For example, if neither `XDG_CONFIG_HOME` nor `XDG_CONFIG_DIRS` is set, then
`cargo-maelstrom` will look for two configuration files:
  - `~/.config/maelstrom/cargo-maelstrom/config.toml`
  - `/etc/xdg/maelstrom/cargo-maelstrom/config.toml`

### Overriding Configuration File Location {#config-file}

Maelstrom programs support the `--config-file` (`-c`) command-line option.
If this option is provided, the specified configuration file, and only that
file, will be used.

If `--config-file` is given `-` as an argument, then no configuration file is used.

Here is a summary of which configuration files will be used for a given value of `--config-file`:

Command Line                                   | Configuration File(s)
-----------------------------------------------|----------------------
`maelstrom-prog --config-file config.toml ...` | only `config.toml`
`maelstrom-prog --config-file - ...`           | none
`maelstrom-prog ...`                           | [search results](#configuration-file-search)

## Optional Values and Defaults

Almost all configuration values have some sort of default value. These default
values come in two types.

The first type is a value that can be specified by the user. For example, an integer
configuration value's default value might be `4`. All boolean and list configuration values
have default values of this form: booleans default to `false` and lists default
to empty lists.

The second type of default value is one that can't be specifically specified by
the user. Once a value of this type is set at one layer, it can't be "unset" at
a higher layer. An example of this is `cargo-maelstrom`'s `target-dir`
configuration value. The default value is "whatever `cargo`'s default is".

Regardless of type, all of the configuration values, along with their defaults,
can be inspected by running the program with the `--help` command-line option.
