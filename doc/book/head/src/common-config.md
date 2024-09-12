# Common Configuration Values

Even Maelstrom program supports the `log-level` configuration value.

## Log Level

The `log-level` configuration value specifies which log messages should be
output. The program will output log messages of the given severity or higher.
This string configuration value must be one of the following:

Level         | Meaning
--------------|---------------------------------------------------
`"error"`     | indicates an unexpected and severe problem
`"warning"`   | indicates an unexpected problem that may degrade functionality
`"info"`      | is purely informational
`"debug"`     | is mostly for developers

The default value is `"info"`.

Most programs output log messages to standard output or standard error, though
the `maelstrom-client` background process will log them to a file in the state
directory.
