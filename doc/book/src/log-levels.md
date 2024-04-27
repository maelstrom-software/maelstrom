# Log Levels

Every Maelstrom program supports the `log-level` configuration value. The
program will output log messages of the given severity or higher. This
string configuration value must be one of the following:

Level         | Meaning
--------------|---------------------------------------------------
`"error"`     | indicates an unexpected and severe problem
`"warning"`   | indicates an unexpected problem that may degrade functionality
`"info"`      | is purely informational
`"debug"`     | is mostly for developers

The default value is `"info"`.
