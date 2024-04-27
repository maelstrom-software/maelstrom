# Log Level
The broker and the worker output log messages. Each message is tagged with log
level. The log level is an ordered listing of named tags. Each subsequent tag has
a decreasing level of severity. The following is an ordered listing of the log
levels (ordered from high to low severity)

- `"error"`: these messages indicate some severe problem
- `"warning"`: these messages indicate a not so severe problem
- `"info"`: these messages are purely informational
- `"debug"`: these messages are mostly for developers

A program will only display messages tagged with equal or higher severity level
to the currently set log level.
