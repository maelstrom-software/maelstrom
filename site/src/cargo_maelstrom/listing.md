# Listing
`cargo-maelstrom` includes some options starting with `--list`. These can be
used to get a listing of various things.

- `--list` or `--list-tests`: all tests across all packages
    - has the format `<package-name> <module>::<test>`
- `--list-binaries`: all the test binaries across all packages
    - has the format `binary <package-name> <binary-name>? (<binary-kind>)`
- `--list-packages`: all the packages in your workspace
    - has the format `package <package-name>`

## Filtering
Filters can be combined with these list options. This can be useful to see what
tests will run before actually running them.

For more information on filtering, see [Filtering Tests](./filtering_tests.md)
