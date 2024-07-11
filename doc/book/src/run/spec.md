# Job Specification Format

Jobs in `maelstrom-run` are specified in JSON, either via standard input or
[from a file](cli.md#--file).

The format is a stream of individual JSON objects. The jobs aren't separated by
any specific character, and they aren't part of a larger object or list.

We chose this format over TOML or other JSON representations because we wanted
something that would allow us to start executing individual jobs as soon as
they were read, without having to read all the job specification first.

Here is a simple example:

```json
{
        "image": {
               "name": "docker://alpine",
               "use": ["layers", "environment"]
        },
        "program": "echo",
        "arguments": ["Hello", "world!"]
}
{
        "image": {
               "name": "docker://alpine",
               "use": ["layers", "environment"]
        },
        "program": "echo",
        "arguments": ["¡Hola", "mundo!"]
}
```

This will print out `Hello world!` and `¡Hola mundo!` on two separate lines. If
you run `cargo-run` without `--file`, and type in the first job specification,
you will see that the job is run in the background and `Hello world!` is
outputted, even before you specify the end of input. You can then input the
second job specification and have it run.

The fields in the job specification JSON object are documented in the [next chapter](spec-fields.md).
