# Jobs

Maelstrom deals in jobs. Each job has a specification that tells Maelstrom how
to run the job. This specification, or "job-spec", consists of:
  - The stack of file system layers,
  - An indication of whether the file system will be writable or read-only,
  - A list of mounts of special file systems (e.g. `sysfs`, `proc`, `tmpfs`, etc.),
  - A list of special devices to put in `/dev` (e.g. `/dev/null`, `/dev/zero`, etc.),
  - A description of the network environment,
  - A path to the program to execute,
  - A list of arguments and environment variables to pass to the program,
  - A user and group IDs to run the program as,

## Layers

The root file system that a job sees is a collapsed union mount of all of the
various layers in the layers stack. In Maelstrom, individual layers can be
specified in a number of ways.
