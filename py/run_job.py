#!/usr/bin/env python

import os
import sys

from maelstrom_client import Client, ImageSpec, ContainerSpec, ContainerRef, JobSpec

def main():
    prog=sys.argv[1]
    arguments=sys.argv[2:]

    client = Client(slots=4)

    image = ImageSpec(name="docker://alpine", use_layers=True)
    container = ContainerSpec(working_directory="/", image=image)
    spec = JobSpec(
        container=ContainerRef(inline=container),
        program=prog,
        arguments=arguments,
    )
    stream = client.run_job(spec)
    for status in stream:
        result = status.completed

    if result.result.HasField("outcome"):
        stdout = result.result.outcome.completed.effects.stdout.inline.decode()
        sys.stdout.write(stdout)
    else:
        print("error:", str(result.result.error).strip())

if __name__ == "__main__":
    main()

