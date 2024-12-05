#!/usr/bin/env python

import os
import sys

from maelstrom_client import Client, ImageRef, ContainerSpec, ContainerRef, JobSpec, ImageUse, ContainerParent

def main():
    prog=sys.argv[1]
    arguments=sys.argv[2:]

    client = Client(slots=4)

    image = ImageRef(name="docker://alpine", use=[ImageUse.IMAGE_USE_LAYERS])
    container = ContainerSpec(working_directory="/", parent=ContainerParent(image=image))
    spec = JobSpec(
        container=container,
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

