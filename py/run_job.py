#!/usr/bin/env nix-shell
#! nix-shell -i python3 -p python3

import os
import sys

from maelstrom_client import Client, ImageSpec

def main():
    prog=sys.argv[1]
    arguments=sys.argv[2:]

    client = Client()

    image = ImageSpec(name="alpine", tag="latest", use_layers=True)
    job = client.run_job(prog, arguments, image=image)
    result = job.result()

    if result.result.HasField("outcome"):
        stdout = result.result.outcome.completed.effects.stdout.inline.decode()
        sys.stdout.write(stdout)
    else:
        print("error:", str(result.result.error).strip())

if __name__ == "__main__":
    main()

