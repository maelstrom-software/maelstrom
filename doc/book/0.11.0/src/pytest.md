# `maelstrom-pytest`

## Choosing a Python Image
Before we start running tests, we need to choose a python image.

First generate a `maelstrom-pytest.toml` file
```bash
maelstrom-pytest --init
```

Then update the file to include a python image
```toml
[[directives]]
image = "docker://python:3.11-slim"
```
This example installs an [image from Docker](https://hub.docker.com/_/python)

## Including Your Project Python Files
So that your tests can be run from the container, your project's python must be included.
```toml
added_layers = [ { glob = "**.py" } ]
```
This example just adds all files with a `.py` extension. You may also need to include `.pyi` files
or other files.

## Including `pip` Packages
If you have an image named "python", maelstrom-pytest will automatically include pip packages for
you as part of the container. It expects to read these packages from a `test-requirements.txt` file
in your project directory. This needs to at a minimum include the `pytest` package

`test-requirements.txt`
```
pytest==8.1.1
```
