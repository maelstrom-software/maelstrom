# `maelstrom-pytest`

## Choosing a Python Image
Before we start running tests, we need to choose a python image.

First generate a `maelstrom-pytest.toml` file
```bash
maelstrom-pytest --init
```

Then update the image in the file to have the version of Python you desire.
```toml
[[directives]]
image = "docker://python:3.11-slim"
```
The default configuration and our example uses an
[image from Docker](https://hub.docker.com/_/python)

## Including Your Project Python Files
So that your tests can be run from the container, your project's python must be included.
Update the `added_layers` in the file to make sure it includes your project's Python.
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

## Running Tests
Once you have finished the configuration, you only need invoke `maelstrom-pytest` to run all the
tests in your project. It must be run from an environment where `pytest` is in the Python path. If
you are using virtualenv for your project make sure to source that first.
