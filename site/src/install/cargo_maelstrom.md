# Installing cargo-maelstrom

We're going to install cargo-maelstrom using cargo. It only works on Linux.

First make sure you've installed [Rust](https://www.rust-lang.org/tools/install).

Then install it by doing

```bash
export GITHUB_URL="https://github.com/maelstrom-software/maelstrom.git"
cargo install --git $GITHUB_URL cargo-maelstrom
```

You should now be able to invoke it by running `cargo maelstrom run`
