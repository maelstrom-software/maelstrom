# Installing cargo-metest

We're going to install cargo-metest using cargo. It only works on Linux.

First make sure you've installed [Rust](https://www.rust-lang.org/tools/install).

Then install it by doing

```bash
export METICULOUS_GITHUB="https://github.com/meticulous-software/meticulous.git"
cargo install --git $METICULOUS_GITHUB cargo-metest
```

You should now be able to invoke it by running `cargo metest`
