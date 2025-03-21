# `--stop-after`

The [`--stop-after` configuration value](config.md#stop-after) tells Maelstrom
to stop executing tests after there have been the provided number of test
failures.

As mentioned [before](repeat.md), this can pair well with `--repeat`.

It can also be useful when you've made a lot of breaking changes and you need
to fix all of the failing tests.

Finally, it gives you more confidence to regularly run a larger set of tests,
especially because of Maelstrom's execution order, which is [discussed
next](test-execution-order.md).
