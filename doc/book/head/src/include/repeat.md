# `--repeat`

The [`--repeat` configuration value](config.md#repeat) tells Maelstrom to
repeat every test the specified number of times. When combined with a large
cluster, this can be very powerful.

Sometimes a bug is very hard to reproduce. In this case, you could run the test
that sometimes reproduces the bug with `--repeat=1000`, or more. In this
situation, it's useful to add [`--stop-after`](stop-after.md).

Another time `--repeat` can be useful is when you have a test of some
nondeterministic code and you want to really stress test it. Running the tests
with a large `--repeat` can really help to instill confidence.
