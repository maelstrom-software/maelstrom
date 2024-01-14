# Test Pattern DSL

This domain-specific language has been designed specifically for
`cargo-maelstrom` to let users easily describe a set of tests to run.

If you are a fan of formal explanations check out the
[BNF](./test_pattern_dsl/bnf.md). Otherwise, this page will attempt to give a
more informal explanation of how the language works.

## Selectors

Conceptually every test has a variety of properties about it. Selecting tests
via a pattern is the process of describing something of these properties.

We call the usage of these properties in expressions "selectors". Here is a
listing of them.

- **name**: Test function name with its module path prepended (e.g. mod1::test1)
- **package**: Package name the test is a part of
- **binary / test / benchmark / example**: Which binary is the test
    a part of. See [Selecting Executables](#selecting-executables)

The simplest usage of the language is to craft an expression which expresses
something about one of these properties.

## Matchers
In order to express something about one of the test properties we need to use a
matcher. Here is a listing of those

- **equals**: true only if the property matches exactly
- **contains**: true only if the property contains the given sub-string
- **starts_with**: true only if the property starts with the given sub-string
- **ends_with**: true only if the property ends with the given sub-string
- **matches**: true only if the property matches the given regular expression
- **globs**: true only if the property matches the given glob expression

## Crafting a Simple Expression
Using a property and a matcher we can craft a simple expression. For example, if
we want to only select tests which have the exact name "my_mod::my_test" we do
something like.

```
name.equals(my_mod::my_test)
```

Or if we want to select all the tests in the package called foobar, we would do

```
package.equals(foobar)
```

## Compound Expressions
To create more interesting expression, simple expressions can be combined using
the following operators

- **`!`, `not`**: logical not
- **`&&`, `and`**: logical and
- **`||`, `or`**: logical or
- **`\`, `-`, `minus`**: same as && !(...)

Here are some examples

- `!name.equals(foobar)` only tests not named foobar
- `package.equals(foo) && name.equals(bar)` test(s) named bar in package foo
- `package.equals(foo) - name.equals(bar)` tests in package foo except ones
  named bar

When combining multiple operators, parenthesis are allowed to control the
precedence.

To select tests named foo or bar in package baz, you can do
```
(name.equals(foo) || name.equals(bar)) && package.equals(baz)
```

To select tests named bar in package baz or tests named foo from any package
```
name.equals(foo) || (name.equals(bar) && package.equals(baz))
```

## Selecting Executables
A package can contain a few different types of executables. When selecting tests
from an executable the type of executable can be selected using the following
selectors.

- **benchmark**: a benchmark
- **binary**: a binary
- **example**: an example
- **library**: a library
- **test**: an integration test

For more details about these different types of targets, check out the [cargo
documentation](https://doc.rust-lang.org/cargo/reference/cargo-targets.html).

These selectors make complete expression in and of themselves, but also they can
be used to match the name of the executable (with the exception of `library`)

- `benchmark.equals(my_benchmark)` test the benchmark called my_benchmark
- `name.equals(cli_test) && binary` test tests called cli_test and inside binaries
- `binary.equals(cli_a) || binary.equals(cli_b)` tests from binaries cli_a, cli_b
- `library` test only the tests found in libraries

Note that you can't do something like `library.equals(foo)`, this is not allowed
in favor of doing `package.equals(foo) && library`. This is because a package is
only allowed one library.

## Special Expressions
These other expressions are useful occasionally

- **`true`, `all`, `any`**: selects all tests
- **`false`, `none`**: selects no tests

When you provide no filter to `cargo-maelstrom` it acts as if you typed `cargo
maelstrom run -i all`

## Abbreviations

The matchers and selectors can be shortened as long as it doesn't create some
ambiguity

For example, the following are all the same
```
name.equals(foo)
name.eq(foo)
n.eq(foo)
```

We can abbreviate `name` to `n` since no other selector starts with "n", but we
can't abbreviate `equals` to `e` because there is another selector `ends_with`
which also starts with an "e".

## Using Regex and Glob Matchers

The regular expression and glob matchers work how you would expect. They allow
you to pass a regular expression or glob pattern and it uses that to evaluate if
the expression matches some test.

Before showing some examples, first lets talk about the parenthesis we've used
when writing expressions. Matchers actually support other punctuation also

- **`(` and `)`**
- **`[` and `]`**
- **`{` and `}`**
- **`<` and `>`**
- **`/` and `/`**

This feature is useful for matchers which accept strings that have potentially
the same punctuation as part of it

Here are some examples of regular expressions and glob patterns

- `name.matches/foo::.*/` tests starting with foo::
- `name.globs(bar::*)` tests starting with bar::
