# Test Filter Patterns

There are times when a user needs to concisely specify a set of tests to
`maelstrom-go-test`. One of those is on the command line: `maelstrom-go-test`
can be told to only run a certain set of tests, or to exclude some tests.
Another is the [`filter`](spec/fields.md#filter) field of `maelstrom-go-test.toml`
directives. This is used to choose which tests a directive applies too.

In order to allow users to easily specify a set of tests to `maelstrom-go-test`,
we created the domain-specific pattern language described here.

If you are a fan of formal explanations check out the [BNF](filter-bnf.md).
Otherwise, this page will attempt to give a more informal explanation of the
language.

## Simple Selectors

The most basic patterns are "simple selectors". These are only sometimes useful
on their own, but they become more powerful when combined with other patterns.
Simple selectors consist solely of one of the these identifiers:

Simple Selector      | What it Matches
---------------------|------------------------------
`true`, `any`, `all` | any test
`false`, `none`      | no test

Simple selectors can optionally be followed by `()`. That is, `all()` and
`all` are equivalent patterns.

## Compound Selectors

"Compound selector patterns" are patterns like `package.equals(foo)`. They
combine "compound selectors" with "matchers" and "arguments". In our example,
`package` is the compound selector, `equals` is the matcher, and `foo` is the
argument.

These are the possible compound selectors:

Compound Selector     | Selected Name
----------------------|-------------------------------------------------
`name`                | the name of the test
`package_import_path` | the import-path of the test's package
`package_path`        | trailing part of the import-path after the module's name
`package_name`        | name the package uses in its `package` declaration

See [below](#specifying-packages) for more details on the various ways to
specify a package.

These are the possible matchers:

Matcher       | Matches If Selected Name...
--------------|---------------------------------------------------------------
`equals`      | exactly equals argument
`contains`    | contains argument
`starts_with` | starts with argument
`ends_with`   | ends with argument
`matches`     | matches argument evaluated as regular expression
`globs`       | matches argument evaluated as glob pattern

Compound selectors and matchers are separated by `.` characters. Arguments are
contained within delimiters, which must be a matched pair:

Left | Right
:---:|:----:
`(`  | `)`
`[`  | `]`
`{`  | `}`
`<`  | `>`
`/`  | `/`

Let's put this all together with some examples:

Pattern                            | What it Matches
-----------------------------------|----------------
`test.equals(foo_test)`            | Any test named `"foo_test"`.
`package.matches{(foo)*bar}`       | Any test whose package import-path matches the regular expression `(foo)*bar`.

## Compound Expressions

Selectors can be joined together with operators to create compound expressions.
These operators are:

Operators          | Action
-------------------|------------
`!`, `~`, `not`    | Logical Not
`&`, `&&`, `and`   | Logical And
`\|`, `\|\|`, `or` | Logical Or
`\`, `-`, `minus`  | Logical Difference
`(`, `)`           | Grouping

The "logical difference" action is defined as follows: `A - B == A && !B`.

As an example,
to select tests named `foo` or `bar` in package `baz`:
```maelstrom-test-pattern
(name.equals(foo) || name.equals(bar)) && package.equals(baz)
```

As another example, to select tests named `bar` in package `baz` or tests named
`foo` from any package:
```maelstrom-test-pattern
name.equals(foo) || (name.equals(bar) && package.equals(baz))
```

## Abbreviations

Selector and matcher names can be shortened to any unambiguous prefix.

For example, the following are all the same
```maelstrom-test-pattern
name.equals(foo)
name.eq(foo)
n.eq(foo)
```

We can abbreviate `name` to `n` since no other selector starts with "n", but we
can't abbreviate `equals` to `e` because there is another selector, `ends_with`,
that also starts with an "e".

The `package_import_path` matcher name has a special case. Any prefix of `package` will resolve
to `package_import_path` instead of `package_path` or `package_name`.

For example, all of the following resolve to `package_import_path`
```maelstrom-test-pattern
package_import_path.equals(foo)
package_i.equals(foo)
package.equals(foo)
p.equals(foo)
```

## Specifying Packages

What exactly counts as a "package name" in Go can be a bit confusing. We
therefore support three different ways to specify a package. To illustrate the
various ways, imagine that we have a module called
`github.org/maelstrom-software/maelstrom`. Inside of that module there is a
subdirectory called `client`, and inside of that, there is another subdirectory
called `rpc`. To confuse things, all the `.go` files in `rpc` start with
`package client_rpc`. In this case, the tests in this directory would all have
the following values:

Selector              | Value
----------------------|------
`package_import_path` | `github.org/maelstrom-software/client/rpc`
`package_path`        | `client/rpc`
`package_name`        | `client_rpc`

The `package_path` for the root of a module will be the empty string.
