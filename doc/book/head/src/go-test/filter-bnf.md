# Test Pattern DSL BNF

Included on this page is the [Backus-Naur
form](https://en.wikipedia.org/wiki/Backus%E2%80%93Naur_form) notation for the
DSL

```BNF
pattern                := or-expression
or-expression          := and-expression
                       |  or-expression or-operator and-expression
or-operator            := "|" | "||" | "or"
and-expression         := not-expression
                       |  and-expression and-operator not-expression
                       |  and-expression diff-operator not-expression
and-operator           := "&" | "&&" | "and" | "+"
diff-operator          := "\" | "-" | "minus"
not-expression         := simple-expression
                       |  not-operator not-expression
not-operator           := "!" | "~" | "not"
simple-expression      := "(" or-expression ")"
                       |  simple-selector
                       |  compound-selector
simple-selector        := simple-selector-name
                       |  simple-selector-name "(" ")"
simple-selector-name   := "all" | "any" | "true"
                       |  "none" | "false"
compound-selector      := compound-selector-name "." matcher-name matcher-parameter
compound-selector-name := "name" | "package"
matcher-name           := "equals" | "contains" | "starts_with" | "ends_with" |
                          "matches" | "globs"
matcher-parameter      := <punctuation mark followed by characters followed by
                           matching punctuation mark>
```
