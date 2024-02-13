# Releasing

## Set Environment Variables

Set two environment variables so we can cut and paste below:
```bash
VERSION=1.2.0
NEXT_VERSION=1.3.0
```

Obviously, you should choose the correct version strings.

## Update `CHANGELOG.md`

We need to update `CHANGELOG.md`, first to add missing items, and then to
change the headings and links to reflect the new release.

### Add Missing Items

The first thing to do is to ensure that `CHANGELOG.md` is accurate and
complete. To do this, look at the `[Unreleased]` link at the bottom of the file
in your editor and fill in the missing items. If there is no `[Unreleased]`
section yet, create one first. Cross-reference these changes to the closed
issues in GitHub for the release. Make sure you add links to issues when
appropriate.

Generally, only add things to `CHANGELOG.md` that you think users would care
about. There need not be an entry for every changeset or even issue.

You should commit these changes in their own separate changeset.

```bash
$EDITOR CHANGELOG.md
git commit -am "Update CHANGELOG.md for version $VERSION."
```

### Change Headings and Links

The second thing to do is to update the headers in `CHANGELOG.md`. This is
mechanical. In the future, we will do this using a tool.

These are the steps to take:
  - At the top of the file, change the `[Unreleased]` header to contain the
    version number and the date. Something like this:
```
## [10.3.1] - 2028-06-26
```
  - Go to the bottom of the file and change the `[unreleased]` line. Change
    "`unreleased`" to the new version number and change "`HEAD`" to the new
    release's tag.

## Update `version` in `[workspace.package]` in `Cargo.toml`

This is a bit tricky because you have to update the version itself as well as
all internal dependencies, so we use the `cargo-set-version` tool to make: sure
we get it right:
```bash
cargo set-version "$VERSION"
```

## Commit Above Changes

This commit should only include the mechanical changes to `CHANGELOG.md`,
`Cargo.toml`, and `Cargo.lock`.
```bash
git status
git commit -am "Version $VERSION."
```

## Tag Previous Changeset with the Version Tag

Tag the last committed changeset with the release's tag.
```bash
git tag "v$VERSION"
```

## Push Changesets and Tags

Ensure that you push, and that you push the tags along with the changesets. We
can do that atomically like this:
```bash
git push --atomic origin main "v$VERSION"
```

## Publish to Crates.io

Publishing to crates.io correctly is a bit of a pain, so we wrote an xtask action to automate it.

First, make sure you're logged in to cargo:
```bash
cat ~/.cargo/credentials
```

You should see something like this:
```
[registry]
token = "abcdefg1234567ABCDEFG123456abcdefg1"
```

If you don't have a token, then get a token from Neal and run:
```bash
cargo login
```

After you're sure you're logged in, run:
```bash
cargo xtask publish
```

This will publish all of the packages.

## Create GitHub Release

There is a cli for GitHub that we use called `gh`. First, make sure you're logged in:
```bash
gh auth status
```

Then, create the GitHub release:
```bash
gh release create "v$VERSION" -F <(cargo xtask extract-release-notes "$VERSION")
```

## Update `CHANGELOG.md` for Unreleased Changes

Add the `[Unreleased]` section into `CHANGELOG.md`. This should look like:
```
## [Unreleased]
### General
#### Changed
#### Added
#### Removed
#### Fixed
### `cargo-maelstrom`
#### Changed
#### Added
#### Removed
#### Fixed
```

At the bottom of the file, add a link like this:
```
[unreleased]: https://github.com/maelstrom-software/maelstrom/compare/v<VERSION>...HEAD
```

## Update `version` in `[workspace.package]` in `Cargo.toml` to Next Dev Version

Use `cargo set-version` again:
```bash
cargo set-version "$NEXT_VERSION-dev"
```

## Commit Above Changes

It's important to commit these new changes right away, so that nothing other that the
actual version has the given version string.

```bash
git commit -am "Update CHANGELOG.md for version $NEXT_VERSION."
```

## Push Changesets and Tags

Ensure that you push again:
```bash
git push
```

## Unset Environment Variables
```bash
unset VERSION NEXT_VERSION
```

## Update GitHub Milestones

Go to the [GitHub milestones page](https://github.com/maelstrom-software/maelstrom/milestones).

If there isn't yet a milestone for the next release, create one.

Then, look at all of the issues in the milestone for the just-released version.
If there are any open issues, either close them or move them out of the
release, as appropriate.

Finally, close the milestone for the just-released version.

## Announce the Release on Discord

Go to the [`#general` channel on the community discord
server](https://discord.gg/nyaGuzJr). Announce the release.
