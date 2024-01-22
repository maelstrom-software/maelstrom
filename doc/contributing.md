# Releasing

These are the items we need to do to release:
  - Update `CHANGELOG.md`.
  - Update `version` in `[workspace.package]` in `Cargo.toml`.
  - Commit above changes.
  - Tag previous changeset with the version tag.
  - Publish to crates.io.
  - Update `version` in `[workspace.package]` in `Cargo.toml` to next dev version.
  - Push changesets and tags.
  - Create GitHub release.
  - Update GitHub milestones.

## Update `CHANGELOG.md`

To update `CHANGELOG.md`, we need to first add missing items, and then change
the headings to reflect the new release.

### Add Missing Items

The first thing to do when updating the `CHANGELOG.md` is to ensure it is complete.
To do this, look at the `[Unreleased]` link at the bottom of the file in your
browser and fill in the missing items in the `[Unreleaed]` section.
Cross-reference these changes to the closed issues in GitHub. Make sure you add
links to issues when appropriate.

Generally, only add things to `CHANGELOG.md` that you think users would care
about. There need not be an entry for every changeset.

It's a good idea to commit these changes in a separate changeset.

### Change Headings

The second thing to do is to update the headers in `CHANGELOG.md`. This is
mechanical. In the future, we will do this using a tool.

These are the steps to take:
  - Go to the bottom of the file and duplicate the `[unreleased]` line. In the
    bottom copy, change "`unreleased`" to the new version number and change
    "`HEAD`" to the new release's tag.
  - In the preceding line (for `[unreleased]`, change the "from" tag before
    "`...HEAD`" to the new release's tag.
  - Go back to the top of the file, duplicate the `[Unreleased]` header, and
    put an empty line between the two duplicate headers.
  - Change the second duplicate header to contain the version number and the
    date:
```
## [10.3.1] - 2028-06-26
```

## Update `version` in `[workspace.package]` in `Cargo.toml`

This is a bit tricky because you have to update the version itself as well as
all internal dependencies. To do this, run `cargo set-version`.

If you're just removing the `-dev` suffix of the version, you can just run:
```
cargo set-version
```

Otherwise, you can set the version explicitly with:
```
cargo set-version <new version>
```

## Commit Above Changes

This commit should only include the mechanical changes to `CHANGELOG.md`,
`Cargo.toml`, and `Cargo.lock`. If you're using `stg`, make sure you commit all
of these changesets using `stg commit -a`. The message should just be `Version
XXX`.

## Tag

Tag the last committed changeset with the release's tag. This will be something
like "`v1.2.3`".
```bash
git tag v1.2.3
```

## Publish to Crates.io

Packages are published to crates.io individually. Moreover, they have to be
published in the right order, such that all a package's depdendencies are
published before the package itself. We'll automate this at some point, but for
now:

```
for i in crates/{maelstrom-{base,plot,simex,test,worker-child,util,web,worker,broker,container,client,client-cli},cargo-maelstrom}; do (cd $i && cargo publish); done
```

To do this, you must have a secret stored in `~/.cargo/credentials.toml`, and
that secret must allow you to publish these crates. If that is not the case,
ask Neal for the secret and then run `cargo login`.

## Update `version` in `[workspace.package]` in `Cargo.toml` to Next Dev Version

This is a mechanical change. In the future, we will do this using a tool.

Use `cargo set-version` again:
```
cargo set-version <NEXT VERSION>-dev
```

## Commit Above Changes

It's important to commit these new changes right away, so that nothing other that the
actual version has the given version string.

## Push Changesets and Tags

Ensure that you push, and that you push the tags along with the changesets. We
can do that atomically like this:
```bash
git push --atomic origin main v1.2.3
```

## Create GitHub Release

Go to the [GitHub releases page](https://github.com/maelstrom-software/maelstrom/releases). Click `Draft a
new release`. Use the tag you just crated to draft the release.

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
