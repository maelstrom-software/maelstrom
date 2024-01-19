# Releasing

These are the items we need to do to release:
  - Update `CHANGELOG.md`.
  - Update `version` in `[workspace.package]` in `Cargo.toml`.
  - Commit above changes.
  - Tag previous changeset with the version tag.
  - Push changesets and tags.

## Update `CHANGELOG.md`

To update `CHANGELOG.md`, we need to first add missing items, and then change
the headings to reflect the new release.

### Add Missing Items

The first thing to do when updating the `CHANGELOG.md` is to ensure it is complete.
To do this, look at the `[Unreleased]` link at the bottom of the file in your
browser and fill in the missing items in the `[Unreleaed]` section. Make sure
you add links to issues when appropriate.

Generally, only add things to `CHANGELOG.md` that you think users would care
about. There shouldn't be an entry for every changeset.

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

## Update `version` in `[workspace.package]` in `Cargo.toml`.

This is a mechanical change. In the future, we will do this using a tool.

Edit `Cargo.toml`, find `version` in `[workspace.package]`, and change it to
the new version.

## Commit Above Changes

This commit should only include the mechanical changes to `CHANGELOG.md` and
`Cargo.toml`. If you're using `stg`, make sure you commit all of these
changesets using `stg commit -a`. The message should just be `Version XXX`.

## Tag

Tag the last committed changeset with the release's tag. This will be something
like "`v1.2.3`".
```bash
git tag v1.2.3
```

## Push Changesets and Tags

Ensure that you push, and that you push the tags along with the changesets. We
can do that atomically like this:
```bash
git push --atomic origin main v1.2.3
```
