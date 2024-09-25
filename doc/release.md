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

The second thing to do is to update the headers and links in `CHANGELOG.md`.
This is done with `cargo xtask changelog close`:
```bash
cargo xtask changelog close "$VERSION"
```

## Update `version` in `[workspace.package]` in `Cargo.toml`

This is a bit tricky because you have to update the version itself as well as
all internal dependencies, so we use the `cargo-set-version` tool to make sure
we get it right:
```bash
cargo set-version "$VERSION"
```

## Update Book Directories

We move the `head` book into the new version, then update the `latest` symlink:
```bash
cargo xtask book close "$VERSION"
```

## Commit Above Changes

This commit should only include the mechanical changes to `CHANGELOG.md`,
`Cargo.toml`, `Cargo.lock`, and the changes in `doc/book`.
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
cat ~/.cargo/credentials.toml
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
gh release create "v$VERSION" -F <(cargo xtask changelog extract-release-notes "$VERSION")
```

## Build and Upload Build Artifacts

Build the code with the release profile
```bash
cargo build --release
```

The upload the artifacts to GitHub:
```bash
cargo xtask distribute publish "v$VERSION"
```

Repeat this step on all supported architectures. Make sure you are at the release revision.

## Update `CHANGELOG.md` for Unreleased Changes

Use `cargo xtask changelog open` to Add the `[Unreleased]` section to `CHANGELOG.md`:
```bash
cargo xtask changelog open
```

## Update `version` in `[workspace.package]` in `Cargo.toml` to Next Dev Version

Use `cargo set-version` again:
```bash
cargo set-version "$NEXT_VERSION-dev"
```

## Update Book Directories

We copy all of the files from the `latest` directory to `head`:
```bash
cargo xtask book open
```

## Commit Above Changes

It's important to commit these new changes right away, so that nothing other that the
actual version has the given version string.

```bash
git commit -am "Start version $NEXT_VERSION."
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

## Announce the Release

Go to the [`#general` channel on the community discord
server](https://discord.gg/8xN4QvjjmF). Announce the release.

Go to the [discussions page](https://github.com/maelstrom-software/maelstrom/discussions).
Announce the release. Pin the new release announcement and unpin the old
release announcement.

Make a "This Week in Rust" pull request. Go to [our
fork](https://github.com/maelstrom-software/this-week-in-rust), sync it, then create a PR.

## Make Build Artifacts for ARM

Turn on the AWS ARM builder instance. Ssh in using the `Maelstrom\ Dev.pem` key.

Set the `VERSION` environment variable:

```bash
VERSION=1.2.0
```

Check out the current version:
```bash
git checkout "v$VERSION"
```

Build:
```bash
cargo build --release
```

Validate `gh` login status:
```bash
gh auth status
```

Now push the just-built artifact:
```bash
cargo xtask distribute publish "v$VERSION"
```

Then stop the instance.
