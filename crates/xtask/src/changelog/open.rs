use anyhow::{bail, Result};
use camino::Utf8PathBuf;
use clap::Parser;
use regex::Regex;
use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
    iter, result,
};

fn filter(input: impl BufRead, mut output: impl Write) -> Result<()> {
    let lines = input.lines().collect::<result::Result<Vec<String>, _>>()?;

    // Find the first version header line. Extract its index and version string.
    let re = Regex::new(r"^## \[([^]]+)\]").unwrap();
    let Some((header_idx, [header_version])) = lines
        .iter()
        .enumerate()
        .find_map(|(i, line)| re.captures(line).map(|captures| (i, captures.extract().1)))
    else {
        bail!("couldn't find any version headers");
    };

    // We're mostly trying to avoid "Unreleased".
    if !header_version.starts_with(|c: char| c.is_ascii_digit()) {
        bail!(r#"first version header is for invalid version "{header_version}""#);
    }

    // Find the first reference-style link with the version comparison URL. Save the index, the
    // relevant versions, and the important bits of the URL.
    let re = Regex::new(r"^\[([^]]+)\]: (https://.+/compare)/v.+\.\.\.v(.+)$").unwrap();
    let Some((link_idx, [first_link_ref_version, compare_url_base, first_link_compare_version])) =
        lines
            .iter()
            .enumerate()
            .find_map(|(i, line)| re.captures(line).map(|captures| (i, captures.extract().1)))
    else {
        bail!("couldn't find reference-style links for version comparisons");
    };

    // Sanity check that the first version header matches the first reference-style link.
    if header_version != first_link_ref_version {
        bail!(
            "versions don't match for first header ({header_version}) \
                and first reference-style link ({first_link_ref_version})"
        );
    }
    if first_link_ref_version != first_link_compare_version {
        bail!(
            "versions don't match for first reference-style link's ref ({first_link_ref_version}) \
                and its comparison version in the URL ({first_link_compare_version})"
        );
    }

    // We could handle the case where the links were on top, but why bother?
    if link_idx < header_idx {
        bail!(
            "reference-style link appears before first version header \
                (lines {} and {}, respectively)",
            link_idx + 1,
            header_idx + 1,
        );
    }

    let headers_to_insert = ["## [Unreleased]", ""];
    let link_to_insert = format!(
        "[unreleased]: {}/v{}...HEAD",
        compare_url_base, first_link_compare_version
    );

    // Generate the changelog as an iterator.
    let out_lines = (lines[0..header_idx].iter().map(String::as_str))
        .chain(headers_to_insert)
        .chain(lines[header_idx..link_idx].iter().map(String::as_str))
        .chain(iter::once(link_to_insert.as_str()))
        .chain(lines[link_idx..].iter().map(String::as_str));

    // Write it out.
    for line in out_lines {
        writeln!(output, "{line}")?;
    }

    Ok(())
}

/// Get changelog ready for changes after a release.
#[derive(Debug, Parser)]
pub struct CliArgs;

pub fn main(changelog: Utf8PathBuf, _args: CliArgs) -> Result<()> {
    let mut tempfile = tempfile::Builder::new().tempfile_in(changelog.parent().unwrap())?;
    filter(BufReader::new(File::open(&changelog)?), &mut tempfile)?;
    tempfile.persist(changelog)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;
    use std::{io::Cursor, str};

    fn run_success_test(input: &str, expected_output: &str) {
        let mut output = vec![];
        filter(Cursor::new(input), Cursor::new(&mut output)).unwrap();
        let output = str::from_utf8(&output).unwrap();
        assert_eq!(
            output,
            expected_output,
            "output differed from expected: {}",
            colored_diff::PrettyDifference {
                expected: expected_output,
                actual: output,
            }
        );
    }

    fn run_failure_test(expected_error: &str, input: &str) {
        let mut output = vec![];
        assert_eq!(
            filter(Cursor::new(input), Cursor::new(&mut output))
                .unwrap_err()
                .to_string(),
            expected_error
        );
    }

    #[test]
    fn test_success() {
        run_success_test(
            indoc! {r###"
                # Changelog

                Stuff

                ## [0.2.0] - 2024-01-2

                ### General

                  - Cool stuff.
                  - More cool stuff.

                ## [0.1.0] - 2024-01-1

                [0.2.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.1.0...v0.2.0
                [0.1.0]: https://github.com/maelstrom-software/maelstrom/releases/tag/v0.1.0
            "###},
            indoc! {r###"
                # Changelog

                Stuff

                ## [Unreleased]

                ## [0.2.0] - 2024-01-2

                ### General

                  - Cool stuff.
                  - More cool stuff.

                ## [0.1.0] - 2024-01-1

                [unreleased]: https://github.com/maelstrom-software/maelstrom/compare/v0.2.0...HEAD
                [0.2.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.1.0...v0.2.0
                [0.1.0]: https://github.com/maelstrom-software/maelstrom/releases/tag/v0.1.0
            "###},
        );
    }

    #[test]
    fn test_failure_no_version_headers() {
        run_failure_test(
            "couldn't find any version headers",
            indoc! {r###"
                # Changelog
                ## Something unexpected
                ## Something else unexpected
                [0.2.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.1.0...v0.2.0
                [0.1.0]: https://github.com/maelstrom-software/maelstrom/releases/tag/v0.1.0
            "###},
        );
    }

    #[test]
    fn test_failure_first_version_header_is_unreleased() {
        run_failure_test(
            r#"first version header is for invalid version "Unreleased""#,
            indoc! {r###"
                # Changelog
                ## [Unreleased]
                ## [0.2.0] - 2024-01-2
                ## [0.1.0] - 2024-01-1
                [0.2.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.1.0...v0.2.0
                [0.1.0]: https://github.com/maelstrom-software/maelstrom/releases/tag/v0.1.0
            "###},
        );
    }

    #[test]
    fn test_failure_no_links() {
        run_failure_test(
            "couldn't find reference-style links for version comparisons",
            indoc! {r###"
                # Changelog
                ## [0.2.0] - 2024-01-2
                ## [0.1.0] - 2024-01-1
            "###},
        );
    }

    #[test]
    fn test_failure_links_and_headers_disagree() {
        run_failure_test(
            "versions don't match for first header (0.3.0) and first reference-style link (0.2.0)",
            indoc! {r###"
                # Changelog
                ## [0.3.0] - 2024-01-3
                ## [0.2.0] - 2024-01-2
                ## [0.1.0] - 2024-01-1
                [0.2.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.1.0...v0.2.0
                [0.1.0]: https://github.com/maelstrom-software/maelstrom/releases/tag/v0.1.0
            "###},
        );
    }

    #[test]
    fn test_failure_link_versions_disagree() {
        run_failure_test(
            "versions don't match for first reference-style link's ref (0.2.0) \
            and its comparison version in the URL (0.3.0)",
            indoc! {r###"
                # Changelog
                ## [0.2.0] - 2024-01-2
                ## [0.1.0] - 2024-01-1
                [0.2.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.1.0...v0.3.0
                [0.1.0]: https://github.com/maelstrom-software/maelstrom/releases/tag/v0.1.0
            "###},
        );
    }

    #[test]
    fn test_failure_links_before_headers() {
        run_failure_test(
            "reference-style link appears before first version header (lines 1 and 4, respectively)",
            indoc! {r###"
                [0.2.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.1.0...v0.2.0
                [0.1.0]: https://github.com/maelstrom-software/maelstrom/releases/tag/v0.1.0
                # Changelog
                ## [0.2.0] - 2024-01-2
                ## [0.1.0] - 2024-01-1
            "###});
    }
}
