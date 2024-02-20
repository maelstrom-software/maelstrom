use anyhow::{bail, Result};
use camino::Utf8PathBuf;
use chrono::Local;
use clap::Parser;
use regex::Regex;
use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
    result,
};

/// Close changelog to prepare for release.
#[derive(Debug, Parser)]
pub struct CliArgs {
    /// Version to release.
    version: String,
}

fn filter(version: &str, today: &str, input: impl BufRead, mut output: impl Write) -> Result<()> {
    let mut lines = input.lines().collect::<result::Result<Vec<String>, _>>()?;

    let Some(line) = lines
        .iter_mut()
        .find(|line| line.starts_with("## [Unreleased]"))
    else {
        bail!(r#"couldn't find "Unreleased" version header"#);
    };
    *line = format!("## [{version}] - {today}");

    let re = Regex::new(r"^\[unreleased\](:.*\.\.\.)HEAD$").unwrap();
    let Some((idx, [body])) = lines.iter().enumerate().find_map(|(idx, line)| {
        re.captures(line)
            .map(|captures| (idx, captures.extract().1))
    }) else {
        bail!(r#"couldn't find reference-style link for "unreleased""#);
    };
    lines[idx] = format!("[{}]{}v{}", version, body, version);

    for line in lines {
        writeln!(output, "{line}")?;
    }

    Ok(())
}

pub fn main(changelog: Utf8PathBuf, args: CliArgs) -> Result<()> {
    let mut tempfile = tempfile::Builder::new().tempfile_in(changelog.parent().unwrap())?;
    filter(
        &args.version,
        &format!("{}", Local::now().format("%Y-%m-%d")),
        BufReader::new(File::open(&changelog)?),
        &mut tempfile,
    )?;
    tempfile.persist(changelog)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;
    use std::{io::Cursor, str};

    fn run_success_test(version: &str, today: &str, input: &str, expected_output: &str) {
        let mut output = vec![];
        filter(version, today, Cursor::new(input), Cursor::new(&mut output)).unwrap();
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

    fn run_failure_test(version: &str, expected_error: &str, input: &str) {
        let mut output = vec![];
        assert_eq!(
            filter(
                version,
                "1900-01-01",
                Cursor::new(input),
                Cursor::new(&mut output)
            )
            .unwrap_err()
            .to_string(),
            expected_error
        );
    }

    #[test]
    fn test_success() {
        run_success_test(
            "0.7.0",
            "2024-07-04",
            indoc! {r###"
                # Changelog

                Stuff

                ## [Unreleased]

                ### General

                  - Cool stuff.
                  - More cool stuff.

                ## [0.2.0] - 2024-01-2

                ## [0.1.0] - 2024-01-1

                [unreleased]: https://github.com/maelstrom-software/maelstrom/compare/v0.2.0...HEAD
                [0.2.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.1.0...v0.2.0
                [0.1.0]: https://github.com/maelstrom-software/maelstrom/releases/tag/v0.1.0
            "###},
            indoc! {r###"
                # Changelog

                Stuff

                ## [0.7.0] - 2024-07-04

                ### General

                  - Cool stuff.
                  - More cool stuff.

                ## [0.2.0] - 2024-01-2

                ## [0.1.0] - 2024-01-1

                [0.7.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.2.0...v0.7.0
                [0.2.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.1.0...v0.2.0
                [0.1.0]: https://github.com/maelstrom-software/maelstrom/releases/tag/v0.1.0
            "###},
        );
    }

    #[test]
    fn test_failure_no_unreleased_header() {
        run_failure_test(
            "0.3.0",
            r#"couldn't find "Unreleased" version header"#,
            indoc! {r###"
                # Changelog
                ## [0.2.0] - 2024-01-2
                ## [0.1.0] - 2024-01-1
                [unreleased]: https://github.com/maelstrom-software/maelstrom/compare/v0.2.0...HEAD
                [0.2.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.1.0...v0.2.0
                [0.1.0]: https://github.com/maelstrom-software/maelstrom/releases/tag/v0.1.0
            "###},
        );
    }

    #[test]
    fn test_failure_no_unreleased_link() {
        run_failure_test(
            "0.3.0",
            r#"couldn't find reference-style link for "unreleased""#,
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
}
