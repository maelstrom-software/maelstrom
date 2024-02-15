use anyhow::{bail, Context as _, Result};
use camino::Utf8PathBuf;
use clap::Parser;
use std::{
    fs::File,
    io::{self, BufRead, BufReader, BufWriter, Write},
};

fn filter(version: &str, input: impl BufRead, mut output: impl Write) -> Result<()> {
    let start_line = format!("## [{}]", version);
    #[derive(Eq, PartialEq)]
    enum State {
        Before,
        In { written_anything: bool, blanks: u32 },
        After,
    }
    let mut state = State::Before;
    for line in input.lines() {
        let line = line?;
        match state {
            State::Before => {
                if line.starts_with(&start_line) {
                    state = State::In {
                        written_anything: false,
                        blanks: 0,
                    };
                }
            }
            State::In {
                ref mut written_anything,
                ref mut blanks,
            } => {
                if line.starts_with(&start_line) {
                    bail!("multiple sections for version {version}");
                } else if line.starts_with("## [") || line.starts_with('[') {
                    if *written_anything {
                        state = State::After;
                    } else {
                        bail!("empty section for version {version}");
                    }
                } else if line.trim().is_empty() {
                    *blanks += 1;
                } else {
                    if *written_anything {
                        for _ in 0..*blanks {
                            writeln!(output)?;
                        }
                    }
                    writeln!(output, "{line}")?;
                    *written_anything = true;
                    *blanks = 0;
                }
            }
            State::After => {
                if line.starts_with(&start_line) {
                    bail!("multiple sections for version {version}");
                }
            }
        }
    }
    match state {
        State::Before => {
            bail!("no section found for version {version}");
        }
        State::In {
            written_anything: false,
            ..
        } => {
            bail!("empty section for version {version}");
        }
        _ => {}
    }
    Ok(())
}

/// Extract release notes from changelog.
#[derive(Debug, Parser)]
pub struct CliArgs {
    /// Release version.
    version: String,

    /// Where to put release notes. If not given, standard output is used.
    #[arg(long, short)]
    output: Option<Utf8PathBuf>,
}

pub fn main(changelog: Utf8PathBuf, args: CliArgs) -> Result<()> {
    filter(
        &args.version,
        BufReader::new(File::open(changelog).context("opening input file")?),
        BufWriter::new(match args.output {
            Some(filename) => Box::new(File::create(filename)?) as Box<dyn Write>,
            None => Box::new(io::stdout()) as Box<dyn Write>,
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;
    use std::{io::Cursor, str};

    fn run_success_test(version: &str, input: &str, expected_output: &str) {
        let mut output = vec![];
        filter(version, Cursor::new(input), Cursor::new(&mut output)).unwrap();
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
            filter(version, Cursor::new(input), Cursor::new(&mut output))
                .unwrap_err()
                .to_string(),
            expected_error
        );
    }

    #[test]
    fn success_section_goes_to_another_section() {
        run_success_test(
            "1.2.3",
            indoc! {r#"
                ## [1.2.2] Something

                ### Subtitle

                ## [1.2.3] Something Else

                ### Subheading 1

                ### Subheading 2

                ## [1.2.4] Another Thing
            "#},
            indoc! {r#"
                ### Subheading 1

                ### Subheading 2
            "#},
        );
    }

    #[test]
    fn success_section_goes_to_eof() {
        run_success_test(
            "1.2.3",
            indoc! {r#"
                ## [1.2.2] Something

                ### Subtitle

                ## [1.2.3] Something Else

                ### Subheading 1
                ### Subheading 2

            "#},
            indoc! {r#"
                ### Subheading 1
                ### Subheading 2
            "#},
        );
    }

    #[test]
    fn success_section_goes_to_links() {
        run_success_test(
            "1.2.3",
            indoc! {r#"
                ## [1.2.2] Something

                ### Subtitle

                ## [1.2.3] Something Else

                ### Subheading 1
                ### Subheading 2

                [1.2.3]: https://foo.bar/1.2.3
            "#},
            indoc! {r#"
                ### Subheading 1
                ### Subheading 2
            "#},
        );
    }

    #[test]
    fn failure_no_section() {
        run_failure_test(
            "1.2.3",
            "no section found for version 1.2.3",
            indoc! {r#"
                ## [1.2.2] Something
                ## [1.2.1] Something Else
            "#},
        );
    }

    #[test]
    fn failure_empty_section_goes_to_another_section() {
        run_failure_test(
            "1.2.3",
            "empty section for version 1.2.3",
            indoc! {r#"
                ## [1.2.3] Something

                ## [1.2.2] Something Else
            "#},
        );
    }

    #[test]
    fn failure_empty_section_goes_to_links() {
        run_failure_test(
            "1.2.3",
            "empty section for version 1.2.3",
            indoc! {r#"
                ## [1.2.3] Something

                [1.2.3]: https://foo.bar/1.2.3
            "#},
        );
    }

    #[test]
    fn failure_empty_section_goes_to_eof() {
        run_failure_test(
            "1.2.3",
            "empty section for version 1.2.3",
            indoc! {r#"
                ## [1.2.3] Something

            "#},
        );
    }

    #[test]
    fn failure_multiple_sections_separated() {
        run_failure_test(
            "1.2.3",
            "multiple sections for version 1.2.3",
            indoc! {r#"
                ## [1.2.3] Something

                Blah, blah.

                ## [1.2.3] Something
            "#},
        );
    }

    #[test]
    fn failure_multiple_sections_contiguous() {
        run_failure_test(
            "1.2.3",
            "multiple sections for version 1.2.3",
            indoc! {r#"
                ## [1.2.3] Something

                Blah, blah.

                ## [1.2.2] Something Else

                Blah, blah.

                ## [1.2.3] Something
            "#},
        );
    }
}
