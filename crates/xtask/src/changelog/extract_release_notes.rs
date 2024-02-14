use anyhow::{Context as _, Result};
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
        NotInSection,
        InSectionLeadingBlanks,
        InSection,
    }
    let mut state = State::NotInSection;
    let mut blanks = 0;
    for line in input.lines() {
        let line = line?;
        if line.starts_with(&start_line) {
            state = State::InSectionLeadingBlanks;
        } else if line.starts_with("## [") || line.starts_with('[') {
            state = State::NotInSection;
        } else if line.trim().is_empty() {
            blanks += 1;
        } else if state != State::NotInSection {
            if state == State::InSection {
                for _ in 0..blanks {
                    writeln!(output)?;
                }
            }
            writeln!(output, "{line}")?;
            blanks = 0;
            state = State::InSection;
        }
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

    fn filter_test(version: &str, input: &str, expected_output: &str) {
        let mut output = vec![];
        filter(version, Cursor::new(input), Cursor::new(&mut output)).unwrap();
        assert_eq!(str::from_utf8(&output).unwrap(), expected_output);
    }

    #[test]
    fn filter_no_output() {
        filter_test(
            "1.2.3",
            r#"
            a line

            another line
        "#,
            "",
        );
    }

    #[test]
    fn filter_section_goes_to_another_section() {
        filter_test(
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
    fn filter_section_goes_to_eof() {
        filter_test(
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
    fn filter_section_goes_to_links() {
        filter_test(
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
}
