use super::{Terminal, Ui, UiJobSummary, UiMessage};
use crate::util::StdoutTty;
use anyhow::{bail, Result};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use std::sync::mpsc::Receiver;

fn create_bar<TermT: Terminal>(term: TermT) -> ProgressBar {
    let bar = ProgressBar::new(0).with_message("tests").with_style(
        ProgressStyle::with_template("{wide_bar:.white} {pos}/{len} {msg:5}")
            .unwrap()
            .progress_chars("##-"),
    );
    bar.set_draw_target(ProgressDrawTarget::term_like_with_hz(Box::new(term), 20));
    bar
}

pub struct QuietUi<TermT> {
    progress_bar: Option<ProgressBar>,
    term: TermT,
}

impl<TermT> QuietUi<TermT>
where
    TermT: Terminal,
{
    pub fn new(list: bool, stdout_tty: StdoutTty, term: TermT) -> Result<Self> {
        if list {
            bail!("`--ui quiet` doesn't support listing");
        }
        Ok(Self {
            progress_bar: stdout_tty.as_bool().then(|| create_bar(term.clone())),
            term,
        })
    }

    fn update_pending_jobs_count(&mut self, count: u64) {
        if let Some(bar) = &mut self.progress_bar {
            bar.set_length(count);
        }
    }

    fn job_finished(&mut self) {
        if let Some(bar) = &mut self.progress_bar {
            bar.inc(1);
        }
    }

    fn finished(&mut self, summary: UiJobSummary) -> Result<()> {
        if let Some(bar) = &mut self.progress_bar {
            bar.finish_and_clear();
        }

        for line in summary.to_lines(self.term.width() as usize) {
            self.term.write_line(&line)?;
        }
        self.term.flush()?;

        Ok(())
    }
}

impl<TermT> Ui for QuietUi<TermT>
where
    TermT: Terminal,
{
    fn run(&mut self, recv: Receiver<UiMessage>) -> Result<()> {
        let mut collection_output = String::new();
        while let Ok(msg) = recv.recv() {
            match msg {
                UiMessage::JobFinished(_) => self.job_finished(),
                UiMessage::UpdatePendingJobsCount(count) => self.update_pending_jobs_count(count),
                UiMessage::AllJobsFinished(summary) => self.finished(summary)?,
                UiMessage::CollectionOutput(output) => {
                    collection_output += &output;
                }
                _ => continue,
            }
        }

        if let Some(bar) = &mut self.progress_bar {
            bar.finish_and_clear();
        }
        self.term.write_str(&collection_output)?;
        self.term.flush()?;

        Ok(())
    }
}
