use super::{NullPrinter, ProgressIndicator};
use anyhow::Result;
use indicatif::{ProgressBar, ProgressDrawTarget, TermLike};

#[derive(Clone)]
pub struct QuietProgressBar {
    bar: ProgressBar,
}

impl QuietProgressBar {
    pub fn new(term: impl TermLike + 'static) -> Self {
        let bar = super::make_progress_bar("white", "jobs", 4, false);
        bar.set_draw_target(ProgressDrawTarget::term_like_with_hz(Box::new(term), 20));
        Self { bar }
    }
}

impl ProgressIndicator for QuietProgressBar {
    type Printer<'a> = NullPrinter;

    fn lock_printing(&self) -> Self::Printer<'_> {
        // quiet mode doesn't print anything
        NullPrinter
    }

    fn job_finished(&self) {
        self.bar.inc(1);
    }

    fn update_length(&self, new_length: u64) {
        self.bar.set_length(new_length);
    }

    fn finished(&self) -> Result<()> {
        self.bar.finish_and_clear();
        Ok(())
    }
}
