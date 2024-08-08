use super::{PrintWidthCb, ProgressIndicator, Terminal};
use anyhow::Result;
use indicatif::{ProgressBar, ProgressDrawTarget};

#[derive(Clone)]
pub struct QuietProgressBar<TermT> {
    bar: ProgressBar,
    term: TermT,
}

impl<TermT> QuietProgressBar<TermT>
where
    TermT: Terminal,
{
    pub fn new(term: TermT) -> Self {
        let bar = super::make_main_progress_bar("white", "jobs", 4);
        bar.set_draw_target(ProgressDrawTarget::term_like_with_hz(
            Box::new(term.clone()),
            20,
        ));
        Self { bar, term }
    }
}

impl<TermT> ProgressIndicator for QuietProgressBar<TermT>
where
    TermT: Terminal,
{
    fn println(&mut self, _msg: String) {}
    fn println_width(&mut self, _cb: impl PrintWidthCb<String>) {}

    fn job_finished(&mut self) {
        self.bar.inc(1);
    }

    fn update_length(&mut self, new_length: u64) {
        self.bar.set_length(new_length);
    }

    fn finished(&mut self, summary: impl PrintWidthCb<Vec<String>>) -> Result<()> {
        self.bar.finish_and_clear();
        for line in summary(self.term.width() as usize) {
            self.term.write_line(&line)?;
        }
        self.term.flush()?;
        Ok(())
    }
}
