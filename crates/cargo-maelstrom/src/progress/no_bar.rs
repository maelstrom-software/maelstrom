use super::ProgressIndicator;
use anyhow::Result;
use indicatif::TermLike;

#[derive(Clone)]
pub struct NoBar<TermT> {
    term: TermT,
}

impl<TermT> NoBar<TermT> {
    pub fn new(term: TermT) -> Self {
        Self { term }
    }
}

impl<TermT> ProgressIndicator for NoBar<TermT>
where
    TermT: TermLike + Clone + Send + Sync + 'static,
{
    fn println(&self, msg: String) {
        self.term.write_line(&msg).ok();
    }

    fn finished(&self) -> Result<()> {
        self.term.write_line("all jobs completed")?;
        self.term.flush()?;
        Ok(())
    }
}
