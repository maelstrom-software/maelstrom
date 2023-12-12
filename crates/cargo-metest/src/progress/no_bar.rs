use super::ProgressIndicator;
use anyhow::Result;
use indicatif::TermLike;

#[derive(Clone)]
pub struct NoBar<Term> {
    term: Term,
}

impl<Term> NoBar<Term> {
    pub fn new(term: Term) -> Self {
        Self { term }
    }
}

impl<Term> ProgressIndicator for NoBar<Term>
where
    Term: TermLike + Clone + Send + Sync + 'static,
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
