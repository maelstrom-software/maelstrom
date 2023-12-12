use super::ProgressIndicator;
use anyhow::Result;
use indicatif::TermLike;

#[derive(Clone)]
pub struct QuietNoBar<Term> {
    term: Term,
}

impl<Term> QuietNoBar<Term> {
    pub fn new(term: Term) -> Self {
        Self { term }
    }
}

impl<Term> ProgressIndicator for QuietNoBar<Term>
where
    Term: TermLike + Clone + Send + Sync + 'static,
{
    fn println(&self, _msg: String) {
        // quiet mode doesn't print anything
    }

    fn finished(&self) -> Result<()> {
        self.term.write_line("all jobs completed")?;
        self.term.flush()?;
        Ok(())
    }
}
