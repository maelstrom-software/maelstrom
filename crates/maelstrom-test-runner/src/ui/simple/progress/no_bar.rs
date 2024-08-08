use super::{PrintWidthCb, ProgressIndicator, Terminal};
use anyhow::Result;

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
    TermT: Terminal,
{
    fn println(&mut self, msg: String) {
        let _ = self.term.write_line(&msg);
        let _ = self.term.flush();
    }

    fn println_width(&mut self, cb: impl PrintWidthCb<String>) {
        self.println(cb(self.term.width() as usize));
    }

    fn finished(&mut self, summary: impl PrintWidthCb<Vec<String>>) -> Result<()> {
        for line in summary(self.term.width() as usize) {
            self.println(line)
        }
        Ok(())
    }
}
