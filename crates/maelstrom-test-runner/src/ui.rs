mod simple;

use crate::config::Quiet;
use crate::progress::{PrintWidthCb, ProgressIndicator, ProgressPrinter, Terminal};
use anyhow::Result;
use derive_more::From;
use maelstrom_client::IntrospectResponse;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex, MutexGuard};

#[allow(dead_code)]
pub enum UiMessage {
    PrintLine(String),
    PrintLineWidth(Box<dyn PrintWidthCb<String>>),
    JobFinished,
    UpdatePendingJobsCount(u64),
    UpdateIntrospectState(IntrospectResponse),
    UpdateEnqueueStatus(String),
    DoneQueuingJobs,
    AllJobsFinished(Box<dyn PrintWidthCb<Vec<String>>>),
}

pub struct UiSenderPrinter<'a> {
    send: Sender<UiMessage>,
    _guard: MutexGuard<'a, ()>,
}

impl<'a> ProgressPrinter for UiSenderPrinter<'a> {
    fn println(&self, msg: String) {
        let _ = self.send.send(UiMessage::PrintLine(msg));
    }

    fn println_width(&self, cb: impl FnOnce(usize) -> String + Send + Sync + 'static) {
        let _ = self.send.send(UiMessage::PrintLineWidth(Box::new(cb)));
    }
}

#[derive(Clone)]
pub struct UiSender {
    send: Sender<UiMessage>,
    print_lock: Arc<Mutex<()>>,
}

impl UiSender {
    pub fn new(send: Sender<UiMessage>) -> Self {
        Self {
            send,
            print_lock: Default::default(),
        }
    }
}

impl ProgressIndicator for UiSender {
    type Printer<'a> = UiSenderPrinter<'a>;

    fn lock_printing(&self) -> Self::Printer<'_> {
        UiSenderPrinter {
            send: self.send.clone(),
            _guard: self.print_lock.lock().unwrap(),
        }
    }

    fn job_finished(&self) {
        let _ = self.send.send(UiMessage::JobFinished);
    }

    fn update_length(&self, new_length: u64) {
        let _ = self
            .send
            .send(UiMessage::UpdatePendingJobsCount(new_length));
    }

    fn update_enqueue_status(&self, msg: impl Into<String>) {
        let _ = self.send.send(UiMessage::UpdateEnqueueStatus(msg.into()));
    }

    fn update_introspect_state(&self, resp: IntrospectResponse) -> bool {
        self.send
            .send(UiMessage::UpdateIntrospectState(resp))
            .is_ok()
    }

    fn done_queuing_jobs(&self) {
        let _ = self.send.send(UiMessage::DoneQueuingJobs);
    }

    fn finished(&self, summary: impl PrintWidthCb<Vec<String>>) -> Result<()> {
        let _ = self
            .send
            .send(UiMessage::AllJobsFinished(Box::new(summary)));
        Ok(())
    }
}

pub enum UiKind {
    Simple,
}

#[derive(From)]
pub enum UiImpl<TermT> {
    Simple(simple::SimpleUi<TermT>),
}

impl<TermT> UiImpl<TermT>
where
    TermT: Terminal,
{
    pub fn new(
        kind: UiKind,
        spinner_message: &'static str,
        list: bool,
        stdout_is_tty: bool,
        quiet: Quiet,
        term: TermT,
    ) -> Self {
        match kind {
            UiKind::Simple => {
                simple::SimpleUi::new(spinner_message, list, stdout_is_tty, quiet, term).into()
            }
        }
    }
    pub fn run(&mut self, recv: Receiver<UiMessage>) -> Result<()> {
        match self {
            Self::Simple(u) => u.run(recv),
        }
    }
}
