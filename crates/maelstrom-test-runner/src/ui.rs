mod simple;

use crate::config::Quiet;
use anyhow::Result;
use colored::Colorize as _;
use maelstrom_client::IntrospectResponse;
use serde::{Deserialize, Serialize};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex, MutexGuard};
use std::{fmt, io, str};

pub use simple::SimpleUi;

pub trait Ui: Send + Sync + 'static {
    fn run(&mut self, recv: Receiver<UiMessage>) -> Result<()>;
}

impl Ui for Box<dyn Ui> {
    fn run(&mut self, recv: Receiver<UiMessage>) -> Result<()> {
        Ui::run(&mut **self, recv)
    }
}

pub trait PrintWidthCb<RetT>: FnOnce(usize) -> RetT + Send + Sync + 'static {}

impl<PrintCbT, RetT> PrintWidthCb<RetT> for PrintCbT where
    PrintCbT: FnOnce(usize) -> RetT + Send + Sync + 'static
{
}

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

impl<'a> UiSenderPrinter<'a> {
    pub fn println(&self, msg: String) {
        let _ = self.send.send(UiMessage::PrintLine(msg));
    }

    pub fn println_width(&self, cb: impl FnOnce(usize) -> String + Send + Sync + 'static) {
        let _ = self.send.send(UiMessage::PrintLineWidth(Box::new(cb)));
    }

    pub fn eprintln(&self, msg: impl AsRef<str>) {
        for line in msg.as_ref().lines() {
            self.println(format!("{} {line}", "stderr:".red()))
        }
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

impl UiSender {
    pub fn lock_printing(&self) -> UiSenderPrinter<'_> {
        UiSenderPrinter {
            send: self.send.clone(),
            _guard: self.print_lock.lock().unwrap(),
        }
    }

    pub fn job_finished(&self) {
        let _ = self.send.send(UiMessage::JobFinished);
    }

    pub fn update_length(&self, new_length: u64) {
        let _ = self
            .send
            .send(UiMessage::UpdatePendingJobsCount(new_length));
    }

    pub fn update_enqueue_status(&self, msg: impl Into<String>) {
        let _ = self.send.send(UiMessage::UpdateEnqueueStatus(msg.into()));
    }

    pub fn update_introspect_state(&self, resp: IntrospectResponse) {
        let _ = self.send.send(UiMessage::UpdateIntrospectState(resp));
    }

    pub fn done_queuing_jobs(&self) {
        let _ = self.send.send(UiMessage::DoneQueuingJobs);
    }

    pub fn finished(&self, summary: impl PrintWidthCb<Vec<String>>) -> Result<()> {
        let _ = self
            .send
            .send(UiMessage::AllJobsFinished(Box::new(summary)));
        Ok(())
    }
}

pub struct UiSenderWriteAdapter {
    send: UiSender,
    line: String,
}

impl UiSenderWriteAdapter {
    pub fn new(send: UiSender) -> Self {
        Self {
            send,
            line: String::new(),
        }
    }
}

impl io::Write for UiSenderWriteAdapter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.line += &String::from_utf8_lossy(buf);
        if let Some(p) = self.line.bytes().position(|b| b == b'\n') {
            let remaining = self.line.split_off(p);
            let line = std::mem::replace(&mut self.line, remaining[1..].into());
            self.send.lock_printing().println(line);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum UiKind {
    Simple,
}

impl fmt::Display for UiKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Simple => write!(f, "simple"),
        }
    }
}

#[derive(Debug)]
pub struct UnknownUiError {
    ui_name: String,
}

impl fmt::Display for UnknownUiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown UI {:?}", self.ui_name)
    }
}

impl std::error::Error for UnknownUiError {}

impl str::FromStr for UiKind {
    type Err = UnknownUiError;

    fn from_str(s: &str) -> std::result::Result<Self, UnknownUiError> {
        match s {
            "simple" => Ok(Self::Simple),
            ui_name => Err(UnknownUiError {
                ui_name: ui_name.into(),
            }),
        }
    }
}

pub fn factory(kind: UiKind, list: bool, stdout_is_tty: bool, quiet: Quiet) -> Box<dyn Ui> {
    match kind {
        UiKind::Simple => Box::new(SimpleUi::new(
            list,
            stdout_is_tty,
            quiet,
            console::Term::buffered_stdout(),
        )),
    }
}
