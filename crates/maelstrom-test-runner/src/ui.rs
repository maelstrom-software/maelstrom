mod fancy;
mod simple;

use crate::config::Quiet;
use anyhow::Result;
use maelstrom_client::IntrospectResponse;
use serde::{Deserialize, Serialize};
use std::sync::mpsc::{Receiver, Sender};
use std::time::Duration;
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

pub enum UiJobStatus {
    Ok,
    Failure(Option<String>),
    TimedOut,
    Error(String),
    Ignored,
}

impl UiJobStatus {
    fn details(self) -> Option<String> {
        match self {
            Self::Failure(d) => d,
            Self::Error(d) => Some(d),
            _ => None,
        }
    }
}

pub struct UiJobSummary {
    pub failed: Vec<String>,
    pub ignored: Vec<String>,
    pub succeeded: usize,
}

pub struct UiJobResult {
    pub name: String,
    pub duration: Option<Duration>,
    pub status: UiJobStatus,
    pub stdout: Vec<String>,
    pub stderr: Vec<String>,
}

pub enum UiMessage {
    BuildOutputLine(String),
    LogMessage(String),
    List(String),
    JobFinished(UiJobResult),
    UpdatePendingJobsCount(u64),
    UpdateIntrospectState(IntrospectResponse),
    UpdateEnqueueStatus(String),
    DoneBuilding,
    DoneQueuingJobs,
    AllJobsFinished(UiJobSummary),
    Shutdown,
}

#[derive(Clone)]
pub struct UiSender {
    send: Sender<UiMessage>,
}

impl UiSender {
    pub fn new(send: Sender<UiMessage>) -> Self {
        Self { send }
    }
}

impl UiSender {
    pub fn shutdown(&self) {
        let _ = self.send.send(UiMessage::Shutdown);
    }

    pub fn done_building(&self) {
        let _ = self.send.send(UiMessage::DoneBuilding);
    }

    pub fn build_output_line(&self, line: String) {
        let _ = self.send.send(UiMessage::BuildOutputLine(line));
    }

    pub fn log_message(&self, line: String) {
        let _ = self.send.send(UiMessage::LogMessage(line));
    }

    pub fn list(&self, line: String) {
        let _ = self.send.send(UiMessage::List(line));
    }

    pub fn job_finished(&self, res: UiJobResult) {
        let _ = self.send.send(UiMessage::JobFinished(res));
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

    pub fn finished(&self, summary: UiJobSummary) -> Result<()> {
        let _ = self.send.send(UiMessage::AllJobsFinished(summary));
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
            self.send.log_message(line);
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
    Fancy,
}

impl fmt::Display for UiKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Simple => write!(f, "simple"),
            Self::Fancy => write!(f, "fancy"),
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
            "fancy" => Ok(Self::Fancy),
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
        UiKind::Fancy => Box::new(fancy::FancyUi::new(list, stdout_is_tty, quiet)),
    }
}
