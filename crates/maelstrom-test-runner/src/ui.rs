mod fancy;
mod simple;

use crate::{config::Quiet, LoggingOutput};
use anyhow::Result;
use maelstrom_client::IntrospectResponse;
use serde::{Deserialize, Serialize};
use std::sync::mpsc::{Receiver, Sender};
use std::time::Duration;
use std::{fmt, str};
use strum::EnumIter;

pub use simple::SimpleUi;

pub struct UiHandle {
    handle: std::thread::JoinHandle<Result<()>>,
    logging_output: LoggingOutput,
    log: slog::Logger,
}

impl UiHandle {
    /// Wait for the UI thread to exit and return any error it had. This must be called after the
    /// associated `UiSender` has been destroyed, or else it will wait forever.
    pub fn join(self) -> Result<()> {
        self.logging_output.display_on_term();
        let ui_res = self.handle.join().unwrap();
        slog::debug!(self.log, "UI thread joined");
        ui_res
    }
}

pub trait Ui: Send + Sync + 'static {
    fn run(&mut self, recv: Receiver<UiMessage>) -> Result<()>;

    fn start_ui_thread(
        mut self,
        logging_output: LoggingOutput,
        log: slog::Logger,
    ) -> (UiHandle, UiSender)
    where
        Self: Sized,
    {
        let (ui_send, ui_recv) = std::sync::mpsc::channel();
        let ui_sender = UiSender::new(ui_send);
        let thread_handle = std::thread::spawn(move || self.run(ui_recv));
        logging_output.display_on_ui(ui_sender.clone());

        (
            UiHandle {
                logging_output,
                log,
                handle: thread_handle,
            },
            ui_sender,
        )
    }
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
    BuildOutputChunk(Vec<u8>),
    SlogRecord(slog_async::AsyncRecord),
    List(String),
    JobFinished(UiJobResult),
    UpdatePendingJobsCount(u64),
    JobEnqueued(String),
    UpdateIntrospectState(IntrospectResponse),
    UpdateEnqueueStatus(String),
    DoneBuilding,
    DoneQueuingJobs,
    AllJobsFinished(UiJobSummary),
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
    /// Should be sent when no more build output is expected.
    /// You shouldn't call [`Self::build_output_line`] or [`Self::build_output_chunk`] after
    /// calling this.
    pub fn done_building(&self) {
        let _ = self.send.send(UiMessage::DoneBuilding);
    }

    /// Display a given line a build output.
    pub fn build_output_line(&self, line: String) {
        let _ = self.send.send(UiMessage::BuildOutputLine(line));
    }

    /// Send a chunk of TTY data as build output. This is parsed by a vt100 parser.
    pub fn build_output_chunk(&self, chunk: &[u8]) {
        let _ = self.send.send(UiMessage::BuildOutputChunk(chunk.into()));
    }

    /// Display a slog log messages.
    pub fn slog_record(&self, r: slog_async::AsyncRecord) {
        let _ = self.send.send(UiMessage::SlogRecord(r));
    }

    /// When being used for listing tests, display a line of listing output.
    pub fn list(&self, line: String) {
        let _ = self.send.send(UiMessage::List(line));
    }

    /// Should be sent when a test job has finished.
    pub fn job_finished(&self, res: UiJobResult) {
        let _ = self.send.send(UiMessage::JobFinished(res));
    }

    /// Update the total number of tests we expect to run by the time we are done.
    pub fn update_length(&self, new_length: u64) {
        let _ = self
            .send
            .send(UiMessage::UpdatePendingJobsCount(new_length));
    }

    /// Sent when we've enqueued a new test job.
    pub fn job_enqueued(&self, name: String) {
        let _ = self.send.send(UiMessage::JobEnqueued(name));
    }

    /// Update the status message. This messages is displayed until [`Self::done_queuing_jobs`] is
    /// called.
    pub fn update_enqueue_status(&self, msg: impl Into<String>) {
        let _ = self.send.send(UiMessage::UpdateEnqueueStatus(msg.into()));
    }

    /// Update the UI with the latest job status information.
    pub fn update_introspect_state(&self, resp: IntrospectResponse) {
        let _ = self.send.send(UiMessage::UpdateIntrospectState(resp));
    }

    /// Should be called when all test jobs that are going to be enqueued are enqueued. This should
    /// cause the enqueue status set by [`Self::update_enqueue_status`] to disappear.
    pub fn done_queuing_jobs(&self) {
        let _ = self.send.send(UiMessage::DoneQueuingJobs);
    }

    /// Should be called when all the test jobs are completed. This may cause the UI to display
    /// some kind of "results" or "summary" to the user.
    pub fn finished(&self, summary: UiJobSummary) -> Result<()> {
        let _ = self.send.send(UiMessage::AllJobsFinished(summary));
        Ok(())
    }
}

pub struct UiSlogDrain(UiSender);

impl UiSlogDrain {
    pub fn new(ui: UiSender) -> Self {
        Self(ui)
    }
}

impl slog::Drain for UiSlogDrain {
    type Ok = ();
    type Err = <slog::Fuse<slog_async::Async> as slog::Drain>::Err;

    fn log(
        &self,
        record: &slog::Record<'_>,
        values: &slog::OwnedKVList,
    ) -> Result<Self::Ok, Self::Err> {
        self.0
            .slog_record(slog_async::AsyncRecord::from(record, values));
        Ok(())
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, EnumIter)]
#[serde(rename_all = "kebab-case")]
pub enum UiKind {
    Auto,
    Simple,
    Fancy,
}

impl fmt::Display for UiKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Simple => write!(f, "simple"),
            Self::Fancy => write!(f, "fancy"),
            Self::Auto => write!(f, "auto"),
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
            "auto" => Ok(Self::Auto),
            ui_name => Err(UnknownUiError {
                ui_name: ui_name.into(),
            }),
        }
    }
}

#[test]
fn ui_kind_parsing_and_fmt() {
    use strum::IntoEnumIterator as _;

    for k in UiKind::iter() {
        let s = k.to_string();

        // parse from Display::fmt value
        let parsed: UiKind = s.parse().unwrap();
        assert_eq!(parsed, k);

        // TOML value serialization matches Display::fmt value
        let mut toml_str = String::new();
        let serializer = toml::ser::ValueSerializer::new(&mut toml_str);
        Serialize::serialize(&k, serializer).unwrap();
        assert_eq!(&toml_str, &format!("\"{s}\""));

        // TOML value deserialization matches original value
        let toml_v = UiKind::deserialize(toml::de::ValueDeserializer::new(&toml_str)).unwrap();
        assert_eq!(toml_v, k);
    }
}

pub fn factory(kind: UiKind, list: bool, stdout_is_tty: bool, quiet: Quiet) -> Result<Box<dyn Ui>> {
    Ok(match kind {
        UiKind::Simple => Box::new(SimpleUi::new(
            list,
            stdout_is_tty,
            quiet,
            console::Term::buffered_stdout(),
        )),
        UiKind::Fancy => Box::new(fancy::FancyUi::new(list, stdout_is_tty, quiet)?),
        UiKind::Auto => {
            if list || !stdout_is_tty || quiet.into_inner() {
                factory(UiKind::Simple, list, stdout_is_tty, quiet)?
            } else {
                factory(UiKind::Fancy, list, stdout_is_tty, quiet)?
            }
        }
    })
}
