mod fancy;
mod quiet;
mod simple;

use crate::{LoggingOutput, NotRunEstimate};
use anyhow::Result;
use derive_more::{From, Into};
use maelstrom_base::stats::{JobState, JobStateCounts};
use maelstrom_client::{IntrospectResponse, JobRunningStatus};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Weak};
use std::time::Duration;
use std::time::Instant;
use std::{fmt, str};
use strum::EnumIter;

pub use quiet::QuietUi;
pub use simple::SimpleUi;

pub trait Terminal:
    indicatif::TermLike + Clone + Send + Sync + UnwindSafe + RefUnwindSafe + 'static
{
}

impl<TermT> Terminal for TermT where
    TermT: indicatif::TermLike + Clone + Send + Sync + UnwindSafe + RefUnwindSafe + 'static
{
}

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

pub trait PrintWidthCb<'a, RetT>: FnOnce(usize) -> RetT + Send + Sync + 'a {}

impl<'a, PrintCbT, RetT> PrintWidthCb<'a, RetT> for PrintCbT where
    PrintCbT: FnOnce(usize) -> RetT + Send + Sync + 'a
{
}

#[derive(Debug, PartialEq, Eq)]
pub enum UiJobStatus {
    Ok,
    Failure(Option<String>),
    TimedOut,
    Error(String),
    Ignored,
}

impl UiJobStatus {
    fn details(&self) -> Option<&str> {
        match self {
            Self::Failure(Some(d)) => Some(d.as_str()),
            Self::Error(d) => Some(d.as_str()),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct UiJobSummary {
    pub failed: Vec<String>,
    pub ignored: Vec<String>,
    pub succeeded: usize,
    pub not_run: Option<NotRunEstimate>,
}

impl UiJobSummary {
    /// Formatting code used by simple and quiet UI
    fn to_lines(&self, width: usize) -> Vec<String> {
        use colored::Colorize as _;
        use unicode_width::UnicodeWidthStr as _;

        let mut summary_lines = vec![];
        summary_lines.push("".into());

        let heading = " Test Summary ";
        let equal_width = (width - heading.width()) / 2;
        summary_lines.push(format!(
            "{empty:=<equal_width$}{heading}{empty:=<equal_width$}",
            empty = ""
        ));
        let success = "Successful Tests";
        let failure = "Failed Tests";
        let ignore = "Ignored Tests";
        let not_run = "Tests Not Run";
        let mut column1_width = std::cmp::max(success.width(), failure.width());
        let max_digits = 9;
        let num_failed = self.failed.len();
        let num_ignored = self.ignored.len();
        let num_not_run = self.not_run;
        let num_succeeded = self.succeeded;
        if num_ignored > 0 {
            column1_width = std::cmp::max(column1_width, ignore.width());
        }
        if num_not_run.is_some() {
            column1_width = std::cmp::max(column1_width, not_run.width());
        }
        summary_lines.push(format!(
            "{:<column1_width$}: {num_succeeded:>max_digits$}",
            success.green(),
        ));
        summary_lines.push(format!(
            "{:<column1_width$}: {num_failed:>max_digits$}",
            failure.red(),
        ));
        let failed_width = self.failed.iter().map(|n| n.width()).max().unwrap_or(0);
        for failed in &self.failed {
            summary_lines.push(format!("    {failed:<failed_width$}: {}", "failure".red()));
        }
        if num_ignored > 0 {
            summary_lines.push(format!(
                "{:<column1_width$}: {num_ignored:>max_digits$}",
                ignore.yellow(),
            ));
            let ignored_width = self.ignored.iter().map(|n| n.width()).max().unwrap_or(0);
            for ignored in &self.ignored {
                summary_lines.push(format!(
                    "    {ignored:<ignored_width$}: {}",
                    "ignored".yellow()
                ));
            }
        }
        if let Some(num_not_run) = num_not_run {
            summary_lines.push(format!(
                "{:<column1_width$}: {num_not_run:>max_digits$}",
                not_run.red(),
            ));
        }
        summary_lines
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct UiJobResult {
    pub name: String,
    pub job_id: UiJobId,
    pub duration: Option<Duration>,
    pub status: UiJobStatus,
    pub stdout: Vec<String>,
    pub stderr: Vec<String>,
}

#[derive(Debug, Copy, Clone, From, Into, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct UiJobId(u32);

#[derive(Debug, PartialEq, Eq)]
pub struct UiJobUpdate {
    pub job_id: UiJobId,
    pub status: JobRunningStatus,
}

#[derive(Debug, PartialEq, Eq)]
pub struct UiJobEnqueued {
    pub job_id: UiJobId,
    pub name: String,
}

pub struct LogRecord(slog_async::AsyncRecord);

// The tests compare UiMessages but slog_async::AsyncRecord isn't comparable, so we hack around
// that here.
impl PartialEq<Self> for LogRecord {
    fn eq(&self, _other: &Self) -> bool {
        unimplemented!()
    }
}

impl Eq for LogRecord {}

impl fmt::Debug for LogRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LogRecord(..)")
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum UiMessage {
    BuildOutputLine(String),
    BuildOutputChunk(Vec<u8>),
    SlogRecord(LogRecord),
    List(String),
    JobUpdated(UiJobUpdate),
    JobFinished(UiJobResult),
    UpdatePendingJobsCount(u64),
    JobEnqueued(UiJobEnqueued),
    UpdateIntrospectState(IntrospectResponse),
    UpdateEnqueueStatus(String),
    DoneBuilding,
    DoneQueuingJobs,
    AllJobsFinished(UiJobSummary),
}

#[derive(Clone)]
pub struct UiSender {
    send: Arc<Sender<UiMessage>>,
}

impl UiSender {
    pub fn new(send: Sender<UiMessage>) -> Self {
        Self {
            send: Arc::new(send),
        }
    }

    pub fn downgrade(&self) -> UiWeakSender {
        UiWeakSender {
            send: Arc::downgrade(&self.send),
        }
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
        let _ = self.send.send(UiMessage::SlogRecord(LogRecord(r)));
    }

    /// When being used for listing tests, display a line of listing output.
    pub fn list(&self, line: String) {
        let _ = self.send.send(UiMessage::List(line));
    }

    /// Should be sent when a test job has changed status.
    pub fn job_updated(&self, status: UiJobUpdate) {
        let _ = self.send.send(UiMessage::JobUpdated(status));
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
    pub fn job_enqueued(&self, msg: UiJobEnqueued) {
        let _ = self.send.send(UiMessage::JobEnqueued(msg));
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
    pub fn finished(&self, summary: UiJobSummary) {
        let _ = self.send.send(UiMessage::AllJobsFinished(summary));
    }

    pub fn send_raw(&self, msg: UiMessage) {
        let _ = self.send.send(msg);
    }
}

#[derive(Clone)]
pub struct UiWeakSender {
    send: Weak<Sender<UiMessage>>,
}

impl UiWeakSender {
    pub fn upgrade(&self) -> Option<UiSender> {
        self.send.upgrade().map(|send| UiSender { send })
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
    Quiet,
}

impl fmt::Display for UiKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Simple => write!(f, "simple"),
            Self::Fancy => write!(f, "fancy"),
            Self::Auto => write!(f, "auto"),
            Self::Quiet => write!(f, "quiet"),
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
            "quiet" => Ok(Self::Quiet),
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

pub fn factory(kind: UiKind, list: bool, stdout_is_tty: bool) -> Result<Box<dyn Ui>> {
    Ok(match kind {
        UiKind::Simple => Box::new(SimpleUi::new(
            list,
            stdout_is_tty,
            console::Term::buffered_stdout(),
        )),
        UiKind::Fancy => Box::new(fancy::FancyUi::new(list, stdout_is_tty)?),
        UiKind::Quiet => Box::new(quiet::QuietUi::new(
            list,
            stdout_is_tty,
            console::Term::buffered_stdout(),
        )?),
        UiKind::Auto => {
            if list || !stdout_is_tty {
                factory(UiKind::Simple, list, stdout_is_tty)?
            } else {
                factory(UiKind::Fancy, list, stdout_is_tty)?
            }
        }
    })
}

#[derive(Default)]
struct JobStatusEntry {
    name: String,
    status: Option<JobRunningStatus>,
    start_time: Option<Instant>,
}

impl JobStatusEntry {
    fn new(name: String) -> Self {
        Self {
            name,
            status: None,
            start_time: None,
        }
    }

    fn is_state(&self, state: JobState) -> bool {
        match &self.status {
            Some(s) => s.to_state() == state,
            None => state == JobState::WaitingForArtifacts,
        }
    }

    fn update(&mut self, status: JobRunningStatus) {
        self.status = Some(status);
        if self.start_time.is_none() && self.is_state(JobState::Running) {
            self.start_time = Some(Instant::now());
        }
    }
}

struct CompletedJob {
    name: String,
    status: UiJobStatus,
}

impl CompletedJob {
    fn is_failure(&self) -> bool {
        matches!(
            self.status,
            UiJobStatus::Failure(_) | UiJobStatus::Error(_) | UiJobStatus::TimedOut
        )
    }
}

impl From<UiJobResult> for CompletedJob {
    fn from(res: UiJobResult) -> Self {
        Self {
            name: res.name,
            status: res.status,
        }
    }
}

#[derive(Default)]
struct JobStatuses {
    running: HashMap<UiJobId, JobStatusEntry>,
    completed: HashMap<UiJobId, CompletedJob>,
}

impl JobStatuses {
    fn job_updated(&mut self, job_id: UiJobId, status: JobRunningStatus) {
        self.running.get_mut(&job_id).unwrap().update(status)
    }

    fn job_finished(&mut self, res: UiJobResult) {
        self.running.remove(&res.job_id);
        self.completed.insert(res.job_id, CompletedJob::from(res));
    }

    fn job_enqueued(&mut self, job_id: UiJobId, name: String) {
        self.running.insert(job_id, JobStatusEntry::new(name));
    }

    fn waiting_for_artifacts(&self) -> u64 {
        self.running
            .values()
            .filter(|e| e.is_state(JobState::WaitingForArtifacts))
            .count() as u64
    }

    fn pending(&self) -> u64 {
        self.running
            .values()
            .filter(|e| e.is_state(JobState::Pending))
            .count() as u64
    }

    fn running(&self) -> u64 {
        self.running
            .values()
            .filter(|e| e.is_state(JobState::Running))
            .count() as u64
    }

    fn completed(&self) -> u64 {
        self.completed.len() as u64
    }

    fn failed(&self) -> u64 {
        self.completed.values().filter(|j| j.is_failure()).count() as u64
    }

    fn running_tests(&self) -> impl Iterator<Item = (&str, &Instant)> {
        self.running
            .values()
            .filter(|t| t.is_state(JobState::Running))
            .map(|t| (t.name.as_str(), t.start_time.as_ref().unwrap()))
    }

    fn failed_tests(&self) -> impl Iterator<Item = &CompletedJob> {
        self.completed.values().filter(|t| t.is_failure())
    }

    fn counts(&self) -> JobStateCounts {
        let mut counts = JobStateCounts::default();
        counts[JobState::WaitingForArtifacts] = self.waiting_for_artifacts();
        counts[JobState::Pending] = self.pending();
        counts[JobState::Running] = self.running();
        counts[JobState::Complete] = self.completed();
        counts
    }
}
