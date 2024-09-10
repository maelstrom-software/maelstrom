use crate::config::{Repeat, StopAfter};
use crate::ui::Ui;
use crate::*;
use maelstrom_base::Timeout;
use maelstrom_client::{ProjectDir, StateDir};
use maelstrom_util::{process::ExitCode, root::Root};

pub struct MainAppCombinedDeps<MainAppDepsT: MainAppDeps> {
    _abstract_deps: MainAppDepsT,
}

impl<MainAppDepsT: MainAppDeps> MainAppCombinedDeps<MainAppDepsT> {
    /// Creates a new `MainAppCombinedDeps`
    ///
    /// `bg_proc`: handle to background client process
    /// `include_filter`: tests which match any of the patterns in this filter are run
    /// `exclude_filter`: tests which match any of the patterns in this filter are not run
    /// `list_action`: if some, tests aren't run, instead tests or other things are listed
    /// `stderr_color`: should terminal color codes be written to `stderr` or not
    /// `project_dir`: the path to the root of the project
    /// `broker_addr`: the network address of the broker which we connect to
    /// `client_driver`: an object which drives the background work of the `Client`
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        _abstract_deps: MainAppDepsT,
        _include_filter: Vec<String>,
        _exclude_filter: Vec<String>,
        _list_action: Option<ListAction>,
        _repeat: Repeat,
        _stop_after: Option<StopAfter>,
        _stderr_color: bool,
        _project_dir: impl AsRef<Root<ProjectDir>>,
        _state_dir: impl AsRef<Root<StateDir>>,
        _collector_options: CollectOptionsM<MainAppDepsT>,
        _log: slog::Logger,
    ) -> Result<Self> {
        todo!()
    }
}

/// Run the given `[Ui]` implementation on a background thread, and run the main test-runner
/// application on this thread using the UI until it is completed.
pub fn run_app_with_ui_multithreaded<MainAppDepsT>(
    _deps: MainAppCombinedDeps<MainAppDepsT>,
    _logging_output: LoggingOutput,
    _timeout_override: Option<Option<Timeout>>,
    _ui: impl Ui,
) -> Result<ExitCode>
where
    MainAppDepsT: MainAppDeps,
{
    todo!()
}
