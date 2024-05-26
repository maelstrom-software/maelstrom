pub mod cargo;
pub mod config;

use anyhow::{anyhow, bail, Context as _, Result};
use cargo_metadata::Metadata as CargoMetadata;
use indicatif::TermLike;
use maelstrom_base::Timeout;
use maelstrom_client::{
    CacheDir, Client, ClientBgProcess, ContainerImageDepotDir, ProjectDir, StateDir,
};
use maelstrom_test_runner::{
    alternative_mains, main_app_new, progress, ListAction, LoggingOutput, MainAppDeps,
    MainAppState, TargetDir, Wait, WorkspaceDir,
};
use maelstrom_util::{
    config::common::{BrokerAddr, CacheSize, InlineLimit, Slots},
    fs::Fs,
    process::ExitCode,
    root::Root,
    template::TemplateVars,
};
use std::io;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::path::Path;

pub use maelstrom_test_runner::{cli, Logger};

/// The Maelstrom target directory is <target-dir>/maelstrom.
pub struct MaelstromTargetDir;

struct DefaultMainAppDeps {
    client: Client,
}

impl DefaultMainAppDeps {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bg_proc: ClientBgProcess,
        broker_addr: Option<BrokerAddr>,
        project_dir: impl AsRef<Root<ProjectDir>>,
        state_dir: impl AsRef<Root<StateDir>>,
        container_image_depot_dir: impl AsRef<Root<ContainerImageDepotDir>>,
        cache_dir: impl AsRef<Root<CacheDir>>,
        cache_size: CacheSize,
        inline_limit: InlineLimit,
        slots: Slots,
        log: slog::Logger,
    ) -> Result<Self> {
        let project_dir = project_dir.as_ref();
        let state_dir = state_dir.as_ref();
        let container_image_depot_dir = container_image_depot_dir.as_ref();
        let cache_dir = cache_dir.as_ref();
        slog::debug!(
            log, "creating app dependencies";
            "broker_addr" => ?broker_addr,
            "project_dir" => ?project_dir,
            "state_dir" => ?state_dir,
            "container_image_depot_dir" => ?container_image_depot_dir,
            "cache_dir" => ?cache_dir,
            "cache_size" => ?cache_size,
            "inline_limit" => ?inline_limit,
            "slots" => ?slots,
        );
        let client = Client::new(
            bg_proc,
            broker_addr,
            project_dir,
            state_dir,
            container_image_depot_dir,
            cache_dir,
            cache_size,
            inline_limit,
            slots,
            log,
        )?;
        Ok(Self { client })
    }
}

struct CargoOptions {
    feature_selection_options: cargo::FeatureSelectionOptions,
    compilation_options: cargo::CompilationOptions,
    manifest_options: cargo::ManifestOptions,
}

impl MainAppDeps for DefaultMainAppDeps {
    type Client = Client;

    fn client(&self) -> &Client {
        &self.client
    }

    type CargoWaitHandle = cargo::WaitHandle;
    type CargoTestArtifactStream = cargo::TestArtifactStream;
    type CargoOptions = CargoOptions;

    fn run_cargo_test(
        &self,
        color: bool,
        options: &CargoOptions,
        packages: Vec<String>,
    ) -> Result<(cargo::WaitHandle, cargo::TestArtifactStream)> {
        cargo::run_cargo_test(
            color,
            &options.feature_selection_options,
            &options.compilation_options,
            &options.manifest_options,
            packages,
        )
    }

    fn get_cases_from_binary(&self, binary: &Path, filter: &Option<String>) -> Result<Vec<String>> {
        cargo::get_cases_from_binary(binary, filter)
    }

    fn get_template_vars(
        &self,
        cargo_options: &CargoOptions,
        target_dir: &Root<TargetDir>,
    ) -> Result<TemplateVars> {
        let profile = cargo_options
            .compilation_options
            .profile
            .clone()
            .unwrap_or("dev".into());
        let mut target = (**target_dir).to_owned();
        match profile.as_str() {
            "dev" => target.push("debug"),
            other => target.push(other),
        }
        let build_dir = target
            .to_str()
            .ok_or_else(|| anyhow!("{} contains non-UTF8", target.display()))?;
        Ok(TemplateVars::new()
            .with_var("build-dir", build_dir)
            .unwrap())
    }
}

impl Wait for cargo::WaitHandle {
    fn wait(self) -> Result<()> {
        cargo::WaitHandle::wait(self)
    }
}

fn maybe_print_build_error(res: Result<ExitCode>) -> Result<ExitCode> {
    if let Err(e) = &res {
        if let Some(e) = e.downcast_ref::<cargo::CargoBuildError>() {
            eprintln!("{}", &e.stderr);
            return Ok(e.exit_code);
        }
    }
    res
}

fn read_cargo_metadata(config: &config::Config) -> Result<CargoMetadata> {
    let output = std::process::Command::new("cargo")
        .args(["metadata", "--format-version=1"])
        .args(config.cargo_feature_selection_options.iter())
        .args(config.cargo_manifest_options.iter())
        .output()
        .context("getting cargo metadata")?;
    if !output.status.success() {
        bail!(String::from_utf8(output.stderr)
            .context("reading stderr")?
            .trim_end()
            .trim_start_matches("error: ")
            .to_owned());
    }
    let cargo_metadata: CargoMetadata =
        serde_json::from_slice(&output.stdout).context("parsing cargo metadata")?;
    Ok(cargo_metadata)
}

pub fn main<TermT>(
    config: config::Config,
    extra_options: cli::ExtraCommandLineOptions,
    bg_proc: ClientBgProcess,
    logger: Logger,
    stderr_is_tty: bool,
    stdout_is_tty: bool,
    terminal: TermT,
) -> Result<ExitCode>
where
    TermT: TermLike + Clone + Send + Sync + UnwindSafe + RefUnwindSafe + 'static,
{
    let cargo_metadata = read_cargo_metadata(&config)?;
    if extra_options.test_metadata.init {
        alternative_mains::init(&cargo_metadata.workspace_root)
    } else if extra_options.list.packages {
        alternative_mains::list_packages(
            &cargo_metadata.workspace_packages(),
            &extra_options.include,
            &extra_options.exclude,
            &mut io::stdout().lock(),
        )
    } else if extra_options.list.binaries {
        alternative_mains::list_binaries(
            &cargo_metadata.workspace_packages(),
            &extra_options.include,
            &extra_options.exclude,
            &mut io::stdout().lock(),
        )
    } else {
        let workspace_dir = Root::<WorkspaceDir>::new(cargo_metadata.workspace_root.as_std_path());
        let logging_output = LoggingOutput::default();
        let log = logger.build(logging_output.clone());

        let list_action = match (extra_options.list.tests, extra_options.list.binaries) {
            (true, _) => Some(ListAction::ListTests),
            (_, _) => None,
        };

        let target_dir = Root::<TargetDir>::new(cargo_metadata.target_directory.as_std_path());
        let maelstrom_target_dir = target_dir.join::<MaelstromTargetDir>("maelstrom");
        let state_dir = maelstrom_target_dir.join::<StateDir>("state");
        let cache_dir = maelstrom_target_dir.join::<CacheDir>("cache");

        Fs.create_dir_all(&state_dir)?;
        Fs.create_dir_all(&cache_dir)?;

        let deps = DefaultMainAppDeps::new(
            bg_proc,
            config.parent.broker,
            workspace_dir.transmute::<ProjectDir>(),
            &state_dir,
            config.parent.container_image_depot_root,
            cache_dir,
            config.parent.cache_size,
            config.parent.inline_limit,
            config.parent.slots,
            log.clone(),
        )?;

        let cargo_options = CargoOptions {
            feature_selection_options: config.cargo_feature_selection_options,
            compilation_options: config.cargo_compilation_options,
            manifest_options: config.cargo_manifest_options,
        };
        let state = MainAppState::new(
            deps,
            extra_options.include,
            extra_options.exclude,
            list_action,
            stderr_is_tty,
            workspace_dir,
            &cargo_metadata.workspace_packages(),
            &state_dir,
            target_dir,
            cargo_options,
            logging_output,
            log,
        )?;

        let res = std::thread::scope(|scope| {
            let mut app = main_app_new(
                &state,
                stdout_is_tty,
                config.parent.quiet,
                terminal,
                progress::DefaultProgressDriver::new(scope),
                config.parent.timeout.map(Timeout::new),
            )?;
            while !app.enqueue_one()?.is_done() {}
            app.drain()?;
            app.finish()
        });
        drop(state);
        maybe_print_build_error(res)
    }
}
