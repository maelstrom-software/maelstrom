use super::{Ui, UiJobResult, UiJobStatus, UiJobSummary, UiMessage};
use crate::config::Quiet;
use anyhow::Result;
use derive_more::From;
use indicatif::HumanBytes;
use maelstrom_client::RemoteProgress;
use maelstrom_util::ext::OptionExt as _;
use ratatui::{
    backend::{Backend, CrosstermBackend},
    buffer::Buffer,
    crossterm::{
        terminal::{disable_raw_mode, enable_raw_mode, Clear, ClearType},
        ExecutableCommand as _,
    },
    layout::{Constraint, Layout, Rect},
    style::{palette::tailwind, Stylize as _},
    terminal::{Terminal, Viewport},
    text::{Line, Span},
    widgets::{Block, Cell, Gauge, Paragraph, Row, Table, Widget},
    TerminalOptions,
};
use std::collections::BTreeMap;
use std::io::stdout;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};
use unicode_width::UnicodeWidthStr as _;

fn format_finished(res: UiJobResult) -> Vec<PrintAbove> {
    let result_span: Span = match &res.status {
        UiJobStatus::Ok => "OK".green(),
        UiJobStatus::Failure(_) => "FAIL".red(),
        UiJobStatus::TimedOut => "TIMEOUT".red(),
        UiJobStatus::Error(_) => "ERR".red(),
        UiJobStatus::Ignored => "IGNORED".yellow(),
    };

    let case = res.name.bold();
    let mut line = vec![case, result_span];

    if let Some(d) = res.duration {
        line.push(format!("{:.3}s", d.as_secs_f64()).into());
    }

    let mut output = vec![Row::new(line.into_iter().map(Cell::from)).into()];

    if let Some(details) = res.status.details() {
        output.extend(details.split('\n').map(|l| Line::from(l.to_owned()).into()));
    }

    for l in res.stdout {
        output.push(Line::from(l).into());
    }

    for l in res.stderr {
        output.push(
            ["stderr: ".red(), l.into()]
                .into_iter()
                .collect::<Line<'static>>()
                .into(),
        );
    }
    output
}

fn format_running_test(name: &str, time: &Instant) -> Row<'static> {
    let d = time.elapsed();

    Row::new([
        Cell::from(name.to_owned()),
        Cell::from(format!("{:.3}s", d.as_secs_f64())),
    ])
}

#[derive(From)]
enum PrintAbove {
    StatusLine(Row<'static>),
    Output(Line<'static>),
}

impl Widget for PrintAbove {
    fn render(self, area: Rect, buf: &mut Buffer) {
        match self {
            Self::StatusLine(row) => Table::new(
                [row],
                [
                    Constraint::Fill(1),
                    Constraint::Length(7),
                    Constraint::Length(8),
                ],
            )
            .render(area, buf),
            Self::Output(l) => l.render(area, buf),
        }
    }
}

pub struct FancyUi {
    jobs_completed: u64,
    jobs_pending: u64,
    all_done: Option<UiJobSummary>,
    done_building: bool,

    running_tests: BTreeMap<String, Instant>,
    build_output: Vec<Line<'static>>,
    print_above: Vec<PrintAbove>,
    enqueue_status: Option<String>,
    throbber_state: throbber_widgets_tui::ThrobberState,
    remote_progress: Vec<RemoteProgress>,
}

impl FancyUi {
    pub fn new(_list: bool, _stdout_is_tty: bool, _quiet: Quiet) -> Self {
        Self {
            jobs_completed: 0,
            jobs_pending: 0,
            all_done: None,
            done_building: false,

            running_tests: BTreeMap::new(),
            build_output: vec![],
            print_above: vec![],
            enqueue_status: Some("starting...".into()),
            throbber_state: Default::default(),
            remote_progress: vec![],
        }
    }
}

impl Ui for FancyUi {
    fn run(&mut self, recv: Receiver<UiMessage>) -> Result<()> {
        let hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let _ = restore_terminal();
            hook(info)
        }));

        let mut terminal = init_terminal()?;

        let mut last_tick = Instant::now();
        terminal.draw(|f| f.render_widget(&mut *self, f.size()))?;
        loop {
            if last_tick.elapsed() > Duration::from_millis(33) {
                if !self.print_above.is_empty() {
                    let rows = std::mem::take(&mut self.print_above);
                    for t in rows {
                        terminal.insert_before(1, move |buf| t.render(buf.area, buf))?;
                    }
                }
                self.throbber_state.calc_next();
                terminal.draw(|f| f.render_widget(&mut *self, f.size()))?;
                last_tick = Instant::now();
            }

            match recv.recv_timeout(Duration::from_millis(33)) {
                Ok(msg) => match msg {
                    UiMessage::LogMessage(line) => {
                        self.print_above.push(Line::from(line).into());
                    }
                    UiMessage::BuildOutputLine(line) => {
                        self.build_output.push(line.into());
                    }
                    UiMessage::List(_) => {}
                    UiMessage::JobFinished(res) => {
                        self.jobs_completed += 1;
                        self.running_tests.remove(&res.name).assert_is_some();
                        self.print_above.extend(format_finished(res));
                    }
                    UiMessage::UpdatePendingJobsCount(count) => self.jobs_pending = count,
                    UiMessage::JobEnqueued(name) => {
                        self.running_tests
                            .insert(name, Instant::now())
                            .assert_is_none();
                    }
                    UiMessage::UpdateIntrospectState(resp) => {
                        let mut states = resp.artifact_uploads;
                        states.extend(resp.image_downloads);
                        self.remote_progress = states;
                    }
                    UiMessage::UpdateEnqueueStatus(msg) => {
                        self.enqueue_status = Some(msg);
                    }
                    UiMessage::DoneBuilding => {
                        self.done_building = true;
                    }
                    UiMessage::DoneQueuingJobs => {
                        self.enqueue_status = None;
                    }
                    UiMessage::AllJobsFinished(summary) => {
                        self.all_done = Some(summary);
                    }
                    UiMessage::Shutdown => break,
                },
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        terminal.draw(|f| f.render_widget(&mut *self, f.size()))?;

        Ok(())
    }
}

impl Drop for FancyUi {
    fn drop(&mut self) {
        let _ = restore_terminal();
    }
}

impl FancyUi {
    fn render_running_tests(&mut self, area: Rect, buf: &mut Buffer) {
        let create_block = |title: String| Block::bordered().gray().title(title.bold());

        let omitted_tests = self
            .running_tests
            .len()
            .saturating_sub((area.height as usize).saturating_sub(2));
        let omitted_trailer = (omitted_tests > 0)
            .then(|| format!(" ({omitted_tests} tests not shown)"))
            .unwrap_or_default();
        let mut running_tests: Vec<_> = self.running_tests.iter().collect();
        running_tests.sort_by_key(|a| a.1);
        Table::new(
            running_tests
                .into_iter()
                .rev()
                .skip(omitted_tests)
                .map(|(name, t)| format_running_test(name.as_str(), t)),
            [Constraint::Fill(1), Constraint::Length(8)],
        )
        .block(create_block(format!("Running Tests{}", omitted_trailer)))
        .gray()
        .render(area, buf);
    }

    fn render_build_output(&mut self, area: Rect, buf: &mut Buffer) {
        let create_block = |title: &'static str| Block::bordered().gray().title(title.bold());

        let vertical_scroll = (self.build_output.len() as u16).saturating_sub(area.height);
        Paragraph::new(self.build_output.clone())
            .block(create_block("Build Output"))
            .gray()
            .scroll((vertical_scroll, 0))
            .render(area, buf);
    }

    fn render_gauge(&mut self, area: Rect, buf: &mut Buffer) {
        let mut prcnt = self.jobs_completed as f64 / self.jobs_pending as f64;
        if prcnt.is_nan() {
            prcnt = 0.0;
        }
        let label = format!(
            "{}/{} tests completed",
            self.jobs_completed, self.jobs_pending
        );
        Gauge::default()
            .gauge_style(tailwind::BLUE.c800)
            .ratio(prcnt)
            .label(label)
            .use_unicode(true)
            .render(area, buf);
    }

    fn render_summary(&mut self, area: Rect, buf: &mut Buffer) {
        let layout = Layout::vertical([Constraint::Length(1), Constraint::Fill(1)]);
        let [title_area, table_area] = layout.areas(area);
        Paragraph::new(Line::from("Test Summary").centered()).render(title_area, buf);

        let summary = self.all_done.as_ref().unwrap();
        let num_failed = summary.failed.len();
        let num_ignored = summary.ignored.len();
        let num_succeeded = summary.succeeded;

        let summary_line = |msg, cnt| {
            (
                Constraint::Length(1),
                Table::new(
                    [Row::new([Cell::from(msg), Cell::from(format!("{cnt}"))])],
                    [Constraint::Fill(1), Constraint::Length(9)],
                ),
            )
        };

        let list_tests = |tests: &Vec<String>, status: Span<'static>| {
            let longest = tests.iter().map(|t| t.width()).max().unwrap_or(0);
            (
                Constraint::Length(tests.len().try_into().unwrap_or(u16::MAX)),
                Table::new(
                    tests.iter().map(|t| {
                        Row::new([
                            Cell::from(""),
                            Cell::from(format!("{t}:")),
                            Cell::from(status.clone()),
                        ])
                    }),
                    [
                        Constraint::Length(4),
                        Constraint::Length(longest as u16 + 1),
                        Constraint::Length(7),
                    ],
                ),
            )
        };

        let mut sections = vec![
            summary_line("Successful Tests".green(), num_succeeded),
            summary_line("Failed Tests".red(), num_failed),
            list_tests(&summary.failed, "failure".red()),
        ];
        if num_ignored > 0 {
            sections.push(summary_line("Ignored Tests".yellow(), num_ignored));
            sections.push(list_tests(&summary.ignored, "ignored".yellow()));
        }

        let layout = Layout::vertical(sections.iter().map(|(c, _)| *c));
        let areas = layout.split(table_area);
        let sections = sections
            .into_iter()
            .zip(areas.iter())
            .map(|((_, t), a)| (*a, t));
        for (rect, t) in sections {
            t.render(rect, buf);
        }
    }

    fn render_enqueue_status(&mut self, area: Rect, buf: &mut Buffer) {
        use ratatui::widgets::StatefulWidget;

        let status = self.enqueue_status.as_ref().unwrap();
        let t = throbber_widgets_tui::Throbber::default()
            .label(status.clone())
            .throbber_set(throbber_widgets_tui::BRAILLE_SIX_DOUBLE)
            .use_type(throbber_widgets_tui::WhichUse::Spin);
        StatefulWidget::render(t, area, buf, &mut self.throbber_state);
    }

    fn render_remote_progress(&mut self, area: Rect, buf: &mut Buffer) {
        let gauge_f = |name, size, progress| {
            let mut prcnt = progress as f64 / size as f64;
            if prcnt.is_nan() {
                prcnt = 0.0;
            }
            let progress = HumanBytes(progress);
            let size = HumanBytes(size);
            Gauge::default()
                .gauge_style(tailwind::ORANGE.c800)
                .ratio(prcnt)
                .label(format!("{progress}/{size} {name}"))
                .use_unicode(true)
        };
        let len = self.remote_progress.len();
        let layout = Layout::vertical(std::iter::repeat(Constraint::Length(1)).take(len));
        for (p, area) in self.remote_progress.iter().zip(layout.split(area).iter()) {
            gauge_f(&p.name, p.size, p.progress).render(*area, buf);
        }
    }

    fn render_sections(&mut self, buf: &mut Buffer, sections: Vec<(Rect, SectionFnPtr)>) {
        for (rect, f) in sections {
            f(self, rect, buf)
        }
    }
}

type SectionFnPtr = fn(&mut FancyUi, Rect, &mut Buffer);

impl Widget for &mut FancyUi {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let mut sections = vec![];

        if self.all_done.is_some() {
            sections.push((Constraint::Fill(1), FancyUi::render_summary as _));
        } else {
            if !self.running_tests.is_empty() {
                let max_height = (self.running_tests.len() + 2)
                    .try_into()
                    .unwrap_or(u16::MAX);
                sections.push((
                    Constraint::Max(max_height),
                    FancyUi::render_running_tests as SectionFnPtr,
                ));
            }
            if !self.remote_progress.is_empty() {
                let max_height = self.remote_progress.len().try_into().unwrap_or(u16::MAX);
                sections.push((
                    Constraint::Max(max_height),
                    FancyUi::render_remote_progress as _,
                ));
            }
            if !self.done_building {
                sections.push((Constraint::Length(4), FancyUi::render_build_output as _));
            }
            if self.enqueue_status.is_some() {
                sections.push((Constraint::Length(1), FancyUi::render_enqueue_status as _));
            }
            sections.push((Constraint::Length(1), FancyUi::render_gauge as _));
        }

        let layout = Layout::vertical(sections.iter().map(|(c, _)| *c));
        let sections = sections
            .into_iter()
            .zip(layout.split(area).iter())
            .map(|((_, f), a)| (*a, f))
            .collect();

        self.render_sections(buf, sections);
    }
}

fn init_terminal() -> Result<Terminal<impl Backend>> {
    enable_raw_mode()?;
    let backend = CrosstermBackend::new(stdout());
    let height = backend.size()?.height;
    let terminal = Terminal::with_options(
        backend,
        TerminalOptions {
            viewport: Viewport::Inline(height / 4),
        },
    )?;
    Ok(terminal)
}

fn restore_terminal() -> Result<()> {
    disable_raw_mode()?;
    stdout().execute(Clear(ClearType::FromCursorDown))?;
    println!();
    Ok(())
}
