use super::{Ui, UiJobResult, UiJobStatus, UiJobSummary, UiMessage};
use crate::config::Quiet;
use anyhow::Result;
use derive_more::From;
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

fn format_finished(res: UiJobResult) -> Vec<CompletedTestOutput> {
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
        output.push(Line::from(details).into());
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
enum CompletedTestOutput {
    StatusLine(Row<'static>),
    Output(Line<'static>),
}

impl Widget for CompletedTestOutput {
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
    completed_tests: Vec<CompletedTestOutput>,
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
            completed_tests: vec![],
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
                if !self.completed_tests.is_empty() {
                    let rows = std::mem::take(&mut self.completed_tests);
                    for t in rows {
                        terminal.insert_before(1, move |buf| t.render(buf.area, buf))?;
                    }
                }
                terminal.draw(|f| f.render_widget(&mut *self, f.size()))?;
                last_tick = Instant::now();
            }

            match recv.recv_timeout(Duration::from_millis(33)) {
                Ok(msg) => match msg {
                    UiMessage::LogMessage(_) => {}
                    UiMessage::BuildOutputLine(line) => {
                        self.build_output.push(line.into());
                    }
                    UiMessage::List(_) => {}
                    UiMessage::JobFinished(res) => {
                        self.jobs_completed += 1;
                        self.running_tests.remove(&res.name).assert_is_some();
                        self.completed_tests.extend(format_finished(res));
                    }
                    UiMessage::UpdatePendingJobsCount(count) => self.jobs_pending = count,
                    UiMessage::JobEnqueued(name) => {
                        self.running_tests
                            .insert(name, Instant::now())
                            .assert_is_none();
                    }
                    UiMessage::UpdateIntrospectState(_resp) => {}
                    UiMessage::UpdateEnqueueStatus(_msg) => {}
                    UiMessage::DoneBuilding => {
                        self.done_building = true;
                    }
                    UiMessage::DoneQueuingJobs => {}
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
        let create_block = |title: &'static str| Block::bordered().gray().title(title.bold());

        let mut running_tests: Vec<_> = self.running_tests.iter().collect();
        running_tests.sort_by_key(|a| a.1);
        Table::new(
            running_tests
                .into_iter()
                .map(|(name, t)| format_running_test(name.as_str(), t)),
            [Constraint::Fill(1), Constraint::Length(8)],
        )
        .block(create_block("Running Tests"))
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
        let [title_area, rest] = layout.areas(area);
        Paragraph::new(Line::from("Test Summary").centered()).render(title_area, buf);

        let summary = self.all_done.as_ref().unwrap();
        let num_failed = summary.failed.len();
        let num_ignored = summary.ignored.len();
        let num_succeeded = summary.succeeded;
        let mut rows = vec![
            Row::new([
                Cell::from("Successful Tests".green()),
                Cell::from(format!("{num_succeeded}")),
            ]),
            Row::new([
                Cell::from("Failed Tests".red()),
                Cell::from(format!("{num_failed}")),
            ]),
        ];
        if num_ignored > 0 {
            rows.push(Row::new([
                Cell::from("Ignored Tests".yellow()),
                Cell::from(format!("{num_ignored}")),
            ]));
        }
        Table::new(rows, [Constraint::Fill(1), Constraint::Length(4)]).render(rest, buf)
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
                let height = std::cmp::min(self.running_tests.len(), 20);
                sections.push((
                    Constraint::Length(height as u16 + 2),
                    FancyUi::render_running_tests as SectionFnPtr,
                ));
            }
            if !self.done_building {
                sections.push((Constraint::Length(4), FancyUi::render_build_output as _));
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
    let terminal = Terminal::with_options(
        backend,
        TerminalOptions {
            viewport: Viewport::Inline(18),
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
