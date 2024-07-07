use super::{Ui, UiMessage};
use crate::config::Quiet;
use anyhow::Result;
use maelstrom_util::ext::BoolExt as _;
use ratatui::{
    backend::{Backend, CrosstermBackend},
    buffer::Buffer,
    crossterm::{
        terminal::{disable_raw_mode, enable_raw_mode, Clear, ClearType},
        ExecutableCommand as _,
    },
    layout::{Alignment, Constraint, Layout, Rect},
    style::{palette::tailwind, Stylize as _},
    terminal::{Terminal, Viewport},
    text::Line,
    widgets::{block::Title, Block, Borders, Gauge, Padding, Paragraph, Widget},
    TerminalOptions,
};
use std::collections::BTreeSet;
use std::io::stdout;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

pub struct FancyUi {
    jobs_completed: u64,
    jobs_pending: u64,
    all_done: bool,
    done_building: bool,

    running_tests: BTreeSet<String>,
    build_output: Vec<Line<'static>>,
    completed_tests: Vec<String>,
}

impl FancyUi {
    pub fn new(_list: bool, _stdout_is_tty: bool, _quiet: Quiet) -> Self {
        Self {
            jobs_completed: 0,
            jobs_pending: 0,
            all_done: false,
            done_building: false,

            running_tests: BTreeSet::new(),
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
                    let t = std::mem::take(&mut self.completed_tests);
                    terminal.insert_before(t.len() as u16, move |buf| {
                        Paragraph::new(t.into_iter().map(Line::from).collect::<Vec<_>>())
                            .render(buf.area, buf)
                    })?;
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
                        self.running_tests.remove(&res.name).assert_is_true();
                        self.completed_tests.push(res.name);
                    }
                    UiMessage::UpdatePendingJobsCount(count) => self.jobs_pending = count,
                    UiMessage::JobEnqueued(name) => {
                        self.running_tests.insert(name).assert_is_true();
                    }
                    UiMessage::UpdateIntrospectState(_resp) => {}
                    UiMessage::UpdateEnqueueStatus(_msg) => {}
                    UiMessage::DoneBuilding => {
                        self.done_building = true;
                    }
                    UiMessage::DoneQueuingJobs => {}
                    UiMessage::AllJobsFinished(_summary) => {
                        self.all_done = true;
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

        let vertical_scroll = (self.running_tests.len() as u16).saturating_sub(area.height);
        Paragraph::new(
            self.running_tests
                .iter()
                .map(|s| Line::from(s.as_str()))
                .collect::<Vec<_>>(),
        )
        .block(create_block("Running Tests"))
        .gray()
        .scroll((vertical_scroll, 0))
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
        let title = title_block("Jobs Completed");
        let mut prcnt = self.jobs_completed as f64 / self.jobs_pending as f64;
        if prcnt.is_nan() {
            prcnt = 0.0;
        }
        let label = format!("{}/{}", self.jobs_completed, self.jobs_pending);
        let color = if self.all_done {
            tailwind::GREEN.c800
        } else {
            tailwind::ORANGE.c800
        };
        Gauge::default()
            .block(title)
            .gauge_style(color)
            .ratio(prcnt)
            .label(label)
            .use_unicode(true)
            .render(area, buf);
    }

    fn render_sections(&mut self, buf: &mut Buffer, sections: Vec<(Rect, SectionFnPtr)>) {
        for (rect, f) in sections {
            f(self, rect, buf)
        }
    }
}

type SectionFnPtr = fn(&mut FancyUi, Rect, &mut Buffer);

fn title_block(title: &str) -> Block {
    let title = Title::from(title).alignment(Alignment::Center);
    Block::new()
        .borders(Borders::NONE)
        .padding(Padding::vertical(1))
        .title(title)
        .fg(tailwind::SLATE.c200)
}

impl Widget for &mut FancyUi {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let mut sections = vec![];
        if !self.running_tests.is_empty() {
            sections.push((
                Constraint::Length(10),
                FancyUi::render_running_tests as SectionFnPtr,
            ));
        }
        if !self.done_building {
            sections.push((Constraint::Length(4), FancyUi::render_build_output as _));
        }
        sections.push((Constraint::Length(4), FancyUi::render_gauge as _));

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
    Ok(())
}
