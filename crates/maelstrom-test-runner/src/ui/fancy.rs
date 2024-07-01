use super::{Ui, UiMessage};
use crate::config::Quiet;
use anyhow::Result;
use crossterm::event::{Event, KeyCode};
use ratatui::{
    backend::{Backend, CrosstermBackend},
    buffer::Buffer,
    crossterm::{
        terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
        ExecutableCommand,
    },
    layout::{Alignment, Constraint, Layout, Rect},
    style::{palette::tailwind, Stylize as _},
    terminal::Terminal,
    text::Line,
    widgets::{block::Title, Block, Borders, Gauge, Padding, Paragraph, Widget},
};
use std::io::stdout;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

pub struct FancyUi {
    jobs_completed: u64,
    jobs_pending: u64,
    all_done: bool,

    completed_tests: Vec<Line<'static>>,
    build_output: Vec<Line<'static>>,
}

impl FancyUi {
    pub fn new(_list: bool, _stdout_is_tty: bool, _quiet: Quiet) -> Self {
        Self {
            jobs_completed: 0,
            jobs_pending: 0,
            all_done: false,

            completed_tests: vec![],
            build_output: vec![],
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
                        self.completed_tests.push(res.name.into());
                    }
                    UiMessage::UpdatePendingJobsCount(count) => self.jobs_pending = count,
                    UiMessage::UpdateIntrospectState(_resp) => {}
                    UiMessage::UpdateEnqueueStatus(_msg) => {}
                    UiMessage::DoneQueuingJobs => {}
                    UiMessage::AllJobsFinished(_summary) => {
                        self.all_done = true;
                    }
                },
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }

        loop {
            terminal.draw(|f| f.render_widget(&mut *self, f.size()))?;
            if crossterm::event::poll(Duration::from_millis(33))? {
                if let Event::Key(key) = crossterm::event::read()? {
                    if key.code == KeyCode::Char('q') {
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}

impl Drop for FancyUi {
    fn drop(&mut self) {
        let _ = restore_terminal();
    }
}

impl FancyUi {
    fn render_completed_tests(&mut self, area: Rect, buf: &mut Buffer) {
        let create_block = |title: &'static str| Block::bordered().gray().title(title.bold());

        let vertical_scroll = (self.completed_tests.len() as u16).saturating_sub(area.height);
        Paragraph::new(self.completed_tests.clone())
            .block(create_block("Completed Tests"))
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
}

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
        let layout = Layout::vertical([
            Constraint::Ratio(1, 2),
            Constraint::Ratio(3, 8),
            Constraint::Ratio(1, 8),
        ]);
        let [tests_area, build_area, gauge_area] = layout.areas(area);
        self.render_completed_tests(tests_area, buf);
        self.render_build_output(build_area, buf);
        self.render_gauge(gauge_area, buf);
    }
}

fn init_terminal() -> Result<Terminal<impl Backend>> {
    enable_raw_mode()?;
    stdout().execute(EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout());
    let terminal = Terminal::new(backend)?;
    Ok(terminal)
}

fn restore_terminal() -> Result<()> {
    disable_raw_mode()?;
    stdout().execute(LeaveAlternateScreen)?;
    Ok(())
}
