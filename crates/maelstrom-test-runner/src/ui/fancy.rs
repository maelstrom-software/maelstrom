use super::{Ui, UiMessage};
use crate::config::Quiet;
use anyhow::Result;
use ratatui::{
    backend::{Backend, CrosstermBackend},
    buffer::Buffer,
    crossterm::{
        terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
        ExecutableCommand,
    },
    layout::{Alignment, Rect},
    style::{palette::tailwind, Stylize as _},
    terminal::Terminal,
    widgets::{block::Title, Block, Borders, Gauge, Padding, Widget},
};
use std::io::stdout;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

pub struct FancyUi {
    jobs_completed: u64,
    jobs_pending: u64,
}

impl FancyUi {
    pub fn new(_list: bool, _stdout_is_tty: bool, _quiet: Quiet) -> Self {
        Self {
            jobs_completed: 0,
            jobs_pending: 0,
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
        loop {
            if last_tick.elapsed() > Duration::from_millis(500) {
                terminal.draw(|f| f.render_widget(&*self, f.size()))?;
                last_tick = Instant::now();
            }

            match recv.recv_timeout(Duration::from_millis(500)) {
                Ok(msg) => match msg {
                    UiMessage::LogMessage(_) => {}
                    UiMessage::List(_) => {}
                    UiMessage::JobFinished(_) => self.jobs_completed += 1,
                    UiMessage::UpdatePendingJobsCount(count) => self.jobs_pending = count,
                    UiMessage::UpdateIntrospectState(_resp) => {}
                    UiMessage::UpdateEnqueueStatus(_msg) => {}
                    UiMessage::DoneQueuingJobs => {}
                    UiMessage::AllJobsFinished(_summary) => {}
                },
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
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
    fn render_gauge(&self, area: Rect, buf: &mut Buffer) {
        let title = title_block("Jobs Completed");
        let mut prcnt = self.jobs_completed as f64 / self.jobs_pending as f64;
        if prcnt.is_nan() {
            prcnt = 0.0;
        }
        let label = format!("{:.1}%", prcnt * 100.0);
        Gauge::default()
            .block(title)
            .gauge_style(tailwind::ORANGE.c800)
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

impl Widget for &FancyUi {
    fn render(self, area: Rect, buf: &mut Buffer) {
        self.render_gauge(area, buf);
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
