mod multi_gauge;

use super::{CompletedJob, JobStatuses, Ui, UiJobResult, UiJobStatus, UiJobSummary, UiMessage};
use anyhow::{bail, Result};
use derive_more::From;
use indicatif::HumanBytes;
use maelstrom_client::RemoteProgress;
use maelstrom_linux as linux;
use multi_gauge::{InnerGauge, MultiGauge};
use ratatui::{
    backend::{Backend, CrosstermBackend},
    buffer::Buffer,
    crossterm::{
        cursor,
        event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
        terminal::{disable_raw_mode, enable_raw_mode, Clear, ClearType},
        ExecutableCommand as _,
    },
    layout::{Alignment, Constraint, Layout, Rect},
    style::{palette::tailwind, Color, Modifier, Style, Stylize as _},
    text::{Line, Span, Text},
    widgets::{Block, Cell, Gauge, Paragraph, Row, Table, Widget, Wrap},
    Terminal, TerminalOptions, Viewport,
};
use slog::Drain as _;
use std::cell::RefCell;
use std::io::{self, stdout};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};
use unicode_width::UnicodeWidthStr as _;

enum SlogEntry {
    Style(Style),
    EndStyle,
    String(String),
}

struct SlogLine {
    entries: Vec<SlogEntry>,
}

impl SlogLine {
    fn empty() -> Self {
        Self { entries: vec![] }
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl From<SlogLine> for Line<'static> {
    fn from(s: SlogLine) -> Self {
        let mut l = Line::default();
        let mut span = None;
        for entry in s.entries {
            match entry {
                SlogEntry::Style(style) => {
                    if let Some(span) = span.take() {
                        l.spans.push(span);
                    }
                    span = Some(Span::default().style(style));
                }
                SlogEntry::EndStyle => {
                    if let Some(span) = span.take() {
                        l.spans.push(span);
                    }
                }
                SlogEntry::String(s) => {
                    if let Some(span) = &mut span {
                        span.content = format!("{}{}", span.content, s).into();
                    } else {
                        span = Some(Span::from(s));
                    }
                }
            }
        }
        l
    }
}

#[derive(Default)]
struct SlogLines {
    lines: Vec<SlogLine>,
}

impl SlogLines {
    fn pop_lines(&mut self) -> Vec<Line<'static>> {
        std::mem::take(&mut self.lines)
            .into_iter()
            .filter(|l| !l.is_empty())
            .map(|l| l.into())
            .collect()
    }

    fn write_to_line(&mut self, s: &str) {
        self.push_entry(SlogEntry::String(s.into()));
    }

    fn push_entry(&mut self, entry: SlogEntry) {
        if self.lines.is_empty() {
            self.next();
        }
        let line = self.lines.last_mut().unwrap();
        line.entries.push(entry);
    }

    fn next(&mut self) {
        self.lines.push(SlogLine::empty());
    }
}

struct UiSlogRecordDecorator<'lines> {
    level: slog::Level,
    lines: &'lines mut SlogLines,
}

impl<'lines> UiSlogRecordDecorator<'lines> {
    pub fn new(level: slog::Level, lines: &'lines mut SlogLines) -> Self {
        Self { level, lines }
    }
}

impl<'lines> io::Write for UiSlogRecordDecorator<'lines> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut buf = buf.to_owned();
        let total_len = buf.len();

        while let Some(p) = buf.iter().position(|&b| b == b'\n') {
            let remaining = buf.split_off(p + 1);
            self.lines
                .write_to_line(&String::from_utf8_lossy(&buf[..(buf.len() - 1)]));
            self.lines.next();

            buf = remaining;
        }
        if !buf.is_empty() {
            self.lines.write_to_line(&String::from_utf8_lossy(&buf));
        }

        Ok(total_len)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'lines> slog_term::RecordDecorator for UiSlogRecordDecorator<'lines> {
    fn reset(&mut self) -> io::Result<()> {
        self.lines.push_entry(SlogEntry::EndStyle);
        Ok(())
    }

    fn start_level(&mut self) -> io::Result<()> {
        let color = match self.level {
            slog::Level::Critical => Color::Magenta,
            slog::Level::Error => Color::Red,
            slog::Level::Warning => Color::Yellow,
            slog::Level::Info => Color::Green,
            slog::Level::Debug => Color::Cyan,
            slog::Level::Trace => Color::Blue,
        };

        self.lines
            .push_entry(SlogEntry::Style(Style::default().fg(color)));
        Ok(())
    }

    fn start_key(&mut self) -> io::Result<()> {
        self.lines.push_entry(SlogEntry::Style(
            Style::default().add_modifier(Modifier::BOLD),
        ));
        Ok(())
    }

    fn start_msg(&mut self) -> io::Result<()> {
        self.lines.push_entry(SlogEntry::Style(
            Style::default().add_modifier(Modifier::BOLD),
        ));
        Ok(())
    }
}

struct UiSlogDecorator<'lines> {
    lines: &'lines RefCell<SlogLines>,
}

impl<'lines> UiSlogDecorator<'lines> {
    fn new(lines: &'lines RefCell<SlogLines>) -> Self {
        Self { lines }
    }
}

impl<'lines> slog_term::Decorator for UiSlogDecorator<'lines> {
    fn with_record<F>(
        &self,
        record: &slog::Record,
        _logger_values: &slog::OwnedKVList,
        f: F,
    ) -> io::Result<()>
    where
        F: FnOnce(&mut dyn slog_term::RecordDecorator) -> io::Result<()>,
    {
        let mut lines = self.lines.borrow_mut();
        let mut d = UiSlogRecordDecorator::new(record.level(), &mut lines);
        f(&mut d)
    }
}

fn format_finished(res: &UiJobResult) -> Vec<PrintAbove> {
    let result_span: Span = match &res.status {
        UiJobStatus::Ok => "OK".green(),
        UiJobStatus::Failure(_) => "FAIL".red(),
        UiJobStatus::TimedOut => "TIMEOUT".red(),
        UiJobStatus::Error(_) => "ERR".red(),
        UiJobStatus::Ignored => "IGNORED".yellow(),
    };

    let case = res.name.clone().bold();
    let mut line = vec![Cell::from(case), Cell::from(result_span)];

    if let Some(d) = res.duration {
        line.push(Cell::from(
            Text::from(format!("{:.3}s", d.as_secs_f64())).alignment(Alignment::Right),
        ));
    }

    let mut output = vec![(Row::new(line.into_iter()), test_status_constraints()).into()];

    if let Some(details) = res.status.details() {
        output.extend(details.split('\n').map(|l| Line::from(l.to_owned()).into()));
    }

    for l in &res.stdout {
        output.push(Line::from(l.clone()).into());
    }

    for l in &res.stderr {
        output.push(
            ["stderr: ".red(), l.to_owned().into()]
                .into_iter()
                .collect::<Line<'static>>()
                .into(),
        );
    }
    output
}

fn format_running_test(name: &str, time: &Instant) -> Row<'static> {
    let d = time.elapsed();

    let duration = if d < Duration::from_secs(1) {
        "<1s".into()
    } else {
        format!("{}s", d.as_secs_f64().round() as usize)
    };

    Row::new([Cell::from(name.to_owned()), Cell::from(duration)])
}

fn format_failed_test(t: &CompletedJob) -> Row<'static> {
    let status = match t.status {
        UiJobStatus::Ok | UiJobStatus::Ignored => unreachable!(),
        UiJobStatus::Failure(_) => "failed".red(),
        UiJobStatus::Error(_) => "error".red(),
        UiJobStatus::TimedOut => "timed out".red(),
    };
    Row::new([Cell::from(t.name.to_owned()), Cell::from(status)])
}

#[derive(From)]
enum PrintAbove {
    #[from]
    StatusLine(Row<'static>, Vec<Constraint>),
    Output(Paragraph<'static>),
}

impl From<Line<'static>> for PrintAbove {
    fn from(l: Line<'static>) -> Self {
        Self::Output(Paragraph::new(l).wrap(Wrap { trim: true }))
    }
}

impl PrintAbove {
    fn height(&self, width: u16) -> u16 {
        match self {
            Self::StatusLine(_, _) => 1,
            Self::Output(l) => l.line_count(width).try_into().unwrap_or(u16::MAX),
        }
    }
}

fn test_status_constraints() -> Vec<Constraint> {
    vec![
        Constraint::Fill(1),
        Constraint::Length(7),
        Constraint::Length(8),
    ]
}

impl Widget for PrintAbove {
    fn render(self, area: Rect, buf: &mut Buffer) {
        match self {
            Self::StatusLine(row, constraints) => Table::new([row], constraints).render(area, buf),
            Self::Output(p) => p.render(area, buf),
        }
    }
}

pub struct FancyUi {
    jobs: JobStatuses,
    expected_total_jobs: u64,
    all_done: Option<UiJobSummary>,
    producing_build_output: bool,

    build_output: vt100::Parser,
    print_above: Vec<PrintAbove>,
    enqueue_status: Option<String>,
    throbber_state: throbber_widgets_tui::ThrobberState,
    remote_progress: Vec<RemoteProgress>,
}

impl FancyUi {
    pub fn new(list: bool, stdout_is_tty: bool) -> Result<Self> {
        if list {
            bail!("`--ui fancy` doesn't support listing");
        }
        if !stdout_is_tty {
            bail!("stdout must be a TTY to use `--ui fancy`")
        }

        Ok(Self {
            jobs: JobStatuses::default(),
            expected_total_jobs: 0,
            all_done: None,
            producing_build_output: false,

            build_output: vt100::Parser::new(3, u16::MAX, 0),
            print_above: vec![],
            enqueue_status: Some("starting...".into()),
            throbber_state: Default::default(),
            remote_progress: vec![],
        })
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

        let log_lines = RefCell::new(SlogLines::default());
        let slog_dec = UiSlogDecorator::new(&log_lines);
        let slog_drain = slog_term::FullFormat::new(slog_dec).build().fuse();

        let mut last_tick = Instant::now();
        terminal.draw(|f| f.render_widget(&mut *self, f.area()))?;
        loop {
            if last_tick.elapsed() > Duration::from_millis(33) {
                while crossterm::event::poll(Duration::from_secs(0))? {
                    if let crossterm::event::Event::Key(key) = crossterm::event::read()? {
                        self.handle_key(key);
                    }
                }

                if !self.print_above.is_empty() {
                    let rows = std::mem::take(&mut self.print_above);
                    let term_width = terminal.backend().size()?.width;
                    for t in rows {
                        terminal.insert_before(t.height(term_width), move |buf| {
                            t.render(buf.area, buf)
                        })?;
                    }
                }
                self.throbber_state.calc_next();
                terminal.draw(|f| f.render_widget(&mut *self, f.area()))?;
                last_tick = Instant::now();
            }

            match recv.recv_timeout(Duration::from_millis(33)) {
                Ok(msg) => match msg {
                    UiMessage::SlogRecord(r) => {
                        let _ = r.log_to(&slog_drain);
                        self.print_above.extend(
                            log_lines
                                .borrow_mut()
                                .pop_lines()
                                .into_iter()
                                .map(|l| l.into()),
                        );
                    }
                    UiMessage::BuildOutputLine(line) => {
                        self.build_output.process(line.as_bytes());
                        self.build_output.process(b"\r\n");
                        self.producing_build_output = true;
                    }
                    UiMessage::BuildOutputChunk(chunk) => {
                        self.build_output.process(&chunk);
                        self.producing_build_output = true;
                    }
                    UiMessage::List(_) => {}
                    UiMessage::JobUpdated(msg) => self.jobs.job_updated(msg.job_id, msg.status),
                    UiMessage::JobFinished(res) => {
                        self.print_above.extend(format_finished(&res));
                        self.jobs.job_finished(res);
                    }
                    UiMessage::UpdatePendingJobsCount(count) => {
                        self.expected_total_jobs = count;
                    }
                    UiMessage::JobEnqueued(msg) => {
                        self.jobs.job_enqueued(msg.job_id, msg.name);
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
                        self.producing_build_output = false;
                    }
                    UiMessage::DoneQueuingJobs => {
                        self.enqueue_status = None;
                    }
                    UiMessage::AllJobsFinished(summary) => {
                        self.all_done = Some(summary);
                    }
                },
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }

        // Spit out the summary
        if self.all_done.is_some() {
            self.render_summary();
        }

        drop(slog_drain);
        if !self.print_above.is_empty() {
            let rows = std::mem::take(&mut self.print_above);
            let term_width = terminal.backend().size()?.width;
            for t in rows {
                terminal.insert_before(t.height(term_width), move |buf| t.render(buf.area, buf))?;
            }
        }

        // Clear away some of the temporal UI elements before we update the screen one last time.
        self.producing_build_output = false;
        self.enqueue_status = None;
        self.remote_progress.clear();
        self.jobs = Default::default();

        terminal.draw(|f| f.render_widget(&mut *self, f.area()))?;

        Ok(())
    }
}

impl Drop for FancyUi {
    fn drop(&mut self) {
        let _ = restore_terminal();
    }
}

impl FancyUi {
    fn handle_key(&mut self, key: KeyEvent) {
        if key.kind != KeyEventKind::Press {
            return;
        }

        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            let _ = restore_terminal();
            linux::raise(linux::Signal::INT).unwrap();
            unreachable!();
        }
    }

    fn render_running_tests(&mut self, area: Rect, buf: &mut Buffer) {
        let create_block = |title: String| Block::bordered().gray().title(title.bold());

        let omitted_tests = self
            .jobs
            .running()
            .saturating_sub((area.height as u64).saturating_sub(2));
        let omitted_trailer = (omitted_tests > 0)
            .then(|| format!(" ({omitted_tests} tests not shown)"))
            .unwrap_or_default();
        let mut running_tests: Vec<_> = self.jobs.running_tests().collect();
        running_tests.sort_by_key(|a| a.1);
        Table::new(
            running_tests
                .into_iter()
                .rev()
                .skip(omitted_tests as usize)
                .map(|(name, t)| format_running_test(name, t)),
            [Constraint::Fill(1), Constraint::Length(4)],
        )
        .block(create_block(format!("Running Tests{}", omitted_trailer)))
        .gray()
        .render(area, buf);
    }

    fn render_failed_tests(&mut self, area: Rect, buf: &mut Buffer) {
        let create_block = |title: String| Block::bordered().gray().title(title.bold());

        let omitted_tests = self
            .jobs
            .failed()
            .saturating_sub((area.height as u64).saturating_sub(2));
        let omitted_trailer = (omitted_tests > 0)
            .then(|| format!(" ({omitted_tests} tests not shown)"))
            .unwrap_or_default();
        let mut failed_tests: Vec<_> = self.jobs.failed_tests().collect();
        failed_tests.sort_by_key(|j| &j.name);
        Table::new(
            failed_tests
                .into_iter()
                .rev()
                .skip(omitted_tests as usize)
                .map(format_failed_test),
            [Constraint::Fill(1), Constraint::Length(4)],
        )
        .block(create_block(format!("Failed Tests{}", omitted_trailer)))
        .gray()
        .render(area, buf);
    }

    fn render_build_output(&mut self, area: Rect, buf: &mut Buffer) {
        let create_block = |title: &'static str| Block::bordered().gray().title(title.bold());
        tui_term::widget::PseudoTerminal::new(self.build_output.screen())
            .block(create_block("Build Output"))
            .render(area, buf);
    }

    fn render_gauge(&mut self, area: Rect, buf: &mut Buffer) {
        let build_gauge = |color, mut n, d| {
            n = std::cmp::min(n, d);
            let mut prcnt = n as f64 / d as f64;
            if prcnt.is_nan() {
                prcnt = 0.0;
            }
            InnerGauge::default().gauge_style(color).ratio(prcnt)
        };

        let d = self.expected_total_jobs;

        let num_failed = self.jobs.failed();
        let failure_trailer = (num_failed > 0)
            .then(|| format!(" ({num_failed}f)"))
            .unwrap_or_default();

        MultiGauge::default()
            .gauge(build_gauge(tailwind::GREEN.c800, self.jobs.completed(), d))
            .gauge(build_gauge(tailwind::BLUE.c800, self.jobs.running(), d))
            .gauge(build_gauge(tailwind::YELLOW.c800, self.jobs.pending(), d))
            .gauge(build_gauge(
                tailwind::PURPLE.c800,
                self.jobs.waiting_for_artifacts(),
                d,
            ))
            .label(format!(
                "{}w {}p {}r {}c{} / {d}e",
                self.jobs.waiting_for_artifacts(),
                self.jobs.pending(),
                self.jobs.running(),
                self.jobs.completed(),
                failure_trailer
            ))
            .render(area, buf);
    }

    fn render_summary(&mut self) {
        self.print_above
            .push(Line::from("Test Summary").centered().into());

        let summary = self.all_done.as_ref().unwrap();
        let num_failed = summary.failed.len();
        let num_ignored = summary.ignored.len();
        let num_succeeded = summary.succeeded;
        let num_not_run = summary.not_run;

        let summary_line = |msg, cnt| {
            (
                Row::new([Cell::from(msg), Cell::from(cnt)]),
                vec![Constraint::Fill(1), Constraint::Length(9)],
            )
                .into()
        };

        let list_tests = |tests: &Vec<String>, status: Span<'static>| -> Vec<_> {
            let longest = tests.iter().map(|t| t.width()).max().unwrap_or(0);
            tests
                .iter()
                .map(|t| {
                    (
                        Row::new([
                            Cell::from(""),
                            Cell::from(format!("{t}:")),
                            Cell::from(status.clone()),
                        ]),
                        vec![
                            Constraint::Length(4),
                            Constraint::Length(longest as u16 + 1),
                            Constraint::Length(7),
                        ],
                    )
                        .into()
                })
                .collect()
        };

        self.print_above.push(summary_line(
            "Successful Tests".green(),
            num_succeeded.to_string(),
        ));
        self.print_above
            .push(summary_line("Failed Tests".red(), num_failed.to_string()));
        self.print_above
            .extend(list_tests(&summary.failed, "failure".red()));

        if num_ignored > 0 {
            self.print_above.push(summary_line(
                "Ignored Tests".yellow(),
                num_ignored.to_string(),
            ));
            self.print_above
                .extend(list_tests(&summary.ignored, "ignored".yellow()));
        }
        if let Some(num_not_run) = num_not_run {
            self.print_above
                .push(summary_line("Tests Not Run".red(), num_not_run.to_string()));
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
                .gauge_style(tailwind::PURPLE.c800)
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

        if self.all_done.is_none() {
            sections.push((
                Constraint::Fill(1),
                FancyUi::render_running_tests as SectionFnPtr,
            ));
            if !self.remote_progress.is_empty() {
                let max_height = self.remote_progress.len().try_into().unwrap_or(u16::MAX);
                sections.push((
                    Constraint::Max(max_height),
                    FancyUi::render_remote_progress as _,
                ));
            }
            if self.producing_build_output {
                sections.push((Constraint::Length(5), FancyUi::render_build_output as _));
            }
            if self.jobs.failed() > 0 {
                let max_height = (self.jobs.failed() + 2).try_into().unwrap_or(u16::MAX);
                sections.push((
                    Constraint::Max(max_height),
                    FancyUi::render_failed_tests as SectionFnPtr,
                ));
            }
            if self.enqueue_status.is_some() {
                sections.push((Constraint::Length(1), FancyUi::render_enqueue_status as _));
            }
            sections.push((Constraint::Length(3), FancyUi::render_gauge as _));
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

fn init_terminal() -> Result<Terminal<CrosstermBackend<std::io::Stdout>>> {
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
    stdout().execute(cursor::Show)?;
    println!();
    Ok(())
}
