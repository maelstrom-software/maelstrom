use ratatui::prelude::*;

#[derive(Debug, Default, Clone, PartialEq)]
pub struct InnerGauge {
    ratio: f64,
    gauge_style: Style,
}

impl InnerGauge {
    pub fn ratio(mut self, ratio: f64) -> Self {
        assert!(
            (0.0..=1.0).contains(&ratio),
            "Ratio should be between 0 and 1 inclusively."
        );
        self.ratio = ratio;
        self
    }

    pub fn gauge_style<S: Into<Style>>(mut self, style: S) -> Self {
        self.gauge_style = style.into();
        self
    }
}

#[derive(Default)]
pub struct MultiGauge<'a> {
    gauges: Vec<InnerGauge>,
    label: Option<Span<'a>>,
}

impl<'a> MultiGauge<'a> {
    pub fn gauge(mut self, gauge: InnerGauge) -> Self {
        self.gauges.push(gauge);
        self
    }

    pub fn label<T>(mut self, label: T) -> Self
    where
        T: Into<Span<'a>>,
    {
        self.label = Some(label.into());
        self
    }
}

enum Section {
    Filled {
        style: Style,
        width: u16,
    },
    Transition {
        style1: Style,
        style2: Option<Style>,
        frac: f64,
    },
}

impl Section {
    fn width(&self) -> u16 {
        match self {
            Self::Filled { width, .. } => *width,
            Self::Transition { .. } => 1,
        }
    }
}

impl Widget for MultiGauge<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        if area.is_empty() || self.gauges.is_empty() {
            return;
        }

        let mut filled_width = 0;
        let mut sections = vec![];
        let mut steal = 0.0;
        let mut frac = 0.0;
        for i in 0..self.gauges.len() {
            let width = (self.gauges[i].ratio * area.width as f64) - steal;
            if width > 0.0 {
                sections.push(Section::Filled {
                    style: self.gauges[i].gauge_style,
                    width: width as u16,
                });
                filled_width += width as u16;
            }

            frac = width % 1.0;
            if frac != 0.0 && i + 1 < self.gauges.len() && self.gauges[i + 1].ratio != 0.0 {
                sections.push(Section::Transition {
                    style1: self.gauges[i].gauge_style,
                    style2: Some(self.gauges[i + 1].gauge_style),
                    frac,
                });
                filled_width += 1;
                steal = 1.0 - frac;
            } else {
                steal = 0.0;
            }
        }
        if filled_width < area.width && frac > 0.0 {
            let last_gauge = self.gauges.last().unwrap();
            sections.push(Section::Transition {
                style1: last_gauge.gauge_style,
                style2: None,
                frac,
            });
        }

        let default_label = Span::raw(String::new());
        let label = self.label.as_ref().unwrap_or(&default_label);
        let clamped_label_width = area.width.min(label.width() as u16);
        let label_col = area.left() + (area.width - clamped_label_width) / 2;
        let label_row = area.top() + area.height / 2;

        let lengths: Vec<_> = sections
            .iter()
            .map(|g| Constraint::Length(g.width()))
            .collect();
        let areas = Layout::horizontal(lengths).split(area);

        for (section, area) in sections.into_iter().zip(areas.iter()) {
            match section {
                Section::Filled { style, .. } => {
                    buf.set_style(*area, style);
                    for y in area.top()..area.bottom() {
                        for x in area.left()..area.right() {
                            let cell = &mut buf[(x, y)];
                            if x < label_col
                                || x >= label_col + clamped_label_width
                                || y != label_row
                            {
                                cell.set_symbol(symbols::block::FULL)
                                    .set_fg(style.fg.unwrap_or(Color::Reset))
                                    .set_bg(style.bg.unwrap_or(Color::Reset));
                            } else {
                                cell.set_symbol(" ")
                                    .set_fg(style.bg.unwrap_or(Color::Reset))
                                    .set_bg(style.fg.unwrap_or(Color::Reset));
                            }
                        }
                    }
                }
                Section::Transition {
                    style1,
                    style2,
                    frac,
                } => {
                    let fg = style1.fg.unwrap_or(Color::Reset);
                    let bg = if let Some(style2) = style2 {
                        style2.fg.unwrap_or(Color::Reset)
                    } else {
                        Color::Reset
                    };
                    for y in area.top()..area.bottom() {
                        let x = area.left();
                        let cell = &mut buf[(x, y)];
                        if x < label_col || x >= label_col + clamped_label_width || y != label_row {
                            cell.set_symbol(get_unicode_block(frac))
                                .set_fg(fg)
                                .set_bg(bg);
                        } else {
                            // The transition overlaps with the label
                            let c = if frac >= 0.5 { fg } else { bg };
                            cell.set_symbol(" ").set_fg(Color::Reset).set_bg(c);
                        }
                    }
                }
            }
        }

        // render the label
        buf.set_span(label_col, label_row, label, clamped_label_width);
    }
}

fn get_unicode_block<'a>(frac: f64) -> &'a str {
    match (frac * 8.0).round() as u16 {
        1 => symbols::block::ONE_EIGHTH,
        2 => symbols::block::ONE_QUARTER,
        3 => symbols::block::THREE_EIGHTHS,
        4 => symbols::block::HALF,
        5 => symbols::block::FIVE_EIGHTHS,
        6 => symbols::block::THREE_QUARTERS,
        7 => symbols::block::SEVEN_EIGHTHS,
        8 => symbols::block::FULL,
        _ => " ",
    }
}
