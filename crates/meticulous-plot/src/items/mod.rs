use super::{Cursor, LabelFormatter, PlotBounds, PlotTransform};
use egui::{
    epaint,
    epaint::{util::FloatOrd, Mesh},
    pos2, vec2, Align2, Color32, NumExt as _, Pos2, Rect, Rgba, Shape, Stroke, TextStyle, Ui,
    WidgetText,
};
use std::ops::RangeInclusive;
use values::{ClosestElem, PlotGeometry};
pub use values::{LineStyle, MarkerShape, Orientation, PlotPoint, PlotPoints};

mod values;

const DEFAULT_FILL_ALPHA: f32 = 0.05;

/// Container to pass-through several parameters related to plot visualization
pub(super) struct PlotConfig<'a> {
    pub ui: &'a Ui,
    pub transform: &'a PlotTransform,
    pub show_x: bool,
    pub show_y: bool,
}

/// Trait shared by things that can be drawn in the plot.
pub(super) trait PlotItem {
    fn shapes(&self, ui: &mut Ui, transform: &PlotTransform, shapes: &mut Vec<Shape>);

    /// For plot-items which are generated based on x values (plotting functions).
    fn initialize(&mut self, x_range: RangeInclusive<f64>);

    fn name(&self) -> &str;

    fn color(&self) -> Color32;

    fn highlight(&mut self);

    fn highlighted(&self) -> bool;

    fn geometry(&self) -> PlotGeometry<'_>;

    fn bounds(&self) -> PlotBounds;

    fn find_closest(&self, point: Pos2, transform: &PlotTransform) -> Option<ClosestElem> {
        match self.geometry() {
            PlotGeometry::None => None,

            PlotGeometry::Points(points) => points
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    let pos = transform.position_from_point(value);
                    let dist_sq = point.distance_sq(pos);
                    ClosestElem { index, dist_sq }
                })
                .min_by_key(|e| e.dist_sq.ord()),
        }
    }

    fn on_hover(
        &self,
        elem: ClosestElem,
        shapes: &mut Vec<Shape>,
        cursors: &mut Vec<Cursor>,
        plot: &PlotConfig<'_>,
        label_formatter: &LabelFormatter,
    ) {
        let points = match self.geometry() {
            PlotGeometry::Points(points) => points,
            PlotGeometry::None => {
                panic!("If the PlotItem has no geometry, on_hover() must not be called")
            }
        };

        let line_color = if plot.ui.visuals().dark_mode {
            Color32::from_gray(100).additive()
        } else {
            Color32::from_black_alpha(180)
        };

        // this method is only called, if the value is in the result set of find_closest()
        let value = points[elem.index];
        let pointer = plot.transform.position_from_point(&value);
        shapes.push(Shape::circle_filled(pointer, 3.0, line_color));

        rulers_at_value(
            pointer,
            value,
            self.name(),
            plot,
            shapes,
            cursors,
            label_formatter,
        );
    }
}

/// A series of values forming a path.
pub struct Line {
    pub(super) series: PlotPoints,
    pub(super) stroke: Stroke,
    pub(super) name: String,
    pub(super) highlight: bool,
    pub(super) fill: Option<f32>,
    pub(super) style: LineStyle,
}

impl Line {
    pub fn new(series: impl Into<PlotPoints>) -> Self {
        Self {
            series: series.into(),
            stroke: Stroke::new(1.0, Color32::TRANSPARENT),
            name: Default::default(),
            highlight: false,
            fill: None,
            style: LineStyle::Solid,
        }
    }

    /// Highlight this line in the plot by scaling up the line.
    pub fn highlight(mut self, highlight: bool) -> Self {
        self.highlight = highlight;
        self
    }

    /// Add a stroke.
    pub fn stroke(mut self, stroke: impl Into<Stroke>) -> Self {
        self.stroke = stroke.into();
        self
    }

    /// Stroke width. A high value means the plot thickens.
    pub fn width(mut self, width: impl Into<f32>) -> Self {
        self.stroke.width = width.into();
        self
    }

    /// Stroke color. Default is `Color32::TRANSPARENT` which means a color will be auto-assigned.
    pub fn color(mut self, color: impl Into<Color32>) -> Self {
        self.stroke.color = color.into();
        self
    }

    /// Fill the area between this line and a given horizontal reference line.
    pub fn fill(mut self, y_reference: impl Into<f32>) -> Self {
        self.fill = Some(y_reference.into());
        self
    }

    /// Set the line's style. Default is `LineStyle::Solid`.
    pub fn style(mut self, style: LineStyle) -> Self {
        self.style = style;
        self
    }

    /// Name of this line.
    ///
    /// This name will show up in the plot legend, if legends are turned on.
    ///
    /// Multiple plot items may share the same name, in which case they will also share an entry in
    /// the legend.
    #[allow(clippy::needless_pass_by_value)]
    pub fn name(mut self, name: impl ToString) -> Self {
        self.name = name.to_string();
        self
    }
}

/// Returns the x-coordinate of a possible intersection between a line segment from `p1` to `p2` and
/// a horizontal line at the given y-coordinate.
fn y_intersection(p1: &Pos2, p2: &Pos2, y: f32) -> Option<f32> {
    ((p1.y > y && p2.y < y) || (p1.y < y && p2.y > y))
        .then_some(((y * (p1.x - p2.x)) - (p1.x * p2.y - p1.y * p2.x)) / (p1.y - p2.y))
}

impl PlotItem for Line {
    fn shapes(&self, _ui: &mut Ui, transform: &PlotTransform, shapes: &mut Vec<Shape>) {
        let Self {
            series,
            stroke,
            highlight,
            mut fill,
            style,
            ..
        } = self;

        let values_tf: Vec<_> = series
            .points()
            .iter()
            .map(|v| transform.position_from_point(v))
            .collect();
        let n_values = values_tf.len();

        // Fill the area between the line and a reference line, if required.
        if n_values < 2 {
            fill = None;
        }
        if let Some(y_reference) = fill {
            let mut fill_alpha = DEFAULT_FILL_ALPHA;
            if *highlight {
                fill_alpha = (2.0 * fill_alpha).at_most(1.0);
            }
            let y = transform
                .position_from_point(&PlotPoint::new(0.0, y_reference))
                .y;
            let fill_color = Rgba::from(stroke.color)
                .to_opaque()
                .multiply(fill_alpha)
                .into();
            let mut mesh = Mesh::default();
            let expected_intersections = 20;
            mesh.reserve_triangles((n_values - 1) * 2);
            mesh.reserve_vertices(n_values * 2 + expected_intersections);
            values_tf.windows(2).for_each(|w| {
                let i = mesh.vertices.len() as u32;
                mesh.colored_vertex(w[0], fill_color);
                mesh.colored_vertex(pos2(w[0].x, y), fill_color);
                if let Some(x) = y_intersection(&w[0], &w[1], y) {
                    let point = pos2(x, y);
                    mesh.colored_vertex(point, fill_color);
                    mesh.add_triangle(i, i + 1, i + 2);
                    mesh.add_triangle(i + 2, i + 3, i + 4);
                } else {
                    mesh.add_triangle(i, i + 1, i + 2);
                    mesh.add_triangle(i + 1, i + 2, i + 3);
                }
            });
            let last = values_tf[n_values - 1];
            mesh.colored_vertex(last, fill_color);
            mesh.colored_vertex(pos2(last.x, y), fill_color);
            shapes.push(Shape::Mesh(mesh));
        }
        style.style_line(values_tf, *stroke, *highlight, shapes);
    }

    fn initialize(&mut self, x_range: RangeInclusive<f64>) {
        self.series.generate_points(x_range);
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn color(&self) -> Color32 {
        self.stroke.color
    }

    fn highlight(&mut self) {
        self.highlight = true;
    }

    fn highlighted(&self) -> bool {
        self.highlight
    }

    fn geometry(&self) -> PlotGeometry<'_> {
        PlotGeometry::Points(self.series.points())
    }

    fn bounds(&self) -> PlotBounds {
        self.series.bounds()
    }
}

/// A series of values forming a path stacked on top of another line
pub struct StackedLine {
    pub(super) series: PlotPoints,
    pub(super) stroke: Stroke,
    pub(super) name: String,
    pub(super) highlight: bool,
    pub(super) stacked_on: Option<PlotPoints>,
    pub(super) style: LineStyle,
}

impl StackedLine {
    pub fn new(series: impl Into<PlotPoints>) -> Self {
        Self {
            series: series.into(),
            stroke: Stroke::new(1.0, Color32::TRANSPARENT),
            name: Default::default(),
            highlight: false,
            stacked_on: None,
            style: LineStyle::Solid,
        }
    }

    /// Highlight this line in the plot by scaling up the line.
    pub fn highlight(mut self, highlight: bool) -> Self {
        self.highlight = highlight;
        self
    }

    /// Add a stroke.
    pub fn stroke(mut self, stroke: impl Into<Stroke>) -> Self {
        self.stroke = stroke.into();
        self
    }

    /// Stroke width. A high value means the plot thickens.
    pub fn width(mut self, width: impl Into<f32>) -> Self {
        self.stroke.width = width.into();
        self
    }

    /// Stroke color. Default is `Color32::TRANSPARENT` which means a color will be auto-assigned.
    pub fn color(mut self, color: impl Into<Color32>) -> Self {
        self.stroke.color = color.into();
        self
    }

    /// This is the other line this line in stacked on top of
    pub fn stacked_on(mut self, stacked_on: impl Into<PlotPoints>) -> Self {
        self.stacked_on = Some(stacked_on.into());
        self
    }

    /// Set the line's style. Default is `LineStyle::Solid`.
    pub fn style(mut self, style: LineStyle) -> Self {
        self.style = style;
        self
    }

    /// Name of this line.
    ///
    /// This name will show up in the plot legend, if legends are turned on.
    ///
    /// Multiple plot items may share the same name, in which case they will also share an entry in
    /// the legend.
    #[allow(clippy::needless_pass_by_value)]
    pub fn name(mut self, name: impl ToString) -> Self {
        self.name = name.to_string();
        self
    }
}

impl PlotItem for StackedLine {
    fn shapes(&self, _ui: &mut Ui, transform: &PlotTransform, shapes: &mut Vec<Shape>) {
        let Self {
            series,
            stroke,
            highlight,
            stacked_on,
            style,
            ..
        } = self;

        if series.points().len() <= 1 {
            let values_tf: Vec<_> = series
                .points()
                .iter()
                .map(|v| transform.position_from_point(v))
                .collect();
            style.style_line(values_tf, *stroke, *highlight, shapes);
            return;
        }

        let bottom = PlotPoints::new(
            (0..series.points().len())
                .map(|i| [i as f64, 0.0])
                .collect(),
        );
        let stacked_on = stacked_on.as_ref().unwrap_or(&bottom);
        let values_tf: Vec<_> = series
            .points()
            .iter()
            .zip(stacked_on.points().iter())
            .map(|(v, y)| {
                (
                    transform.position_from_point(v),
                    transform.position_from_point(y),
                )
            })
            .collect();

        let n_values = values_tf.len();
        let mut fill_alpha = DEFAULT_FILL_ALPHA;
        if *highlight {
            fill_alpha = (2.0 * fill_alpha).at_most(1.0);
        }
        let fill_color = Rgba::from(stroke.color)
            .to_opaque()
            .multiply(fill_alpha)
            .into();
        let mut mesh = Mesh::default();
        let expected_intersections = 20;
        mesh.reserve_triangles((n_values - 1) * 2);
        mesh.reserve_vertices(n_values * 2 + expected_intersections);
        values_tf.windows(2).for_each(|w| {
            let primary = [w[0].0, w[1].0];
            let stacked = [w[0].1, w[1].1];
            let i = mesh.vertices.len() as u32;
            mesh.colored_vertex(primary[0], fill_color);
            mesh.colored_vertex(pos2(primary[0].x, stacked[0].y), fill_color);
            if let Some(x) = y_intersection(&primary[0], &primary[1], stacked[0].y) {
                let point = pos2(x, stacked[0].y);
                mesh.colored_vertex(point, fill_color);
                mesh.add_triangle(i, i + 1, i + 2);
                mesh.add_triangle(i + 2, i + 3, i + 4);
            } else {
                mesh.add_triangle(i, i + 1, i + 2);
                mesh.add_triangle(i + 1, i + 2, i + 3);
            }
        });
        let last = values_tf[n_values - 1];
        mesh.colored_vertex(last.0, fill_color);
        mesh.colored_vertex(pos2(last.0.x, last.1.y), fill_color);
        shapes.push(Shape::Mesh(mesh));

        let values_tf = values_tf.iter().map(|e| e.0).collect();
        style.style_line(values_tf, *stroke, *highlight, shapes);
    }

    fn initialize(&mut self, x_range: RangeInclusive<f64>) {
        self.series.generate_points(x_range);
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn color(&self) -> Color32 {
        self.stroke.color
    }

    fn highlight(&mut self) {
        self.highlight = true;
    }

    fn highlighted(&self) -> bool {
        self.highlight
    }

    fn geometry(&self) -> PlotGeometry<'_> {
        PlotGeometry::Points(self.series.points())
    }

    fn bounds(&self) -> PlotBounds {
        self.series.bounds()
    }
}

/// A convex polygon.
pub struct Polygon {
    pub(super) series: PlotPoints,
    pub(super) stroke: Stroke,
    pub(super) name: String,
    pub(super) highlight: bool,
    pub(super) fill_color: Option<Color32>,
    pub(super) style: LineStyle,
}

impl Polygon {
    pub fn new(series: impl Into<PlotPoints>) -> Self {
        Self {
            series: series.into(),
            stroke: Stroke::new(1.0, Color32::TRANSPARENT),
            name: Default::default(),
            highlight: false,
            fill_color: None,
            style: LineStyle::Solid,
        }
    }

    /// Highlight this polygon in the plot by scaling up the stroke and reducing the fill
    /// transparency.
    pub fn highlight(mut self, highlight: bool) -> Self {
        self.highlight = highlight;
        self
    }

    /// Add a custom stroke.
    pub fn stroke(mut self, stroke: impl Into<Stroke>) -> Self {
        self.stroke = stroke.into();
        self
    }

    /// Set the stroke width.
    pub fn width(mut self, width: impl Into<f32>) -> Self {
        self.stroke.width = width.into();
        self
    }

    #[deprecated = "Use `fill_color`."]
    #[allow(unused, clippy::needless_pass_by_value)]
    pub fn color(mut self, color: impl Into<Color32>) -> Self {
        self
    }

    #[deprecated = "Use `fill_color`."]
    #[allow(unused, clippy::needless_pass_by_value)]
    pub fn fill_alpha(mut self, _alpha: impl Into<f32>) -> Self {
        self
    }

    /// Fill color. Defaults to the stroke color with added transparency.
    pub fn fill_color(mut self, color: impl Into<Color32>) -> Self {
        self.fill_color = Some(color.into());
        self
    }

    /// Set the outline's style. Default is `LineStyle::Solid`.
    pub fn style(mut self, style: LineStyle) -> Self {
        self.style = style;
        self
    }

    /// Name of this polygon.
    ///
    /// This name will show up in the plot legend, if legends are turned on.
    ///
    /// Multiple plot items may share the same name, in which case they will also share an entry in
    /// the legend.
    #[allow(clippy::needless_pass_by_value)]
    pub fn name(mut self, name: impl ToString) -> Self {
        self.name = name.to_string();
        self
    }
}

impl PlotItem for Polygon {
    fn shapes(&self, _ui: &mut Ui, transform: &PlotTransform, shapes: &mut Vec<Shape>) {
        let Self {
            series,
            stroke,
            highlight,
            fill_color,
            style,
            ..
        } = self;

        let mut values_tf: Vec<_> = series
            .points()
            .iter()
            .map(|v| transform.position_from_point(v))
            .collect();

        let fill_color = fill_color.unwrap_or(stroke.color.linear_multiply(DEFAULT_FILL_ALPHA));

        let shape = Shape::convex_polygon(values_tf.clone(), fill_color, Stroke::NONE);
        shapes.push(shape);
        values_tf.push(*values_tf.first().unwrap());
        style.style_line(values_tf, *stroke, *highlight, shapes);
    }

    fn initialize(&mut self, x_range: RangeInclusive<f64>) {
        self.series.generate_points(x_range);
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn color(&self) -> Color32 {
        self.stroke.color
    }

    fn highlight(&mut self) {
        self.highlight = true;
    }

    fn highlighted(&self) -> bool {
        self.highlight
    }

    fn geometry(&self) -> PlotGeometry<'_> {
        PlotGeometry::Points(self.series.points())
    }

    fn bounds(&self) -> PlotBounds {
        self.series.bounds()
    }
}

/// Text inside the plot.
#[derive(Clone)]
pub struct Text {
    pub(super) text: WidgetText,
    pub(super) position: PlotPoint,
    pub(super) name: String,
    pub(super) highlight: bool,
    pub(super) color: Color32,
    pub(super) anchor: Align2,
}

impl Text {
    pub fn new(position: PlotPoint, text: impl Into<WidgetText>) -> Self {
        Self {
            text: text.into(),
            position,
            name: Default::default(),
            highlight: false,
            color: Color32::TRANSPARENT,
            anchor: Align2::CENTER_CENTER,
        }
    }

    /// Highlight this text in the plot by drawing a rectangle around it.
    pub fn highlight(mut self, highlight: bool) -> Self {
        self.highlight = highlight;
        self
    }

    /// Text color.
    pub fn color(mut self, color: impl Into<Color32>) -> Self {
        self.color = color.into();
        self
    }

    /// Anchor position of the text. Default is `Align2::CENTER_CENTER`.
    pub fn anchor(mut self, anchor: Align2) -> Self {
        self.anchor = anchor;
        self
    }

    /// Name of this text.
    ///
    /// This name will show up in the plot legend, if legends are turned on.
    ///
    /// Multiple plot items may share the same name, in which case they will also share an entry in
    /// the legend.
    #[allow(clippy::needless_pass_by_value)]
    pub fn name(mut self, name: impl ToString) -> Self {
        self.name = name.to_string();
        self
    }
}

impl PlotItem for Text {
    fn shapes(&self, ui: &mut Ui, transform: &PlotTransform, shapes: &mut Vec<Shape>) {
        let color = if self.color == Color32::TRANSPARENT {
            ui.style().visuals.text_color()
        } else {
            self.color
        };

        let galley =
            self.text
                .clone()
                .into_galley(ui, Some(false), f32::INFINITY, TextStyle::Small);

        let pos = transform.position_from_point(&self.position);
        let rect = self
            .anchor
            .anchor_rect(Rect::from_min_size(pos, galley.size()));

        let mut text_shape = egui::epaint::TextShape::new(rect.min, galley.galley);
        if !galley.galley_has_color {
            text_shape.override_text_color = Some(color);
        }
        shapes.push(text_shape.into());

        if self.highlight {
            shapes.push(Shape::rect_stroke(
                rect.expand(2.0),
                1.0,
                Stroke::new(0.5, color),
            ));
        }
    }

    fn initialize(&mut self, _x_range: RangeInclusive<f64>) {}

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn color(&self) -> Color32 {
        self.color
    }

    fn highlight(&mut self) {
        self.highlight = true;
    }

    fn highlighted(&self) -> bool {
        self.highlight
    }

    fn geometry(&self) -> PlotGeometry<'_> {
        PlotGeometry::None
    }

    fn bounds(&self) -> PlotBounds {
        let mut bounds = PlotBounds::NOTHING;
        bounds.extend_with(&self.position);
        bounds
    }
}

/// A set of points.
pub struct Points {
    pub(super) series: PlotPoints,

    pub(super) shape: MarkerShape,

    /// Color of the marker. `Color32::TRANSPARENT` means that it will be picked automatically.
    pub(super) color: Color32,

    /// Whether to fill the marker. Does not apply to all types.
    pub(super) filled: bool,

    /// The maximum extent of the marker from its center.
    pub(super) radius: f32,

    pub(super) name: String,

    pub(super) highlight: bool,

    pub(super) stems: Option<f32>,
}

impl Points {
    pub fn new(series: impl Into<PlotPoints>) -> Self {
        Self {
            series: series.into(),
            shape: MarkerShape::Circle,
            color: Color32::TRANSPARENT,
            filled: true,
            radius: 1.0,
            name: Default::default(),
            highlight: false,
            stems: None,
        }
    }

    /// Set the shape of the markers.
    pub fn shape(mut self, shape: MarkerShape) -> Self {
        self.shape = shape;
        self
    }

    /// Highlight these points in the plot by scaling up their markers.
    pub fn highlight(mut self, highlight: bool) -> Self {
        self.highlight = highlight;
        self
    }

    /// Set the marker's color.
    pub fn color(mut self, color: impl Into<Color32>) -> Self {
        self.color = color.into();
        self
    }

    /// Whether to fill the marker.
    pub fn filled(mut self, filled: bool) -> Self {
        self.filled = filled;
        self
    }

    /// Whether to add stems between the markers and a horizontal reference line.
    pub fn stems(mut self, y_reference: impl Into<f32>) -> Self {
        self.stems = Some(y_reference.into());
        self
    }

    /// Set the maximum extent of the marker around its position.
    pub fn radius(mut self, radius: impl Into<f32>) -> Self {
        self.radius = radius.into();
        self
    }

    /// Name of this set of points.
    ///
    /// This name will show up in the plot legend, if legends are turned on.
    ///
    /// Multiple plot items may share the same name, in which case they will also share an entry in
    /// the legend.
    #[allow(clippy::needless_pass_by_value)]
    pub fn name(mut self, name: impl ToString) -> Self {
        self.name = name.to_string();
        self
    }
}

impl PlotItem for Points {
    fn shapes(&self, _ui: &mut Ui, transform: &PlotTransform, shapes: &mut Vec<Shape>) {
        let sqrt_3 = 3_f32.sqrt();
        let frac_sqrt_3_2 = 3_f32.sqrt() / 2.0;
        let frac_1_sqrt_2 = 1.0 / 2_f32.sqrt();

        let Self {
            series,
            shape,
            color,
            filled,
            mut radius,
            highlight,
            stems,
            ..
        } = self;

        let stroke_size = radius / 5.0;

        let default_stroke = Stroke::new(stroke_size, *color);
        let mut stem_stroke = default_stroke;
        let (fill, stroke) = if *filled {
            (*color, Stroke::NONE)
        } else {
            (Color32::TRANSPARENT, default_stroke)
        };

        if *highlight {
            radius *= 2f32.sqrt();
            stem_stroke.width *= 2.0;
        }

        let y_reference = stems.map(|y| transform.position_from_point(&PlotPoint::new(0.0, y)).y);

        series
            .points()
            .iter()
            .map(|value| transform.position_from_point(value))
            .for_each(|center| {
                let tf = |dx: f32, dy: f32| -> Pos2 { center + radius * vec2(dx, dy) };

                if let Some(y) = y_reference {
                    let stem = Shape::line_segment([center, pos2(center.x, y)], stem_stroke);
                    shapes.push(stem);
                }

                match shape {
                    MarkerShape::Circle => {
                        shapes.push(Shape::Circle(epaint::CircleShape {
                            center,
                            radius,
                            fill,
                            stroke,
                        }));
                    }
                    MarkerShape::Diamond => {
                        let points = vec![
                            tf(0.0, 1.0),  // bottom
                            tf(-1.0, 0.0), // left
                            tf(0.0, -1.0), // top
                            tf(1.0, 0.0),  // right
                        ];
                        shapes.push(Shape::convex_polygon(points, fill, stroke));
                    }
                    MarkerShape::Square => {
                        let points = vec![
                            tf(-frac_1_sqrt_2, frac_1_sqrt_2),
                            tf(-frac_1_sqrt_2, -frac_1_sqrt_2),
                            tf(frac_1_sqrt_2, -frac_1_sqrt_2),
                            tf(frac_1_sqrt_2, frac_1_sqrt_2),
                        ];
                        shapes.push(Shape::convex_polygon(points, fill, stroke));
                    }
                    MarkerShape::Cross => {
                        let diagonal1 = [
                            tf(-frac_1_sqrt_2, -frac_1_sqrt_2),
                            tf(frac_1_sqrt_2, frac_1_sqrt_2),
                        ];
                        let diagonal2 = [
                            tf(frac_1_sqrt_2, -frac_1_sqrt_2),
                            tf(-frac_1_sqrt_2, frac_1_sqrt_2),
                        ];
                        shapes.push(Shape::line_segment(diagonal1, default_stroke));
                        shapes.push(Shape::line_segment(diagonal2, default_stroke));
                    }
                    MarkerShape::Plus => {
                        let horizontal = [tf(-1.0, 0.0), tf(1.0, 0.0)];
                        let vertical = [tf(0.0, -1.0), tf(0.0, 1.0)];
                        shapes.push(Shape::line_segment(horizontal, default_stroke));
                        shapes.push(Shape::line_segment(vertical, default_stroke));
                    }
                    MarkerShape::Up => {
                        let points =
                            vec![tf(0.0, -1.0), tf(0.5 * sqrt_3, 0.5), tf(-0.5 * sqrt_3, 0.5)];
                        shapes.push(Shape::convex_polygon(points, fill, stroke));
                    }
                    MarkerShape::Down => {
                        let points = vec![
                            tf(0.0, 1.0),
                            tf(-0.5 * sqrt_3, -0.5),
                            tf(0.5 * sqrt_3, -0.5),
                        ];
                        shapes.push(Shape::convex_polygon(points, fill, stroke));
                    }
                    MarkerShape::Left => {
                        let points =
                            vec![tf(-1.0, 0.0), tf(0.5, -0.5 * sqrt_3), tf(0.5, 0.5 * sqrt_3)];
                        shapes.push(Shape::convex_polygon(points, fill, stroke));
                    }
                    MarkerShape::Right => {
                        let points = vec![
                            tf(1.0, 0.0),
                            tf(-0.5, 0.5 * sqrt_3),
                            tf(-0.5, -0.5 * sqrt_3),
                        ];
                        shapes.push(Shape::convex_polygon(points, fill, stroke));
                    }
                    MarkerShape::Asterisk => {
                        let vertical = [tf(0.0, -1.0), tf(0.0, 1.0)];
                        let diagonal1 = [tf(-frac_sqrt_3_2, 0.5), tf(frac_sqrt_3_2, -0.5)];
                        let diagonal2 = [tf(-frac_sqrt_3_2, -0.5), tf(frac_sqrt_3_2, 0.5)];
                        shapes.push(Shape::line_segment(vertical, default_stroke));
                        shapes.push(Shape::line_segment(diagonal1, default_stroke));
                        shapes.push(Shape::line_segment(diagonal2, default_stroke));
                    }
                }
            });
    }

    fn initialize(&mut self, x_range: RangeInclusive<f64>) {
        self.series.generate_points(x_range);
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn color(&self) -> Color32 {
        self.color
    }

    fn highlight(&mut self) {
        self.highlight = true;
    }

    fn highlighted(&self) -> bool {
        self.highlight
    }

    fn geometry(&self) -> PlotGeometry<'_> {
        PlotGeometry::Points(self.series.points())
    }

    fn bounds(&self) -> PlotBounds {
        self.series.bounds()
    }
}

// ----------------------------------------------------------------------------
// Helper functions

pub(crate) fn rulers_color(ui: &Ui) -> Color32 {
    if ui.visuals().dark_mode {
        Color32::from_gray(100).additive()
    } else {
        Color32::from_black_alpha(180)
    }
}

pub(crate) fn vertical_line(
    pointer: Pos2,
    transform: &PlotTransform,
    line_color: Color32,
) -> Shape {
    let frame = transform.frame();
    Shape::line_segment(
        [
            pos2(pointer.x, frame.top()),
            pos2(pointer.x, frame.bottom()),
        ],
        (1.0, line_color),
    )
}

pub(crate) fn horizontal_line(
    pointer: Pos2,
    transform: &PlotTransform,
    line_color: Color32,
) -> Shape {
    let frame = transform.frame();
    Shape::line_segment(
        [
            pos2(frame.left(), pointer.y),
            pos2(frame.right(), pointer.y),
        ],
        (1.0, line_color),
    )
}

/// Draws a cross of horizontal and vertical ruler at the `pointer` position.
/// `value` is used to for text displaying X/Y coordinates.
#[allow(clippy::too_many_arguments)]
pub(super) fn rulers_at_value(
    pointer: Pos2,
    value: PlotPoint,
    name: &str,
    plot: &PlotConfig<'_>,
    shapes: &mut Vec<Shape>,
    cursors: &mut Vec<Cursor>,
    label_formatter: &LabelFormatter,
) {
    if plot.show_x {
        cursors.push(Cursor::Vertical { x: value.x });
    }
    if plot.show_y {
        cursors.push(Cursor::Horizontal { y: value.y });
    }

    let mut prefix = String::new();

    if !name.is_empty() {
        prefix = format!("{name}\n");
    }

    let text = {
        let scale = plot.transform.dvalue_dpos();
        let x_decimals = ((-scale[0].abs().log10()).ceil().at_least(0.0) as usize).clamp(1, 6);
        let y_decimals = ((-scale[1].abs().log10()).ceil().at_least(0.0) as usize).clamp(1, 6);
        if let Some(custom_label) = label_formatter {
            custom_label(name, &value)
        } else if plot.show_x && plot.show_y {
            format!(
                "{}x = {:.*}\ny = {:.*}",
                prefix, x_decimals, value.x, y_decimals, value.y
            )
        } else if plot.show_x {
            format!("{}x = {:.*}", prefix, x_decimals, value.x)
        } else if plot.show_y {
            format!("{}y = {:.*}", prefix, y_decimals, value.y)
        } else {
            unreachable!()
        }
    };

    let font_id = TextStyle::Body.resolve(plot.ui.style());
    plot.ui.fonts(|f| {
        shapes.push(Shape::text(
            f,
            pointer + vec2(3.0, -2.0),
            Align2::LEFT_BOTTOM,
            text,
            font_id,
            plot.ui.visuals().text_color(),
        ));
    });
}
