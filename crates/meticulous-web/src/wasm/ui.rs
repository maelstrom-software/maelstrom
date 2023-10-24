use crate::wasm::rpc::ClientConnection;
use eframe::{App, CreationContext, Frame};
use egui::{CentralPanel, Context, Ui};
use meticulous_base::{
    proto::{BrokerToClient, ClientToBroker},
    stats::{BrokerStatistics, JobState},
    ClientJobId, JobDetails,
};
use meticulous_plot::{Legend, Plot, PlotBounds, PlotPoints, StackedLine};
use std::collections::BTreeSet;
use std::time::Duration;

pub struct UiHandler<RpcConnectionT> {
    rpc: RpcConnectionT,
    stats: Option<BrokerStatistics>,
    next_job_id: u32,
    command: String,
    freshness: f64,
}

impl<RpcConnectionT: ClientConnection> UiHandler<RpcConnectionT> {
    pub fn new(rpc: RpcConnectionT, _cc: &CreationContext<'_>) -> Self {
        Self {
            rpc,
            stats: None,
            next_job_id: 0,
            command: String::new(),
            freshness: 0.0,
        }
    }

    fn submit_job(&mut self) {
        let mut command_parts = self.command.split(" ").map(|p| p.to_string());
        self.rpc
            .send(ClientToBroker::JobRequest(
                ClientJobId::from(self.next_job_id),
                JobDetails {
                    program: command_parts.next().unwrap_or_default(),
                    arguments: command_parts.collect(),
                    layers: vec![],
                },
            ))
            .unwrap();
        self.next_job_id += 1;
    }

    fn draw_stats(&self, ui: &mut Ui, stats: &BrokerStatistics) {
        ui.label(&format!("number of clients: {}", stats.num_clients));
        ui.label(&format!("number of workers: {}", stats.num_workers));

        let clients: BTreeSet<_> = stats
            .job_statistics
            .iter()
            .map(|s| s.client_to_stats.keys())
            .flatten()
            .collect();

        for client in clients {
            ui.heading(&format!("Client {client}"));
            Plot::new("job_statistics")
                .width(1000.0)
                .height(200.0)
                .legend(Legend::default())
                .show(ui, |plot_ui| {
                    let mut prev_points: Option<Vec<[f64; 2]>> = None;
                    let mut lines = vec![];
                    let mut max_height = 100.0;
                    for state in JobState::iter().rev() {
                        let mut points: Vec<_> = stats
                            .job_statistics
                            .iter()
                            .enumerate()
                            .filter_map(|(i, s)| {
                                s.client_to_stats
                                    .get(client)
                                    .map(|e| [i as f64, e[state] as f64])
                            })
                            .collect();

                        if let Some(prev_points) = &prev_points {
                            for (p, prev) in points.iter_mut().zip(prev_points.iter()) {
                                p[1] += prev[1];
                            }
                        }

                        for p in &points {
                            if p[1] > max_height {
                                max_height = p[1];
                            }
                        }

                        if let Some(prev_points) = prev_points.take() {
                            lines.push(
                                StackedLine::new(PlotPoints::new(points.clone()))
                                    .stacked_on(prev_points)
                                    .name(state.to_string()),
                            );
                        } else {
                            lines.push(
                                StackedLine::new(PlotPoints::new(points.clone()))
                                    .name(state.to_string()),
                            );
                        }
                        prev_points = Some(points);
                    }

                    for line in lines.into_iter().rev() {
                        plot_ui.stacked_line(line);
                    }

                    let capacity = stats.job_statistics.capacity();
                    plot_ui.set_plot_bounds(PlotBounds::from_min_max(
                        [0.0, 0.0],
                        [capacity as f64, max_height],
                    ))
                });
        }
    }
}

const REFRESH_INTERVAL: u64 = 100;

impl<RpcConnectionT: ClientConnection> App for UiHandler<RpcConnectionT> {
    fn update(&mut self, ctx: &Context, _frame: &mut Frame) {
        CentralPanel::default().show(ctx, |ui| {
            ui.heading("Meticulous UI");

            ui.add(egui::TextEdit::singleline(&mut self.command));
            if ui.button("submit").clicked() {
                self.submit_job();
            }

            if let Some(stats) = &self.stats {
                self.draw_stats(ui, stats)
            } else {
                ui.label("loading..");
            }

            let now = crate::wasm::window().performance().unwrap().now();
            if now - self.freshness > REFRESH_INTERVAL as f64 {
                self.rpc.send(ClientToBroker::StatisticsRequest).unwrap();
                self.freshness = now;
            }

            if let Some(msg) = self.rpc.try_recv().unwrap() {
                match msg {
                    BrokerToClient::StatisticsResponse(stats) => self.stats = Some(stats),
                    BrokerToClient::JobResponse(..) => (),
                    _ => unimplemented!(),
                }
            }

            ctx.request_repaint_after(Duration::from_millis(REFRESH_INTERVAL / 2));
        });
    }
}
