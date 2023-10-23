use crate::wasm::rpc::ClientConnection;
use eframe::{App, CreationContext, Frame};
use egui::plot::{Line, Plot, PlotBounds, PlotPoints};
use egui::{CentralPanel, Context};
use meticulous_base::{
    proto::{BrokerToClient, ClientToBroker},
    stats::{BrokerStatistics, JobState},
    ClientJobId, JobDetails,
};
use std::time::Duration;

pub struct UiHandler<RpcConnectionT> {
    rpc: RpcConnectionT,
    stats: Option<BrokerStatistics>,
    next_job_id: u32,
    command: String,
    freshness: f64,
}

impl<RpcConnectionT> UiHandler<RpcConnectionT> {
    pub fn new(rpc: RpcConnectionT, _cc: &CreationContext<'_>) -> Self {
        Self {
            rpc,
            stats: None,
            next_job_id: 0,
            command: String::new(),
            freshness: 0.0,
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

            if let Some(stats) = &self.stats {
                ui.label(&format!("number of clients: {}", stats.num_clients));
                ui.label(&format!("number of workers: {}", stats.num_workers));

                let clients: Vec<_> = stats
                    .job_statistics
                    .iter()
                    .map(|s| s.client_to_stats.keys())
                    .flatten()
                    .collect();
                if let Some(client) = clients.get(0) {
                    Plot::new("job_statistics")
                        .width(1000.0)
                        .height(200.0)
                        .show(ui, |plot_ui| {
                            for state in JobState::iter() {
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

                                let capacity = stats.job_statistics.capacity();
                                plot_ui.line(Line::new(PlotPoints::new(points)));
                                plot_ui.set_plot_bounds(PlotBounds::from_min_max(
                                    [0.0, 0.0],
                                    [capacity as f64, 100.0],
                                ))
                            }
                        });
                }
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
                    _ => unimplemented!(),
                }
            }

            ctx.request_repaint_after(Duration::from_millis(REFRESH_INTERVAL / 2));
        });
    }
}
