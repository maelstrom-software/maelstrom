use crate::wasm::rpc::ClientConnection;
use eframe::{App, CreationContext, Frame};
use egui::{CentralPanel, Context, Ui};
use meticulous_base::{
    proto::{BrokerToClient, ClientToBroker},
    stats::{BrokerStatistics, JobState, JobStateCounts},
    ClientJobId, JobDetails,
};
use meticulous_plot::{Legend, Plot, PlotBounds, PlotPoints, PlotUi, StackedLine};
use std::collections::BTreeSet;
use std::time::Duration;

fn merge_job_state_counts(mut a: JobStateCounts, b: &JobStateCounts) -> JobStateCounts {
    for state in JobState::iter() {
        a[state] += b[state];
    }
    a
}

pub struct UiHandler<RpcConnectionT> {
    rpc: RpcConnectionT,
    stats: Option<BrokerStatistics>,
    next_job_id: u32,
    command: String,
    freshness: f64,
}

struct LineStacker {
    prev_points: Option<Vec<[f64; 2]>>,
    max_height: f64,
}

impl LineStacker {
    fn new() -> Self {
        Self {
            prev_points: None,
            max_height: 100.0,
        }
    }

    fn stack_points(&self, points: &mut Vec<[f64; 2]>) {
        if let Some(prev_points) = &self.prev_points {
            for (p, prev) in points.iter_mut().zip(prev_points.iter()) {
                p[1] += prev[1];
            }
        }
    }

    fn find_max(&mut self, points: &[[f64; 2]]) {
        for p in points {
            if p[1] > self.max_height {
                self.max_height = p[1];
            }
        }
    }

    fn plot_line<'a>(
        &mut self,
        state: JobState,
        data: impl Iterator<Item = &'a JobStateCounts> + 'a,
    ) -> StackedLine {
        let mut points: Vec<_> = data
            .enumerate()
            .map(|(i, e)| [i as f64, e[state] as f64])
            .collect();

        self.stack_points(&mut points);
        self.find_max(&points);

        let mut line = StackedLine::new(PlotPoints::new(points.clone())).name(state.to_string());
        if let Some(prev_points) = self.prev_points.take() {
            line = line.stacked_on(prev_points);
        }
        self.prev_points = Some(points);

        line
    }
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

    fn plot_graph<'a>(
        &self,
        capacity: usize,
        data: impl Iterator<Item = &'a JobStateCounts> + 'a + Clone,
        plot_ui: &mut PlotUi,
    ) {
        let mut stacker = LineStacker::new();
        let mut lines = vec![];

        for state in JobState::iter().rev() {
            lines.push(stacker.plot_line(state, data.clone()));
        }

        for line in lines.into_iter().rev() {
            plot_ui.stacked_line(line);
        }

        plot_ui.set_plot_bounds(PlotBounds::from_min_max(
            [0.0, 0.0],
            [capacity as f64, stacker.max_height],
        ))
    }

    fn draw_all_clients_graph(&self, ui: &mut Ui, stats: &BrokerStatistics) {
        let capacity = stats.job_statistics.capacity();

        let all_jobs: Vec<_> = stats
            .job_statistics
            .iter()
            .map(|s| {
                s.client_to_stats
                    .values()
                    .fold(JobStateCounts::default(), merge_job_state_counts)
            })
            .collect();
        ui.heading(&format!("All Clients"));
        Plot::new(&format!("all_clients_job_statistics"))
            .width(1000.0)
            .height(200.0)
            .legend(Legend::default())
            .show(ui, |plot_ui| {
                self.plot_graph(capacity, all_jobs.iter(), plot_ui)
            });
    }

    fn draw_client_graphs(&self, ui: &mut Ui, stats: &BrokerStatistics) {
        let capacity = stats.job_statistics.capacity();

        let last_entry = stats.job_statistics.iter().last();
        let clients: BTreeSet<_> = last_entry
            .into_iter()
            .map(|s| s.client_to_stats.keys())
            .flatten()
            .collect();

        for client in clients {
            ui.heading(&format!("Client {client}"));

            let data = stats
                .job_statistics
                .iter()
                .filter_map(|s| s.client_to_stats.get(client));
            Plot::new(&format!("client_{client}_job_statistics"))
                .width(1000.0)
                .height(200.0)
                .legend(Legend::default())
                .show(ui, |plot_ui| self.plot_graph(capacity, data, plot_ui));
        }
    }

    fn draw_stats(&self, ui: &mut Ui, stats: &BrokerStatistics) {
        ui.label(&format!("number of clients: {}", stats.num_clients));
        ui.label(&format!("number of workers: {}", stats.num_workers));

        self.draw_all_clients_graph(ui, stats);
        self.draw_client_graphs(ui, stats);
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
