use crate::wasm::rpc::ClientConnection;
use eframe::{App, CreationContext, Frame};
use egui::{CentralPanel, CollapsingHeader, Color32, Context, ScrollArea, Ui};
use egui_gauge::Gauge;
use meticulous_base::{
    proto::{BrokerToClient, ClientToBroker},
    stats::{BrokerStatistics, JobState, JobStateCounts, BROKER_STATISTICS_INTERVAL},
    ClientJobId, JobDetails, JobResult,
};
use meticulous_plot::{Legend, Plot, PlotBounds, PlotPoints, PlotUi, StackedLine};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

const REFRESH_INTERVAL: Duration = BROKER_STATISTICS_INTERVAL;

fn merge_job_state_counts(mut a: JobStateCounts, b: &JobStateCounts) -> JobStateCounts {
    for state in JobState::iter() {
        a[state] += b[state];
    }
    a
}

struct JobInfo {
    command: String,
    result: Option<JobResult>,
}

pub struct UiHandler<RpcConnectionT> {
    rpc: RpcConnectionT,
    stats: Option<BrokerStatistics>,
    next_job_id: u32,
    command: String,
    freshness: f64,
    jobs: BTreeMap<ClientJobId, JobInfo>,
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
            jobs: BTreeMap::new(),
        }
    }

    fn submit_job(&mut self) {
        let mut command_parts = self.command.split(" ").map(|p| p.to_string());
        let job_id = ClientJobId::from(self.next_job_id);
        self.jobs.insert(
            job_id.clone(),
            JobInfo {
                command: self.command.clone(),
                result: None,
            },
        );
        self.rpc
            .send(ClientToBroker::JobRequest(
                job_id,
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
        CollapsingHeader::new(&format!("All Clients Job Graph"))
            .default_open(true)
            .show(ui, |ui| {
                Plot::new(&format!("all_clients_job_statistics"))
                    .width(1000.0)
                    .height(200.0)
                    .legend(Legend::default())
                    .show(ui, |plot_ui| {
                        self.plot_graph(capacity, all_jobs.iter(), plot_ui)
                    });
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
            let data = stats
                .job_statistics
                .iter()
                .filter_map(|s| s.client_to_stats.get(client));
            ui.collapsing(&format!("Client {client} Job Graph"), |ui| {
                Plot::new(&format!("client_{client}_job_statistics"))
                    .width(1000.0)
                    .height(200.0)
                    .legend(Legend::default())
                    .show(ui, |plot_ui| self.plot_graph(capacity, data, plot_ui));
            });
        }
    }

    fn draw_stats(&self, ui: &mut Ui, stats: &BrokerStatistics) {
        let last_stat = stats.job_statistics.iter().last();
        let num_clients = last_stat.map(|s| s.client_to_stats.len()).unwrap_or(0);
        let num_workers = stats.worker_statistics.len();

        let num_slots: u64 = stats
            .worker_statistics
            .values()
            .map(|s| s.slots as u64)
            .sum();
        let num_running_jobs = last_stat
            .map(|s| {
                s.client_to_stats
                    .values()
                    .map(|c| c[JobState::Running])
                    .sum()
            })
            .unwrap_or(0);
        let num_total_jobs = last_stat
            .map(|s| {
                s.client_to_stats
                    .values()
                    .map(|c| {
                        JobState::iter()
                            .filter(|s| s != &JobState::Complete)
                            .map(|s| c[s])
                            .sum::<u64>()
                    })
                    .sum()
            })
            .unwrap_or(0);

        ui.horizontal(|ui| {
            ui.vertical(|ui| {
                ui.heading(num_clients.to_string());
                ui.label("client(s) connected");
                ui.heading(num_workers.to_string());
                ui.label("worker(s) connected");
                ui.heading(num_total_jobs.to_string());
                ui.label("total job(s)");
            });

            if num_slots > 0 {
                ui.add(
                    Gauge::new(num_running_jobs, 0..=(num_slots * 2), 150.0, Color32::RED)
                        .text("used slots"),
                );
            }
        });

        self.draw_all_clients_graph(ui, stats);
        self.draw_client_graphs(ui, stats);
    }

    fn handle_rpcs(&mut self) {
        let now = crate::wasm::window().performance().unwrap().now();
        if now - self.freshness > REFRESH_INTERVAL.as_millis() as f64 {
            self.rpc.send(ClientToBroker::StatisticsRequest).unwrap();
            self.freshness = now;
        }

        if let Some(msg) = self.rpc.try_recv().unwrap() {
            match msg {
                BrokerToClient::StatisticsResponse(stats) => self.stats = Some(stats),
                BrokerToClient::JobResponse(job_id, result) => {
                    self.jobs.get_mut(&job_id).unwrap().result = Some(result);
                }
                _ => unimplemented!(),
            }
        }
    }
}

impl<RpcConnectionT: ClientConnection> App for UiHandler<RpcConnectionT> {
    fn update(&mut self, ctx: &Context, _frame: &mut Frame) {
        CentralPanel::default().show(ctx, |ui| {
            ScrollArea::vertical().show(ui, |ui| {
                ui.heading("Meticulous UI");

                if let Some(stats) = &self.stats {
                    self.draw_stats(ui, stats)
                } else {
                    ui.label("loading..");
                }

                ui.collapsing("Submit a Job", |ui| {
                    ui.horizontal(|ui| {
                        ui.label("command");
                        ui.add(egui::TextEdit::singleline(&mut self.command));
                        if ui.button("submit").clicked() {
                            self.submit_job();
                        }
                    });
                    ScrollArea::vertical().max_height(100.0).show(ui, |ui| {
                        for (id, job) in &self.jobs {
                            let result = if let Some(res) = &job.result {
                                format!("{res:?}")
                            } else {
                                "pending".into()
                            };
                            ui.label(&format!("{} {}: {}", id, &job.command, result));
                        }
                    });
                });
            });

            self.handle_rpcs();

            ctx.request_repaint_after(REFRESH_INTERVAL / 2);
        });
    }
}
