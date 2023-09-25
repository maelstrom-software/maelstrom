use meticulous_base::proto::{BrokerStatistics, UiRequest, UiResponse};
use std::time::Duration;

pub type Result<T> = std::result::Result<T, anyhow::Error>;

pub trait ClientRpcConnection {
    fn send(&self, message: UiRequest) -> Result<()>;
    fn try_recv(&self) -> Result<Option<UiResponse>>;
}

pub struct UiHandler<RpcConnectionT> {
    rpc: RpcConnectionT,
    stats: Option<BrokerStatistics>,
}

impl<RpcConnectionT> UiHandler<RpcConnectionT> {
    pub fn new(rpc: RpcConnectionT, _cc: &eframe::CreationContext<'_>) -> Self {
        Self { rpc, stats: None }
    }
}

impl<RpcConnectionT: ClientRpcConnection> eframe::App for UiHandler<RpcConnectionT> {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("Meticulous UI");
            if let Some(stats) = &self.stats {
                ui.label(&format!("number of clients: {}", stats.num_clients));
                ui.label(&format!("number of workers: {}", stats.num_workers));
                ui.label(&format!("number of requests: {}", stats.num_requests));
            } else {
                ui.label("loading..");
            }

            self.rpc.send(UiRequest::GetStatistics).unwrap();

            if let Some(UiResponse::GetStatistics(stats)) = self.rpc.try_recv().unwrap() {
                self.stats = Some(stats);
            }

            ctx.request_repaint_after(Duration::from_millis(500));
        });
    }
}
