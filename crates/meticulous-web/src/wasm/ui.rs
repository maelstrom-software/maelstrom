use crate::wasm::rpc::ClientConnection;
use eframe::{App, CreationContext, Frame};
use egui::{CentralPanel, Context};
use meticulous_base::{
    proto::{BrokerToClient, ClientToBroker},
    stats::BrokerStatistics,
};
use std::time::Duration;

pub struct UiHandler<RpcConnectionT> {
    rpc: RpcConnectionT,
    stats: Option<BrokerStatistics>,
}

impl<RpcConnectionT> UiHandler<RpcConnectionT> {
    pub fn new(rpc: RpcConnectionT, _cc: &CreationContext<'_>) -> Self {
        Self { rpc, stats: None }
    }
}

impl<RpcConnectionT: ClientConnection> App for UiHandler<RpcConnectionT> {
    fn update(&mut self, ctx: &Context, _frame: &mut Frame) {
        CentralPanel::default().show(ctx, |ui| {
            ui.heading("Meticulous UI");
            if let Some(stats) = &self.stats {
                ui.label(&format!("number of clients: {}", stats.num_clients));
                ui.label(&format!("number of workers: {}", stats.num_workers));
            } else {
                ui.label("loading..");
            }

            self.rpc.send(ClientToBroker::StatisticsRequest).unwrap();

            if let Some(msg) = self.rpc.try_recv().unwrap() {
                match msg {
                    BrokerToClient::StatisticsResponse(stats) => self.stats = Some(stats),
                    _ => unimplemented!(),
                }
            }

            ctx.request_repaint_after(Duration::from_millis(500));
        });
    }
}
