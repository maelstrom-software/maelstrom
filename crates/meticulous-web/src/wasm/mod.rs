use anyhow::Result;
use eframe::{WebOptions, WebRunner};
use gloo_utils::errors::JsError;
use wasm_logger::Config;
use web_sys::Window;

mod rpc;
mod ui;

fn window() -> Window {
    web_sys::window().expect("no global `window` exists")
}

fn host() -> String {
    window().location().host().unwrap()
}

pub async fn start() -> Result<(), JsError> {
    console_error_panic_hook::set_once();
    wasm_logger::init(Config::default());

    let uri = format!("ws://{}", host());
    let rpc = rpc::RpcConnection::new(&uri)?;

    let runner = WebRunner::new();
    let web_options = WebOptions::default();
    runner
        .start(
            "canvas",
            web_options,
            Box::new(|cc| Box::new(ui::UiHandler::new(rpc, cc))),
        )
        .await
        .map_err(|e| JsError::try_from(e).unwrap())?;

    Ok(())
}
