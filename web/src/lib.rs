#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

struct UiHandler;

impl UiHandler {
    fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        Self
    }
}

impl eframe::App for UiHandler {
    fn update(&mut self, _ctx: &egui::Context, _frame: &mut eframe::Frame) {}
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(start)]
pub async fn start() -> Result<(), wasm_bindgen::JsValue> {
    console_error_panic_hook::set_once();
    wasm_logger::init(wasm_logger::Config::default());

    let runner = eframe::WebRunner::new();
    let web_options = eframe::WebOptions::default();
    runner
        .start(
            "canvas",
            web_options,
            Box::new(|cc| Box::new(UiHandler::new(cc))),
        )
        .await?;

    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
pub fn start() {
    let native_options = eframe::NativeOptions::default();
    eframe::run_native(
        "meticulous",
        native_options,
        Box::new(|cc| Box::new(UiHandler::new(cc))),
    )
    .unwrap();
}
