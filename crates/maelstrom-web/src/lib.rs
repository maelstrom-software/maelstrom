#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

#[cfg(target_arch = "wasm32")]
mod wasm;

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(start)]
pub async fn start() -> Result<(), JsValue> {
    match wasm::start().await {
        Ok(()) => Ok(()),
        Err(e) => panic!("error: {e:?}"),
    }
}

#[cfg(all(not(target_arch = "wasm32"), not(debug_assertions)))]
pub const WASM_TAR: &[u8] = include_bytes!("../../../target/wasm_release/web.tar");

#[cfg(all(not(target_arch = "wasm32"), debug_assertions))]
pub const WASM_TAR: &[u8] = include_bytes!("../../../target/wasm_dev/web.tar");
