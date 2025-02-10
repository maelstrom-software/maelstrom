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

#[cfg(all(not(target_arch = "wasm32"), not(doc), feature = "web-ui"))]
pub const WASM_TAR: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/web.tar"));
