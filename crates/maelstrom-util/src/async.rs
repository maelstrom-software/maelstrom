use std::future::Future;

pub async fn await_and_every_sec<RetT>(
    mut fut: impl Future<Output = RetT> + Unpin,
    mut periodic: impl FnMut(),
) -> RetT {
    loop {
        tokio::select! {
            ret = &mut fut => {
                return ret;
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                periodic();
            }
        }
    }
}
