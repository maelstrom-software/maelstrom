use crate::driver::{ClientDeps, ClientDriver};
use anyhow::Result;
use async_trait::async_trait;
use maelstrom_client_base::ClientMessageKind;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Default, Clone)]
pub struct SingleThreadedClientDriver {
    deps: Arc<Mutex<Option<ClientDeps>>>,
}

#[async_trait]
impl ClientDriver for SingleThreadedClientDriver {
    async fn drive(&self, deps: ClientDeps) {
        *self.deps.lock().await = Some(deps);
    }

    async fn stop(&self) -> Result<()> {
        Ok(())
    }

    async fn process_broker_msg_single_threaded(&self, count: usize) {
        let mut locked_deps = self.deps.lock().await;
        let deps = locked_deps.as_mut().unwrap();

        for _ in 0..count {
            deps.socket_reader.process_one().await;
            deps.dispatcher.process_one().await.unwrap();
        }
    }

    async fn process_client_messages_single_threaded(&self, wanted: ClientMessageKind) {
        loop {
            let mut locked_deps = self.deps.lock().await;
            let deps = locked_deps.as_mut().unwrap();
            let kind = deps.dispatcher.process_one_and_tell().await;
            if kind.is_some_and(|k| k == wanted) {
                break;
            }
        }
    }

    async fn process_artifact_single_threaded(&self) {
        let mut locked_deps = self.deps.lock().await;
        let deps = locked_deps.as_mut().unwrap();
        deps.artifact_pusher.process_one().await;
        deps.artifact_pusher.wait().await;
    }
}
