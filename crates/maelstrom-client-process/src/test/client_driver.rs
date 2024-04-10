use crate::driver::{ClientDeps, ClientDriver};
use anyhow::Result;
use async_trait::async_trait;
use maelstrom_client_base::ClientMessageKind;
use tokio::sync::Mutex;

#[derive(Default)]
pub struct SingleThreadedClientDriver {
    deps: Mutex<Option<ClientDeps>>,
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
            deps.dispatcher
                .receive_message(deps.dispatcher_receiver.recv().await.unwrap())
                .await
                .unwrap();
        }
    }

    async fn process_client_messages_single_threaded(&self, wanted: ClientMessageKind) {
        loop {
            let mut locked_deps = self.deps.lock().await;
            let deps = locked_deps.as_mut().unwrap();
            let msg = deps.dispatcher_receiver.recv().await;
            if let Some(msg) = msg {
                let kind = msg.kind();
                if deps.dispatcher.receive_message(msg).await.is_ok() && kind == wanted {
                    break;
                }
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
