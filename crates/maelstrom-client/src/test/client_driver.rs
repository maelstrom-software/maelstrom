use crate::{ClientDeps, ClientDriver};
use anyhow::Result;
use std::sync::{Arc, Mutex};
use std::thread;

#[derive(Default, Clone)]
pub struct SingleThreadedClientDriver {
    deps: Arc<Mutex<Option<ClientDeps>>>,
}

impl ClientDriver for SingleThreadedClientDriver {
    fn drive(&mut self, deps: ClientDeps) {
        *self.deps.lock().unwrap() = Some(deps);
    }

    fn stop(&mut self) -> Result<()> {
        Ok(())
    }

    fn process_broker_msg_single_threaded(&self, count: usize) {
        let mut locked_deps = self.deps.lock().unwrap();
        let deps = locked_deps.as_mut().unwrap();

        for _ in 0..count {
            deps.socket_reader.process_one();
            deps.dispatcher.try_process_one().unwrap();
        }
    }

    fn process_client_messages_single_threaded(&self) {
        let mut locked_deps = self.deps.lock().unwrap();
        let deps = locked_deps.as_mut().unwrap();
        while deps.dispatcher.try_process_one().is_ok() {}
    }

    fn process_artifact_single_threaded(&self) {
        let mut locked_deps = self.deps.lock().unwrap();
        let deps = locked_deps.as_mut().unwrap();
        thread::scope(|scope| {
            deps.artifact_pusher.process_one(scope);
        });
    }
}
