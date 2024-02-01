use anyhow::Result;
use maelstrom_client::{ClientDeps, ClientDriver};
use std::sync::{Arc, Mutex};
use std::thread;

#[derive(Default, Clone)]
pub struct TestClientDriver {
    deps: Arc<Mutex<Option<ClientDeps>>>,
}

impl ClientDriver for TestClientDriver {
    fn drive(&mut self, deps: ClientDeps) {
        *self.deps.lock().unwrap() = Some(deps);
    }

    fn stop(&mut self) -> Result<()> {
        Ok(())
    }
}

impl TestClientDriver {
    pub fn process_broker_msg(&self, count: usize) {
        let mut locked_deps = self.deps.lock().unwrap();
        let deps = locked_deps.as_mut().unwrap();

        for _ in 0..count {
            deps.socket_reader.process_one();
            deps.dispatcher.try_process_one().unwrap();
        }
    }

    pub fn process_client_messages(&self) {
        let mut locked_deps = self.deps.lock().unwrap();
        let deps = locked_deps.as_mut().unwrap();
        while deps.dispatcher.try_process_one().is_ok() {}
    }

    pub fn process_artifact(&self, body: impl FnOnce()) {
        let mut locked_deps = self.deps.lock().unwrap();
        let deps = locked_deps.as_mut().unwrap();
        thread::scope(|scope| {
            deps.artifact_pusher.process_one(scope);
            body()
        });
    }
}
