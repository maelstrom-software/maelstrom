use std::process;
use std::sync::mpsc;
use std::thread;

use meticulous::{Scheduler, SchedulerDeps};

type InstanceId = u32;
struct InstanceDetails {
    program: String,
    args: Vec<String>,
}
type InstanceResult = process::ExitStatus;

fn start_program(
    id: InstanceId,
    details: &InstanceDetails,
    sender: mpsc::Sender<(InstanceId, InstanceResult)>,
) {
    let mut child = process::Command::new(&details.program)
        .args(&details.args)
        .stdin(process::Stdio::null())
        .spawn()
        .expect("starting child process failed");
    thread::spawn(move || {
        let result = child.wait();
        sender
            .send((id, result.expect("waiting on child process failed")))
            .unwrap();
    });
}

struct SchedulerAdapter {
    sender: mpsc::Sender<(InstanceId, InstanceResult)>,
}

impl SchedulerDeps for SchedulerAdapter {
    type ExecutorId = u32;
    type InstanceId = InstanceId;
    type InstanceDetails = InstanceDetails;

    fn start_test_execution(
        &mut self,
        _executor: Self::ExecutorId,
        instance_id: InstanceId,
        instance_details: &InstanceDetails,
    ) {
        start_program(instance_id, instance_details, self.sender.clone());
    }
}

fn main() {
    let (sender, receiver) = mpsc::channel();
    let scheduler_adapter = SchedulerAdapter { sender: sender };
    let mut scheduler = Scheduler::new(scheduler_adapter);

    scheduler.add_executor(1000, 10);

    for i in 1..100 {
        let details = InstanceDetails {
            program: String::from("echo"),
            args: Vec::from([format!("{}", i)]),
        };
        scheduler.add_tests([(i, details)]);
    }

    loop {
        match receiver.recv() {
            Err(_) => return,
            Ok((instance_id, instance_result)) => {
                println!(
                    "instance {} exited with result {}",
                    instance_id, instance_result
                );
                scheduler.receive_test_execution_done(1000, instance_id);
            }
        }
    }
}
