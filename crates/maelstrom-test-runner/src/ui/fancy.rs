use super::{Ui, UiMessage};
use crate::config::Quiet;
use anyhow::Result;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, SystemTime};

pub struct FancyUi {}

impl FancyUi {
    pub fn new(_list: bool, _stdout_is_tty: bool, _quiet: Quiet) -> Self {
        Self {}
    }
}

impl Ui for FancyUi {
    fn run(&mut self, recv: Receiver<UiMessage>) -> Result<()> {
        let mut last_tick = SystemTime::now();
        loop {
            if last_tick
                .elapsed()
                .is_ok_and(|v| v > Duration::from_millis(500))
            {
                // animate?
                last_tick = SystemTime::now();
            }

            match recv.recv_timeout(Duration::from_millis(500)) {
                Ok(msg) => match msg {
                    UiMessage::PrintLine(_line) => todo!(),
                    UiMessage::PrintLineWidth(_cb) => todo!(),
                    UiMessage::JobFinished => todo!(),
                    UiMessage::UpdatePendingJobsCount(_count) => todo!(),
                    UiMessage::UpdateIntrospectState(_resp) => {
                        todo!()
                    }
                    UiMessage::UpdateEnqueueStatus(_msg) => todo!(),
                    UiMessage::DoneQueuingJobs => todo!(),
                    UiMessage::AllJobsFinished(_summary) => todo!(),
                },
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        Ok(())
    }
}
