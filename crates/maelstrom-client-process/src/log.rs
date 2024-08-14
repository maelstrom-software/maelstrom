use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use maelstrom_client_base::{RpcLogKeyValue, RpcLogMessage};
use slog::KV as _;

#[derive(Clone)]
pub struct RpcLogSink {
    sender: UnboundedSender<RpcLogMessage>,
}

impl RpcLogSink {
    pub fn new() -> (Self, UnboundedReceiver<RpcLogMessage>) {
        let (sender, receiver) = unbounded();
        (Self { sender }, receiver)
    }
}

impl slog::Drain for RpcLogSink {
    type Ok = ();
    type Err = slog::Never;

    fn log(
        &self,
        record: &slog::Record<'_>,
        values: &slog::OwnedKVList,
    ) -> Result<Self::Ok, Self::Err> {
        let mut s = Serializer::default();
        values.serialize(record, &mut s).unwrap();
        let _ = self.sender.unbounded_send(RpcLogMessage {
            message: record.msg().to_string(),
            level: record.level(),
            tag: record.tag().into(),
            key_values: s.0,
        });
        Ok(())
    }
}

#[derive(Default)]
struct Serializer(Vec<RpcLogKeyValue>);

impl slog::Serializer for Serializer {
    fn emit_arguments(&mut self, key: slog::Key, val: &std::fmt::Arguments<'_>) -> slog::Result {
        self.0.push(RpcLogKeyValue {
            key: key.into(),
            value: val.to_string(),
        });
        Ok(())
    }
}
