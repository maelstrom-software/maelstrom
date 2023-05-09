@0xa7470ed7ed800eae;

struct ExecutionId {
    id              @0: UInt64;
}

struct ExecutionDetails {
    program         @0: Float32;
    arguments       @1: Float32;
}

struct ExecutionResults {
    union {
        exitStatus  @0: UInt8;
        signal      @1: Text;
    }
}

struct WorkerDetails {
    name            @0: Text;
    slots           @1: UInt32;
}

interface Worker {
    enqueue @0 (id: ExecutionId, details: ExecutionDetails);
    cancel @1 (id: ExecutionId) -> stream;
}

interface Client {
    hello @0 (details: WorkerDetails);
    completed @1 (id: ExecutionId, results: ExecutionResults);
    canceled @2 (id: ExecutionId) -> stream;
}
