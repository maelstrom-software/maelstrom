use crate::{artifact_fetcher, artifact_pusher, cache::TempFileFactory, scheduler_task, IdVendor};
use anyhow::Result;
use futures::FutureExt as _;
use maelstrom_base::{
    proto::{ClientToBroker, Hello, MonitorToBroker, WorkerToBroker},
    ClientId, MonitorId, WorkerId,
};
use maelstrom_github::{GitHubClient, GitHubQueue, GitHubQueueAcceptor};
use maelstrom_util::net::{self, AsRawFdExt};
use serde::Serialize;
use slog::{debug, error, info, o, warn, Logger};
use std::{
    future::Future,
    sync::{Arc, Mutex},
    thread,
};
use tokio::{
    io::BufReader,
    net::{TcpListener, TcpStream},
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
    task::{self, JoinSet},
};

/// Main loop for a client, worker, or monitor socket-like object (socket or websocket). There
/// should be one of these for each connected client, worker, or monitor socket. This function will
/// run until the client, worker, or monitor is closed. There is no error return since this
/// function will always eventually run into an error, and we don't really care what that error is.
///
/// # Type Arguments
///
/// * `IdT` - The type of the ID used to identify this client, worker, or monitor
///
/// * `FromSchedulerMessageT` - The type of the messages sent from the scheduler to this client,
///   worker, or monitor
///
/// # Arguments
///
/// * `scheduler_task_sender` - The sender for sending messages to the sender. A "connected" message
///   will be sent using this sender which will include a newly-created FromSchedulerTaskMessageT
///   sender that the scheduler task can use to send messages back to this task. After that
///   messages read from `reader` will be sent to the scheduler task over this sender. Finally,
///   when it's time to disconnected, a "disconnected" message will be sent using this sender
///
/// * `id` - The id for this client or worker. This will be used in the "connected" message and the
///   "disconnected" message
///
/// * `connected_msg_builder` - A closure used to build the "connected" scheduler task message. It
///   takes the `id` and the newly-created FromSchedulerTaskMessageT sender used to communicate
///   with this task. This will be sent immediately to the scheduler task on
///   `scheduler_task_sender`
///
/// * `disconnected_msg_builder` - A closure used to build the "disconnected" scheduler task
///   message. It takes the `id`. This will be sent on `scheduler_task_sender` right before this
///   function returns to tell the scheduler task that this client/worker/monitor has disconnected
///
/// * `socket_reader_main` - An async closure that is called on a new task to read all of the
///   messages from the socket-like object and write them to the supplied scheduler task sender.
///   The scheduler task sender will be a clone of `scheduler_task_sender`
///
/// * `socket_writer_main` - An async closure that is called on a new task to read all of the
///   messages from the supplied scheduler task receiver and write them to the socket-like object.
///   The scheduler task receiver will be a newly-created FromSchedulerTaskMessageT receiver
///
pub async fn connection_main<
    IdT,
    FromSchedulerTaskMessageT,
    ReaderFutureT,
    WriterFutureT,
    TempFileT,
>(
    scheduler_task_sender: scheduler_task::Sender<TempFileT>,
    id: IdT,
    connected_msg_builder: impl FnOnce(
        IdT,
        UnboundedSender<FromSchedulerTaskMessageT>,
    ) -> scheduler_task::Message<TempFileT>,
    disconnected_msg_builder: impl FnOnce(IdT) -> scheduler_task::Message<TempFileT>,
    socket_reader_main: impl FnOnce(scheduler_task::Sender<TempFileT>) -> ReaderFutureT,
    socket_writer_main: impl FnOnce(UnboundedReceiver<FromSchedulerTaskMessageT>) -> WriterFutureT,
) where
    IdT: Copy + Send + 'static,
    FromSchedulerTaskMessageT: Serialize + Send + 'static,
    ReaderFutureT: Future<Output = ()> + Send + 'static,
    WriterFutureT: Future<Output = ()> + Send + 'static,
{
    let (socket_sender, socket_receiver) = mpsc::unbounded_channel();

    // Tell the scheduler that a client/worker has connected. This messages needs to be sent before
    // any messages from this client/worker.
    if scheduler_task_sender
        .send(connected_msg_builder(id, socket_sender))
        .is_err()
    {
        // If we couldn't send a message to the scheduler, it means the scheduler has quit and we
        // should too.
        return;
    }

    // Spawn two tasks to read and write from the socket.
    let mut join_set = JoinSet::new();
    join_set.spawn(socket_reader_main(scheduler_task_sender.clone()));
    join_set.spawn(socket_writer_main(socket_receiver));

    // Wait for one task to complete and then cancel the other one and wait for it.
    join_set.join_next().await;
    join_set.shutdown().await;

    // Tell the scheduler we're done. We do this after waiting for all tasks to complete, since we
    // need to ensure that all messages sent by the socket_reader arrive before this one.
    let _ = scheduler_task_sender.send(disconnected_msg_builder(id));
}

async fn unassigned_tcp_connection_main<TempFileFactoryT>(
    socket: TcpStream,
    scheduler_task_sender: scheduler_task::Sender<TempFileFactoryT::TempFile>,
    id_vendor: Arc<IdVendor>,
    temp_file_factory: TempFileFactoryT,
    log: Logger,
) where
    TempFileFactoryT: TempFileFactory + Send + 'static,
    TempFileFactoryT::TempFile: Send + Sync + 'static,
{
    let mut socket = match socket.set_socket_options() {
        Ok(socket) => socket,
        Err(err) => {
            warn!(log, "error setting socket options"; "error" => %err);
            return;
        }
    };
    match net::read_message_from_async_socket(&mut socket, &log).await {
        Ok(Hello::Client) => {
            let (read_stream, write_stream) = socket.into_split();
            let read_stream = BufReader::new(read_stream);
            let cid: ClientId = id_vendor.vend();
            let log = log.new(o!("cid" => cid.to_string()));
            let log_clone = log.clone();
            let log_clone2 = log.clone();
            debug!(log, "client connected");
            connection_main(
                scheduler_task_sender,
                cid,
                scheduler_task::Message::ClientConnected,
                scheduler_task::Message::ClientDisconnected,
                |scheduler_task_sender| async move {
                    let _ = net::async_socket_reader(
                        read_stream,
                        scheduler_task_sender,
                        |msg| match msg {
                            ClientToBroker::JobRequest(cjid, job_spec) => {
                                assert!(!job_spec.must_be_run_locally());
                                scheduler_task::Message::JobRequestFromClient(cid, cjid, job_spec)
                            }
                            ClientToBroker::ArtifactTransferred(digest, location) => {
                                scheduler_task::Message::ArtifactTransferredFromClient(
                                    cid, digest, location,
                                )
                            }
                        },
                        log_clone,
                        "reading from client socket",
                    )
                    .await;
                },
                |scheduler_task_receiver| async {
                    let _ = net::async_socket_writer(
                        scheduler_task_receiver,
                        write_stream,
                        log_clone2,
                        "writing to client socket",
                    )
                    .await;
                },
            )
            .await;
            debug!(log, "client disconnected");
        }
        Ok(Hello::Worker { slots }) => {
            let (read_stream, write_stream) = socket.into_split();
            let read_stream = BufReader::new(read_stream);
            let wid: WorkerId = id_vendor.vend();
            let log = log.new(o!("wid" => wid.to_string(), "slots" => slots));
            info!(log, "worker connected");
            let log_clone = log.clone();
            let log_clone2 = log.clone();
            connection_main(
                scheduler_task_sender,
                wid,
                |id, sender| scheduler_task::Message::WorkerConnected(id, slots as usize, sender),
                scheduler_task::Message::WorkerDisconnected,
                |scheduler_task_sender| async move {
                    let _ = net::async_socket_reader(
                        read_stream,
                        scheduler_task_sender,
                        |msg| match msg {
                            WorkerToBroker::JobResponse(jid, result) => {
                                scheduler_task::Message::JobResponseFromWorker(wid, jid, result)
                            }
                            WorkerToBroker::JobStatusUpdate(jid, status) => {
                                scheduler_task::Message::JobStatusUpdateFromWorker(wid, jid, status)
                            }
                        },
                        log_clone,
                        "reading from worker socket",
                    )
                    .await;
                },
                |scheduler_task_receiver| async {
                    let _ = net::async_socket_writer(
                        scheduler_task_receiver,
                        write_stream,
                        log_clone2,
                        "writing to worker socket",
                    )
                    .await;
                },
            )
            .await;
            info!(log, "worker disconnected");
        }
        Ok(Hello::Monitor) => {
            let (read_stream, write_stream) = socket.into_split();
            let read_stream = BufReader::new(read_stream);
            let mid: MonitorId = id_vendor.vend();
            let log = log.new(o!("mid" => mid.to_string()));
            let log_clone = log.clone();
            let log_clone2 = log.clone();
            debug!(log, "monitor connected");
            connection_main(
                scheduler_task_sender,
                mid,
                scheduler_task::Message::MonitorConnected,
                scheduler_task::Message::MonitorDisconnected,
                |scheduler_task_sender| async move {
                    let _ = net::async_socket_reader(
                        read_stream,
                        scheduler_task_sender,
                        |msg| match msg {
                            MonitorToBroker::StatisticsRequest => {
                                scheduler_task::Message::StatisticsRequestFromMonitor(mid)
                            }
                        },
                        log_clone,
                        "reading from monitor socket",
                    )
                    .await;
                },
                |scheduler_task_receiver| async {
                    let _ = net::async_socket_writer(
                        scheduler_task_receiver,
                        write_stream,
                        log_clone2,
                        "writing to monitor socket",
                    )
                    .await;
                },
            )
            .await;
            debug!(log, "monitor disconnected");
        }
        Ok(Hello::ArtifactFetcher) => {
            let log = log.new(o!("afid" => id_vendor.vend::<u32>().to_string()));
            let socket = socket.into_std().unwrap();
            socket.set_nonblocking(false).unwrap();
            thread::spawn(move || -> Result<()> {
                artifact_fetcher::connection_main(socket, scheduler_task_sender, log)
            });
        }
        Ok(Hello::ArtifactPusher) => {
            let log = log.new(o!("apid" => id_vendor.vend::<u32>().to_string()));
            let socket = socket.into_std().unwrap();
            socket.set_nonblocking(false).unwrap();
            thread::spawn(move || -> Result<()> {
                artifact_pusher::connection_main(
                    socket,
                    scheduler_task_sender,
                    temp_file_factory,
                    log,
                )
            });
        }
        Err(err) => {
            warn!(log, "error reading hello message"; "error" => %err);
        }
    }
}

pub async fn tcp_listener_main<TempFileFactoryT>(
    listener: TcpListener,
    scheduler_task_sender: scheduler_task::Sender<TempFileFactoryT::TempFile>,
    id_vendor: Arc<IdVendor>,
    temp_file_factory: TempFileFactoryT,
    log: Logger,
) where
    TempFileFactoryT: TempFileFactory + Send + 'static,
    TempFileFactoryT::TempFile: Send + Sync + 'static,
{
    loop {
        match listener.accept().await {
            Ok((socket, peer_addr)) => {
                let log = log.new(o!("peer_addr" => peer_addr));
                debug!(log, "new connection");
                task::spawn(unassigned_tcp_connection_main(
                    socket,
                    scheduler_task_sender.clone(),
                    id_vendor.clone(),
                    temp_file_factory.clone(),
                    log,
                ));
            }
            Err(err) => {
                error!(log, "error accepting connection"; "error" => %err);
                return;
            }
        }
    }
}

async fn unassigned_github_connection_main<TempFileT>(
    queue: GitHubQueue,
    scheduler_task_sender: scheduler_task::Sender<TempFileT>,
    id_vendor: Arc<IdVendor>,
    log: Logger,
) where
    TempFileT: Send + Sync + 'static,
{
    let (mut read_queue, mut write_queue) = queue.into_split();
    match net::read_message_from_github_queue(&mut read_queue, &log).await {
        Ok(Some(Hello::Client)) => {
            warn!(log, "github queue said it was client");
        }
        Ok(Some(Hello::Worker { slots })) => {
            let wid: WorkerId = id_vendor.vend();
            let log = log.new(o!("wid" => wid.to_string(), "slots" => slots));
            info!(log, "worker connected");
            let log_clone = log.clone();
            let log_clone2 = log.clone();

            let write_queue = Arc::new(tokio::sync::Mutex::new(write_queue));
            let write_queue_clone = write_queue.clone();
            connection_main(
                scheduler_task_sender,
                wid,
                |id, sender| scheduler_task::Message::WorkerConnected(id, slots as usize, sender),
                scheduler_task::Message::WorkerDisconnected,
                |scheduler_task_sender| async move {
                    let _ = net::github_queue_reader(
                        &mut read_queue,
                        scheduler_task_sender,
                        |msg| match msg {
                            WorkerToBroker::JobResponse(jid, result) => {
                                scheduler_task::Message::JobResponseFromWorker(wid, jid, result)
                            }
                            WorkerToBroker::JobStatusUpdate(jid, status) => {
                                scheduler_task::Message::JobStatusUpdateFromWorker(wid, jid, status)
                            }
                        },
                        log_clone,
                        "reading from worker github queue",
                    )
                    .await;
                },
                |scheduler_task_receiver| async move {
                    let mut write_queue = write_queue_clone.lock().await;
                    let _ = net::github_queue_writer(
                        scheduler_task_receiver,
                        &mut write_queue,
                        log_clone2,
                        "writing to worker github queue",
                    )
                    .await;
                },
            )
            .await;
            info!(log, "worker disconnected");
            let _ = write_queue.lock().await.shut_down().await;

            return;
        }
        Ok(Some(Hello::Monitor)) => {
            warn!(log, "github queue said it was monitor");
        }
        Ok(Some(Hello::ArtifactFetcher)) => {
            warn!(log, "github queue said it was artifact fetcher");
        }
        Ok(Some(Hello::ArtifactPusher)) => {
            warn!(log, "github queue said it was artifact pusher");
        }
        Ok(None) => {
            warn!(log, "github queue shutdown");
        }
        Err(err) => {
            warn!(log, "error reading hello message"; "error" => %err);
        }
    }

    let _ = write_queue.shut_down().await;
}

pub async fn github_acceptor_main<TempFileT>(
    client: GitHubClient,
    scheduler_task_sender: scheduler_task::Sender<TempFileT>,
    id_vendor: Arc<IdVendor>,
    log: Logger,
    tasks: Arc<Mutex<JoinSet<()>>>,
) where
    TempFileT: Send + Sync + 'static,
{
    let mut acceptor = match GitHubQueueAcceptor::new(client, "maelstrom-broker").await {
        Ok(a) => a,
        Err(err) => {
            error!(log, "error accepting connection"; "error" => %err);
            return;
        }
    };
    info!(log, "listening for connections via GitHub");

    loop {
        match acceptor.accept_one().await {
            Ok(queue) => {
                let log = log.new(o!("peer_addr" => "github peer"));
                debug!(log, "new connection");

                let mut tasks = tasks.lock().unwrap();

                // Remove any completed tasks from the set
                while let Some(Some(_)) =
                    tokio::task::unconstrained(tasks.join_next()).now_or_never()
                {}

                tasks.spawn(unassigned_github_connection_main(
                    queue,
                    scheduler_task_sender.clone(),
                    id_vendor.clone(),
                    log,
                ));
            }
            Err(err) => {
                error!(log, "error accepting connection"; "error" => %err);
                return;
            }
        }
    }
}
