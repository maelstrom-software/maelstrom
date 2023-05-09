use capnp::capability::Promise;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::AsyncReadExt;
use meticulous::worker_capnp::{client, worker};
use std::net::{SocketAddr, ToSocketAddrs};
use std::process::ExitCode;
use tokio::io::{self, AsyncWriteExt, AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;
use tokio::task;

async fn print(output: String) {
    let mut stdout = io::stdout();
    stdout.write_all(output.as_bytes()).await.unwrap();
    stdout.flush().await.unwrap();
}

struct Worker {}

impl worker::Server for Worker {
    fn enqueue(
        &mut self,
        _params: worker::EnqueueParams,
        _results: worker::EnqueueResults,
    ) -> Promise<(), capnp::Error> {
        Promise::ok(())
    }

    fn cancel(
        &mut self,
        _params: worker::CancelParams,
        _results: worker::CancelResults,
    ) -> Promise<(), capnp::Error> {
        Promise::ok(())
    }
}

async fn command_interpreter(addr: SocketAddr) {
    print(format!("Welcome to the worker tool. Connecting to {}.\n", addr)).await;

    let stream = TcpStream::connect(&addr).await.unwrap();
    let (reader, writer) = tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();

    let network = twoparty::VatNetwork::new(
        reader,
        writer,
        rpc_twoparty_capnp::Side::Client,
        Default::default(),
        );

    let mut rpc_system = RpcSystem::new(Box::new(network), None);

    let broker_client: client::Client = rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);

    task::spawn_local(rpc_system);

    let mut request = broker_client.hello_request();
    let mut details = request.get().init_details();
    details.set_name("worker1");
    details.set_slots(16);
    request.send().promise.await.unwrap();

    print(format!("Done.\n")).await;
    print(format!("% ")).await;

    let mut lines = BufReader::new(io::stdin()).lines();

    while let Some(line) = lines.next_line().await.unwrap() {
        match line.as_str() {
            "hello" => print(format!("Hello!\n")).await,
            "exit" => return,
            "" => {},
                _ => print(format!("Unknown command: {}\n", line)).await
        }
        print(format!("% ")).await;
    }
}

async fn main2(addr: SocketAddr) -> ExitCode {
    println!("address is: {:?}", addr);

    task::LocalSet::new().run_until(async move {
            command_interpreter(addr).await
    }).await;

    ExitCode::SUCCESS
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = ::std::env::args().collect();

    if args.len() != 2 {
        eprintln!("usage: {} ADDRESS", args[0]);
        return ExitCode::FAILURE;
    }

    match args[1].to_socket_addrs() {
        Err(e) => {
            eprintln!("parsing address: {}", e);
            return ExitCode::FAILURE;
        }
        Ok(mut iter) => match iter.next() {
            None => {
                eprintln!("address spec didn't yield any addresses");
                return ExitCode::FAILURE;
            }
            Some(addr) => main2(addr).await,
        },
    }
}
