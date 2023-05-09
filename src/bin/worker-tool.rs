use capnp::capability::Promise;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem, pry};
use futures::AsyncReadExt;
use meticulous::worker_capnp::client;
use std::io::Error;
use std::net::{SocketAddr, ToSocketAddrs};
use std::process::ExitCode;
use tokio::io::{self, AsyncWriteExt, AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::task;

async fn print(output: String) {
    let mut stdout = io::stdout();
    stdout.write_all(output.as_bytes()).await.unwrap();
    stdout.flush().await.unwrap();
}

struct WorkerTool {
}

impl client::Server for WorkerTool {
    fn hello(
        &mut self,
        params: client::HelloParams,
        _results: client::HelloResults,
    ) -> Promise<(), capnp::Error> {
        let details = pry!(pry!(params.get()).get_details());
        let name = pry!(details.get_name()).to_string();
        let slots = details.get_slots();

        Promise::from_future(async move {
            print(format!("Received hello message: {}:{}\n", name, slots)).await;
            Ok(())
        })
    }

    fn completed(
        &mut self,
        _params: client::CompletedParams,
        _results: client::CompletedResults,
    ) -> Promise<(), capnp::Error> {
        Promise::ok(())
    }

    fn canceled(
        &mut self,
        _params: client::CanceledParams,
        _results: client::CanceledResults,
    ) -> Promise<(), capnp::Error> {
        Promise::ok(())
    }
}

async fn handle_connection(stream: TcpStream) {
    let (reader, writer) = tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();

    let network = twoparty::VatNetwork::new(
        reader,
        writer,
        rpc_twoparty_capnp::Side::Server,
        Default::default(),
        );

    let clnt: client::Client = capnp_rpc::new_client(WorkerTool {});

    RpcSystem::new(Box::new(network), Some(clnt.client)).await.unwrap();
}

async fn server(addr: SocketAddr) -> Result<(), Error> {
    let listener = TcpListener::bind(addr).await?;

    loop {
        let (stream, _) = listener.accept().await?;
        task::spawn_local(async move { handle_connection(stream).await });
    }
}

async fn command_interpreter(addr: SocketAddr) {
    print(format!("Welcome to the worker tool. Listening on {}.\n", addr)).await;
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
        let addr2 = addr.clone();
        tokio::select! {
            _ = task::spawn_local(async move { server(addr).await.unwrap() }) => {},
            _ = task::spawn_local(async move { command_interpreter(addr2).await }) => {},
        };
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
