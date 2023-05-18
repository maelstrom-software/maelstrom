use meticulous::proto::read_message;
use meticulous::{WorkerHello, WorkerResponse};
use std::io::Error;
use std::net::{SocketAddr, ToSocketAddrs};
use std::process::ExitCode;
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::task;

async fn print(output: String) {
    let mut stdout = io::stdout();
    stdout.write_all(output.as_bytes()).await.unwrap();
    stdout.flush().await.unwrap();
}

async fn handle_connection(mut stream: TcpStream) {
    let hello: WorkerHello = read_message(&mut stream).await.unwrap();
    print(format!("Got hello: {:?}.\n", hello)).await;
    loop {
        let msg: WorkerResponse = read_message(&mut stream).await.unwrap();
        print(format!("Got execution completed: {:?}.\n", msg)).await;
    }
}

async fn server(addr: SocketAddr) -> Result<(), Error> {
    let listener = TcpListener::bind(addr).await?;

    loop {
        let (stream, _) = listener.accept().await?;
        task::spawn(async move { handle_connection(stream).await });
    }
}

async fn command_interpreter(addr: SocketAddr) {
    print(format!(
        "Welcome to the worker tool. Listening on {}.\n",
        addr
    ))
    .await;
    print("% ".to_string()).await;

    let mut lines = BufReader::new(io::stdin()).lines();

    while let Some(line) = lines.next_line().await.unwrap() {
        match line.as_str() {
            "hello" => print("Hello!\n".to_string()).await,
            "exit" => return,
            "" => {}
            _ => print(format!("Unknown command: {}\n", line)).await,
        }
        print("% ".to_string()).await;
    }
}

async fn main2(addr: SocketAddr) -> ExitCode {
    println!("address is: {:?}", addr);

    tokio::select! {
        _ = task::spawn(async move { server(addr).await.unwrap() }) => {},
        _ = task::spawn(async move { command_interpreter(addr).await }) => {},
    };

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
            ExitCode::FAILURE
        }
        Ok(mut iter) => match iter.next() {
            None => {
                eprintln!("address spec didn't yield any addresses");
                ExitCode::FAILURE
            }
            Some(addr) => main2(addr).await,
        },
    }
}
