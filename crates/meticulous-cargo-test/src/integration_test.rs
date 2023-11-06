use crate::MainApp;
use indicatif::InMemoryTerm;
use meticulous_client::Client;
use std::net::{Ipv6Addr, SocketAddrV6, TcpListener};
use std::sync::Mutex;

#[test]
fn no_tests() {
    let conn = TcpListener::bind(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0)).unwrap();
    let broker_address = conn.local_addr().unwrap();
    let client = Mutex::new(Client::new(broker_address).unwrap());
    let cargo = "/bin/echo";
    let app = MainApp::new(client, cargo.into(), std::io::sink(), false);
    let term = InMemoryTerm::new(50, 50);
    app.run(false, false, term.clone()).unwrap();
    assert_eq!(
        term.contents(),
        "\
        all jobs completed\n\
        \n\
        ================== Test Summary ==================\n\
        Successful Tests:         0\n\
        Failed Tests    :         0\
        "
    );
}
