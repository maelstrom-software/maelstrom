fn main() {
    meticulous_worker_child::rtnetlink::ifup_loopback().unwrap();
}
