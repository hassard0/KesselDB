//! `kesseldb` — the runnable node binary.
//!
//! Usage: `kesseldb [LISTEN_ADDR] [DATA_DIR]`
//! defaults: 127.0.0.1:7878  ./kesseldb-data

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let addr = args.get(1).cloned().unwrap_or_else(|| "127.0.0.1:7878".into());
    let dir = args.get(2).cloned().unwrap_or_else(|| "kesseldb-data".into());
    println!("KesselDB listening on {addr}, data dir {dir}");
    if let Err(e) = kesseldb_server::run(&addr, &dir) {
        eprintln!("kesseldb: fatal: {e}");
        std::process::exit(1);
    }
}
