//! `kessel` — the KesselDB command-line client.
//!
//! Designed for both humans and agents: deterministic, line-oriented,
//! scriptable, with meaningful exit codes.
//!
//! Usage:
//!   kessel [--addr HOST:PORT] [--token TOKEN] ["SQL STATEMENT"]
//!
//!   * with a SQL argument  -> one-shot: run it, print the result, exit
//!     (exit code 1 if the statement errored, so scripts/agents can tell).
//!   * without one          -> read statements from stdin, one per line
//!     (interactive prompt if a TTY; otherwise a clean pipe consumer).
//!
//! Examples:
//!   kessel "CREATE TABLE t (v U64 NOT NULL)"
//!   kessel --addr 10.0.0.1:7878 "SELECT SUM(v) FROM t"
//!   echo "SELECT * FROM t ID 1" | kessel
//!   kessel --token s3cret              # interactive shell
//!
//! Lines beginning with `#` or `--` are treated as comments; `quit`,
//! `exit` or `\q` end an interactive session. Zero external dependencies.

use kessel_client::{format_result, render_rows, Client};
use kessel_proto::OpResult;
use std::io::{BufRead, IsTerminal, Write};

const HELP: &str = "\
kessel — KesselDB CLI

USAGE:
  kessel [--addr HOST:PORT] [--token TOKEN] [\"SQL\"]

OPTIONS:
  --addr  HOST:PORT   server address (default 127.0.0.1:7878)
  --token TOKEN       shared-secret token, if the server requires auth
  -h, --help          show this help

MODES:
  one-shot   pass a SQL string as the final argument
  pipe       no SQL arg + piped stdin: one statement per line
  shell      no SQL arg + a TTY: interactive prompt

EXIT CODES:
  0  success    1  statement error / connection failure   2  bad usage
";

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut addr = "127.0.0.1:7878".to_string();
    let mut token: Option<String> = None;
    let mut sql_parts: Vec<String> = Vec::new();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "-h" | "--help" => {
                print!("{HELP}");
                return;
            }
            "--addr" => {
                i += 1;
                match args.get(i) {
                    Some(a) => addr = a.clone(),
                    None => fail_usage("--addr needs a value"),
                }
            }
            "--token" => {
                i += 1;
                match args.get(i) {
                    Some(t) => token = Some(t.clone()),
                    None => fail_usage("--token needs a value"),
                }
            }
            other => sql_parts.push(other.to_string()),
        }
        i += 1;
    }

    let mut client = match &token {
        Some(t) => Client::connect_authed(&addr, t.as_bytes()),
        None => Client::connect(&addr),
    }
    .unwrap_or_else(|e| {
        eprintln!("kessel: cannot connect to {addr}: {e}");
        std::process::exit(1);
    });

    // One-shot mode.
    if !sql_parts.is_empty() {
        let stmt = sql_parts.join(" ");
        std::process::exit(run_one(&mut client, &stmt));
    }

    // Pipe / interactive mode.
    let interactive = std::io::stdin().is_terminal();
    if interactive {
        println!("KesselDB shell — connected to {addr}.  \\q to quit.");
        print!("kessel> ");
        let _ = std::io::stdout().flush();
    }
    let stdin = std::io::stdin();
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        let s = line.trim();
        if !s.is_empty() && !s.starts_with('#') && !s.starts_with("--") {
            if matches!(s, "quit" | "exit" | "\\q") {
                break;
            }
            run_one(&mut client, s);
        }
        if interactive {
            print!("kessel> ");
            let _ = std::io::stdout().flush();
        }
    }
    if interactive {
        println!();
    }
}

/// Run one statement, print the formatted result, return the process exit
/// code it implies (0 ok, 1 errored) — so one-shot callers and agents get
/// a reliable signal without parsing text.
fn run_one(client: &mut Client, sql: &str) -> i32 {
    match client.sql(sql) {
        Ok(OpResult::Got(b)) => {
            // EXPLAIN returns plain plan text — print it as text.
            if sql.trim_start().get(..7).map_or(false, |k| {
                k.eq_ignore_ascii_case("EXPLAIN")
            }) {
                println!("{}", String::from_utf8_lossy(&b));
                return 0;
            }
            // Whole-row single-table SELECT → decode & print real columns
            // (best-DX path). Falls back cleanly if it isn't one or the
            // schema/rows don't decode.
            if let Some(t) = kessel_sql::select_star_table(sql) {
                if let Ok(OpResult::Got(def)) =
                    client.sql(&format!("DESCRIBE {t}"))
                {
                    if let Some(table) = render_rows(&def, &b) {
                        println!("{table}");
                        return 0;
                    }
                }
            } else if let Some((t, cols)) = kessel_sql::select_columns(sql) {
                // Projection: decode the column-oriented result.
                if let Ok(OpResult::Got(def)) =
                    client.sql(&format!("DESCRIBE {t}"))
                {
                    if let Some(table) =
                        kessel_client::render_projection(&def, &cols, &b)
                    {
                        println!("{table}");
                        return 0;
                    }
                }
            }
            println!("{}", format_result(&OpResult::Got(b)));
            0
        }
        Ok(r) => {
            println!("{}", format_result(&r));
            match r {
                OpResult::SchemaError(_)
                | OpResult::Unauthorized
                | OpResult::Unavailable => 1,
                _ => 0,
            }
        }
        Err(e) => {
            eprintln!("kessel: I/O error: {e}");
            1
        }
    }
}

fn fail_usage(msg: &str) -> ! {
    eprintln!("kessel: {msg}\n\n{HELP}");
    std::process::exit(2);
}
