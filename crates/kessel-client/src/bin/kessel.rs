//! `kessel` — the KesselDB command-line client.
//!
//! Designed for both humans and agents: deterministic, line-oriented,
//! scriptable, with meaningful exit codes and an optional `--json` mode.
//!
//! Usage:
//!   kessel [--addr HOST:PORT] [--token TOKEN] [--json] ["SQL STATEMENT"]
//!
//!   * with a SQL argument  -> one-shot: run it, print the result, exit
//!     (exit code 1 if the statement errored, so scripts/agents can tell).
//!   * without one          -> read statements from stdin, one per line
//!     (interactive prompt if a TTY; otherwise a clean pipe consumer).
//!
//! Examples:
//!   kessel "CREATE TABLE t (v U64 NOT NULL)"
//!   kessel --addr 10.0.0.1:7878 "SELECT SUM(v) FROM t"
//!   kessel --json "SELECT * FROM t"          # machine-readable
//!   echo "SELECT * FROM t ID 1" | kessel     # pipe a .sql file
//!   kessel --token s3cret                    # interactive shell
//!
//! Lines beginning with `#` or `--` are comments. In the shell, `\?`
//! lists meta-commands, `\d <table>` describes a table, `\timing`
//! toggles query timing, and `\q` (or `quit`/`exit`) ends the session.
//! Zero external dependencies.

use kessel_client::{
    format_result, format_result_json, render_projection, render_rows,
    render_rows_json, render_schema, render_schema_json,
    render_typed_result, render_typed_result_json, Client,
};
use kessel_proto::OpResult;
use std::io::{BufRead, IsTerminal, Write};
use std::time::Instant;

const HELP: &str = "\
kessel — KesselDB CLI

USAGE:
  kessel [--addr HOST:PORT] [--token TOKEN] [--json] [\"SQL\"]

OPTIONS:
  --addr  HOST:PORT   server address (default 127.0.0.1:7878)
  --token TOKEN       shared-secret token, if the server requires auth
  --json              emit one JSON object per statement (for agents)
  -h, --help          show this help

MODES:
  one-shot   pass a SQL string as the final argument
  pipe       no SQL arg + piped stdin: one statement per line
  shell      no SQL arg + a TTY: interactive prompt (\\? for commands)

EXIT CODES:
  0  success    1  statement error / connection failure   2  bad usage
";

const META: &str = "\
shell commands:
  \\?  \\h  \\help     this list
  \\d <table>         describe a table (columns, types, indexes)
  \\timing            toggle per-statement timing on/off
  \\q  quit  exit     leave the shell
anything else is sent as SQL.
";

struct Opts {
    json: bool,
    timing: bool,
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut addr = "127.0.0.1:7878".to_string();
    let mut token: Option<String> = None;
    let mut sql_parts: Vec<String> = Vec::new();
    let mut opts = Opts { json: false, timing: false };
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "-h" | "--help" => {
                print!("{HELP}");
                return;
            }
            "--json" => opts.json = true,
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
        eprintln!(
            "  hint: is a server running? start one with:\n        \
             cargo run --release --bin kesseldb -- {addr} ./data"
        );
        std::process::exit(1);
    });

    // One-shot mode.
    if !sql_parts.is_empty() {
        let stmt = sql_parts.join(" ");
        std::process::exit(run_one(&mut client, &stmt, &opts));
    }

    // Pipe / interactive mode.
    let interactive = std::io::stdin().is_terminal();
    if interactive {
        println!(
            "KesselDB shell — connected to {addr}.  \\? for commands, \
             \\q to quit."
        );
        prompt();
    }
    let stdin = std::io::stdin();
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        let s = line.trim();
        if s.is_empty() || s.starts_with('#') || s.starts_with("--") {
            if interactive {
                prompt();
            }
            continue;
        }
        if matches!(s, "quit" | "exit" | "\\q") {
            break;
        }
        // Meta-commands (shell ergonomics; not sent as SQL).
        if let Some(rest) = s.strip_prefix('\\') {
            handle_meta(rest.trim(), &mut client, &mut opts);
        } else {
            run_one(&mut client, s, &opts);
        }
        if interactive {
            prompt();
        }
    }
    if interactive {
        println!();
    }
}

fn prompt() {
    print!("kessel> ");
    let _ = std::io::stdout().flush();
}

/// Backslash meta-commands. Kept to what existing server ops back, so
/// nothing here is a half-working illusion.
fn handle_meta(cmd: &str, client: &mut Client, opts: &mut Opts) {
    let (head, arg) = match cmd.split_once(char::is_whitespace) {
        Some((h, a)) => (h, a.trim()),
        None => (cmd, ""),
    };
    match head {
        "" | "?" | "h" | "help" => print!("{META}"),
        "timing" => {
            opts.timing = !opts.timing;
            println!("timing {}", if opts.timing { "on" } else { "off" });
        }
        "d" => {
            if arg.is_empty() {
                eprintln!("usage: \\d <table>");
            } else {
                run_one(client, &format!("DESCRIBE {arg}"), opts);
            }
        }
        other => eprintln!(
            "unknown command \\{other} — \\? for the list (or send SQL \
             without a leading backslash)"
        ),
    }
}

/// Run one statement, print the result in the chosen format, return the
/// process exit code it implies (0 ok, 1 errored) — so one-shot callers
/// and agents get a reliable signal without parsing prose.
fn run_one(client: &mut Client, sql: &str, opts: &Opts) -> i32 {
    let t0 = Instant::now();
    let code = match client.sql(sql) {
        Ok(OpResult::Got(b)) => {
            let is_explain = sql
                .trim_start()
                .get(..7)
                .map_or(false, |k| k.eq_ignore_ascii_case("EXPLAIN"));
            if opts.json {
                print_got_json(client, sql, &b, is_explain);
                0
            } else {
                print_got_text(client, sql, b, is_explain);
                0
            }
        }
        Ok(r) => {
            if opts.json {
                println!("{}", format_result_json(&r));
            } else {
                println!("{}", format_result(&r));
            }
            match r {
                OpResult::SchemaError(_)
                | OpResult::Unauthorized
                | OpResult::Unavailable => 1,
                _ => 0,
            }
        }
        Err(e) => {
            if opts.json {
                println!(
                    r#"{{"status":"error","message":"I/O: {}"}}"#,
                    e.to_string().replace('"', "'")
                );
            } else {
                eprintln!("kessel: I/O error: {e}");
            }
            1
        }
    };
    if opts.timing && !opts.json {
        let us = t0.elapsed().as_micros();
        if us >= 1000 {
            println!("time: {:.3} ms", us as f64 / 1000.0);
        } else {
            println!("time: {us} µs");
        }
    }
    code
}

/// Text mode: decode whole-row / projection SELECTs into aligned tables,
/// EXPLAIN into its plan text, everything else into a one-line summary.
fn print_got_text(client: &mut Client, sql: &str, b: Vec<u8>, explain: bool) {
    if explain {
        println!("{}", String::from_utf8_lossy(&b));
        return;
    }
    if is_describe(sql) {
        if let Some(s) = render_schema(&b) {
            println!("{s}");
            return;
        }
    }
    // Self-describing typed result (JOINs, …) — renders generically.
    if let Some(s) = render_typed_result(&b) {
        println!("{s}");
        return;
    }
    if let Some(t) = kessel_sql::select_star_table(sql) {
        if let Ok(OpResult::Got(def)) = client.sql(&format!("DESCRIBE {t}")) {
            if let Some(table) = render_rows(&def, &b) {
                println!("{table}");
                return;
            }
        }
    } else if let Some((t, cols)) = kessel_sql::select_columns(sql) {
        if let Ok(OpResult::Got(def)) = client.sql(&format!("DESCRIBE {t}")) {
            if let Some(table) = render_projection(&def, &cols, &b) {
                println!("{table}");
                return;
            }
        }
    }
    println!("{}", format_result(&OpResult::Got(b)));
}

/// JSON mode: a single object per statement. Whole-row SELECT* gets a
/// real `rows` array; EXPLAIN gets its `plan`; otherwise the stable
/// scalar/status object.
fn print_got_json(client: &mut Client, sql: &str, b: &[u8], explain: bool) {
    if explain {
        let plan = String::from_utf8_lossy(b).replace('"', "'");
        println!(r#"{{"status":"ok","plan":"{plan}"}}"#);
        return;
    }
    if is_describe(sql) {
        if let Some(s) = render_schema_json(b) {
            println!("{s}");
            return;
        }
    }
    if let Some(rows) = render_typed_result_json(b) {
        println!(r#"{{"status":"ok","rows":{rows}}}"#);
        return;
    }
    if let Some(t) = kessel_sql::select_star_table(sql) {
        if let Ok(OpResult::Got(def)) = client.sql(&format!("DESCRIBE {t}")) {
            if let Some(rows) = render_rows_json(&def, b) {
                println!(r#"{{"status":"ok","rows":{rows}}}"#);
                return;
            }
        }
    }
    println!("{}", format_result_json(&OpResult::Got(b.to_vec())));
}

fn is_describe(sql: &str) -> bool {
    sql.trim_start()
        .get(..8)
        .map_or(false, |k| k.eq_ignore_ascii_case("DESCRIBE"))
}

fn fail_usage(msg: &str) -> ! {
    eprintln!("kessel: {msg}\n\n{HELP}");
    std::process::exit(2);
}
