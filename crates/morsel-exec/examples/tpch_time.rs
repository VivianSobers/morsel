#![allow(dead_code, unused_imports)]

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use morsel_exec::catalog::{load_parquet_dir, Catalog};
use morsel_exec::execute::execute;
use morsel_plan::LogicalPlan;

#[path = "../tests/tpch_hand.rs"]
mod tpch_hand;

fn get_commit() -> String {
    Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn get_hostname() -> String {
    Command::new("hostname")
        .output()
        .ok()
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "localhost".to_string())
}

fn build_query_plan(query_nr: usize, catalog: &Catalog) -> LogicalPlan {
    match query_nr {
        1 => tpch_hand::plan_q01(catalog),
        3 => tpch_hand::plan_q03(catalog),
        4 => tpch_hand::plan_q04(catalog),
        5 => tpch_hand::plan_q05(catalog),
        6 => tpch_hand::plan_q06(catalog),
        10 => tpch_hand::plan_q10(catalog),
        12 => tpch_hand::plan_q12(catalog),
        13 => tpch_hand::plan_q13(catalog),
        14 => tpch_hand::plan_q14(catalog),
        18 => tpch_hand::plan_q18(catalog),
        other => panic!("unsupported hand-built TPC-H query: Q{}", other),
    }
}

fn parse_query_str(s: &str) -> Vec<usize> {
    let s = s.trim().to_lowercase();
    if s == "all" {
        return vec![1, 3, 4, 5, 6, 10, 12, 13, 14, 18];
    }
    let num_str = s.strip_prefix('q').unwrap_or(&s);
    let q: usize = num_str.parse().expect("valid query number");
    vec![q]
}

fn main() {
    let mut args = std::env::args().skip(1);
    let mut query_opt: Option<String> = None;
    let mut runs: usize = 3;
    let mut threads: usize = 16;
    let mut data_dir_opt: Option<PathBuf> = None;
    let mut sf_opt: Option<f64> = None;
    let mut out_path_opt: Option<PathBuf> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--query" | "-q" => {
                query_opt = args.next();
            }
            "--runs" | "-r" => {
                runs = args
                    .next()
                    .expect("number for --runs")
                    .parse()
                    .expect("integer runs");
            }
            "--threads" | "-t" => {
                threads = args
                    .next()
                    .expect("number for --threads")
                    .parse()
                    .expect("integer threads");
            }
            "--data-dir" | "-d" => {
                data_dir_opt = args.next().map(PathBuf::from);
            }
            "--sf" => {
                sf_opt = args.next().map(|s| s.parse().expect("float for --sf"));
            }
            "--out" | "-o" => {
                out_path_opt = args.next().map(PathBuf::from);
            }
            other => panic!("unknown argument: {}", other),
        }
    }

    let data_dir = data_dir_opt.unwrap_or_else(|| {
        std::env::var("MORSEL_TPCH_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/dev/shm/morsel/tpch/sf10"))
    });

    let sf = sf_opt.unwrap_or_else(|| {
        let dir_str = data_dir.to_string_lossy();
        if dir_str.contains("sf10") {
            10.0
        } else {
            1.0
        }
    });

    let queries = match query_opt {
        Some(s) => parse_query_str(&s),
        None => vec![1, 3, 4, 5, 6, 10, 12, 13, 14, 18],
    };

    let commit = get_commit();
    let hostname = get_hostname();

    eprintln!(
        "Loading Parquet data from {:?} with {} threads...",
        data_dir, threads
    );
    let catalog = load_parquet_dir(&data_dir, threads).expect("load parquet dir");
    eprintln!("Data loaded. Running benchmarks...");

    for &q in &queries {
        let plan = build_query_plan(q, &catalog);
        for run_idx in 0..runs {
            let start = Instant::now();
            let batches = execute(&plan, &catalog, threads).expect("execute query");
            let elapsed = start.elapsed();
            let ms = elapsed.as_secs_f64() * 1000.0;
            let rows: usize = batches.iter().map(|b| b.len).sum();

            let line = format!(
                "{{\"query\":{},\"sf\":{},\"threads\":{},\"run\":{},\"ms\":{:.2},\"commit\":\"{}\",\"hostname\":\"{}\"}}",
                q, sf, threads, run_idx, ms, commit, hostname
            );
            println!("{}", line);

            if let Some(ref out_path) = out_path_opt {
                if let Some(parent) = out_path.parent() {
                    std::fs::create_dir_all(parent).ok();
                }
                let mut f = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(out_path)
                    .expect("open out file");
                writeln!(f, "{}", line).expect("write line to out file");
            }
            eprintln!(
                "  Q{:02} run {}: {:.2} ms ({} rows emitted)",
                q, run_idx, ms, rows
            );
        }
    }
}
