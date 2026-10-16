#![forbid(unsafe_code)]

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;

use morsel_core::{Batch, Scalar, Schema};
use serde::Serialize;

#[derive(Serialize)]
struct TimingLine<'a> {
    load_ms: f64,
    plan_ms: f64,
    execute_ms: f64,
    rows: usize,
    threads: usize,
    query_file: &'a str,
    data_dir: &'a str,
    git_commit: &'a str,
    hostname: &'a str,
}

fn error_exit(msg: &str) -> ! {
    let single_line = msg.replace(['\r', '\n'], " ");
    eprintln!("{}", single_line.trim());
    std::process::exit(1);
}

fn get_hostname() -> String {
    if let Ok(h) = std::env::var("HOSTNAME") {
        if !h.trim().is_empty() {
            return h.trim().to_string();
        }
    }
    if let Ok(h) = std::fs::read_to_string("/etc/hostname") {
        if !h.trim().is_empty() {
            return h.trim().to_string();
        }
    }
    if let Ok(out) = std::process::Command::new("hostname").output() {
        let h = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if !h.is_empty() {
            return h;
        }
    }
    "unknown".to_string()
}

fn get_git_commit() -> String {
    let commit = env!("MORSEL_GIT_COMMIT");
    if commit != "unknown" && !commit.is_empty() {
        return commit.to_string();
    }
    if let Ok(out) = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
    {
        let c = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if !c.is_empty() {
            return c;
        }
    }
    "unknown".to_string()
}

fn run_stage<F, R>(stage: &str, f: F) -> R
where
    F: FnOnce() -> anyhow::Result<R> + std::panic::UnwindSafe,
{
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    std::panic::set_hook(prev_hook);

    match res {
        Ok(Ok(val)) => val,
        Ok(Err(err)) => {
            error_exit(&format!("{} error: {}", stage, err));
        }
        Err(payload) => {
            let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                (*s).to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "unknown panic".to_string()
            };
            error_exit(&format!("{} panic: {}", stage, msg));
        }
    }
}

fn escape_csv(s: &str) -> String {
    if s.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn scalar_to_csv(s: &Scalar) -> String {
    match s {
        Scalar::Null => String::new(),
        Scalar::Bool(b) => b.to_string(),
        Scalar::Int32(x) => x.to_string(),
        Scalar::Int64(x) => x.to_string(),
        Scalar::Float64(f) => f.to_string(),
        Scalar::Decimal(v, scale) => {
            if *scale == 0 {
                v.to_string()
            } else {
                let sign = if *v < 0 { "-" } else { "" };
                let v_abs = v.abs();
                let div = 10i128.pow(*scale as u32);
                let whole = v_abs / div;
                let frac = v_abs % div;
                format!(
                    "{}{}.{:0width$}",
                    sign,
                    whole,
                    frac,
                    width = *scale as usize
                )
            }
        }
        Scalar::Date32(days) => {
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            if let Some(d) = epoch.checked_add_signed(chrono::Duration::days(*days as i64)) {
                d.format("%Y-%m-%d").to_string()
            } else {
                days.to_string()
            }
        }
        Scalar::Utf8(st) => escape_csv(st),
    }
}

fn write_csv_output<W: Write>(
    schema: &Schema,
    batches: &[Batch],
    writer: &mut W,
) -> std::io::Result<()> {
    let header = schema
        .fields
        .iter()
        .map(|f| escape_csv(&f.name))
        .collect::<Vec<_>>()
        .join(",");
    writeln!(writer, "{}", header)?;

    for batch in batches {
        for row_idx in 0..batch.len {
            let mut row = Vec::with_capacity(batch.columns.len());
            for col in &batch.columns {
                let scalar = col.get(row_idx);
                row.push(scalar_to_csv(&scalar));
            }
            writeln!(writer, "{}", row.join(","))?;
        }
    }
    writer.flush()?;
    Ok(())
}

fn append_timing_line(path: &Path, timing: &TimingLine) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            let _ = std::fs::create_dir_all(parent);
        }
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    let line = serde_json::to_string(timing)?;
    writeln!(file, "{}", line)?;
    Ok(())
}

fn print_plan(plan: &morsel_plan::LogicalPlan, indent: usize) {
    let pad = "  ".repeat(indent);
    match plan {
        morsel_plan::LogicalPlan::Scan { table, columns, .. } => {
            println!("{}Scan: {} cols={:?}", pad, table, columns);
        }
        morsel_plan::LogicalPlan::Filter { input, predicate } => {
            println!("{}Filter: {:?}", pad, predicate);
            print_plan(input, indent + 1);
        }
        morsel_plan::LogicalPlan::Project { input, exprs } => {
            let names: Vec<&str> = exprs.iter().map(|(_, name)| name.as_str()).collect();
            println!("{}Project: {:?}", pad, names);
            print_plan(input, indent + 1);
        }
        morsel_plan::LogicalPlan::Aggregate {
            input,
            group_by,
            aggs,
        } => {
            let gb_names: Vec<&str> = group_by.iter().map(|(_, name)| name.as_str()).collect();
            let agg_names: Vec<&str> = aggs.iter().map(|(_, name)| name.as_str()).collect();
            println!(
                "{}Aggregate: group_by={:?} aggs={:?}",
                pad, gb_names, agg_names
            );
            print_plan(input, indent + 1);
        }
        morsel_plan::LogicalPlan::Join {
            left,
            right,
            join_type,
            on,
            filter,
        } => {
            println!(
                "{}Join: {:?} on={:?} filter={:?}",
                pad, join_type, on, filter
            );
            print_plan(left, indent + 1);
            print_plan(right, indent + 1);
        }
        morsel_plan::LogicalPlan::CrossJoin { left, right } => {
            println!("{}CrossJoin", pad);
            print_plan(left, indent + 1);
            print_plan(right, indent + 1);
        }
        morsel_plan::LogicalPlan::Sort { input, keys } => {
            println!("{}Sort: {} keys", pad, keys.len());
            print_plan(input, indent + 1);
        }
        morsel_plan::LogicalPlan::Limit { input, n } => {
            println!("{}Limit: {}", pad, n);
            print_plan(input, indent + 1);
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        error_exit(
            "Usage: morsel query --data DIR --sql-file FILE [--threads T] [--out FILE.csv] [--repeat N] [--timing-json FILE] [--explain]",
        );
    }

    if args[1] != "query" {
        error_exit(&format!("unknown subcommand: {}", args[1]));
    }

    let mut data_dir: Option<PathBuf> = None;
    let mut sql_file: Option<PathBuf> = None;
    let mut threads: Option<usize> = None;
    let mut out_file: Option<PathBuf> = None;
    let mut repeat: usize = 1;
    let mut timing_json: Option<PathBuf> = None;
    let mut explain = false;

    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--explain" => {
                explain = true;
            }
            "--data" => {
                i += 1;
                if i >= args.len() {
                    error_exit("missing value for --data");
                }
                data_dir = Some(PathBuf::from(&args[i]));
            }
            "--sql-file" => {
                i += 1;
                if i >= args.len() {
                    error_exit("missing value for --sql-file");
                }
                sql_file = Some(PathBuf::from(&args[i]));
            }
            "--threads" => {
                i += 1;
                if i >= args.len() {
                    error_exit("missing value for --threads");
                }
                let t = args[i]
                    .parse::<usize>()
                    .unwrap_or_else(|_| error_exit("invalid value for --threads"));
                threads = Some(t);
            }
            "--out" => {
                i += 1;
                if i >= args.len() {
                    error_exit("missing value for --out");
                }
                out_file = Some(PathBuf::from(&args[i]));
            }
            "--repeat" => {
                i += 1;
                if i >= args.len() {
                    error_exit("missing value for --repeat");
                }
                let r = args[i]
                    .parse::<usize>()
                    .unwrap_or_else(|_| error_exit("invalid value for --repeat"));
                if r == 0 {
                    error_exit("--repeat must be at least 1");
                }
                repeat = r;
            }
            "--timing-json" => {
                i += 1;
                if i >= args.len() {
                    error_exit("missing value for --timing-json");
                }
                timing_json = Some(PathBuf::from(&args[i]));
            }
            other => {
                error_exit(&format!("unknown argument: {}", other));
            }
        }
        i += 1;
    }

    let data_path = match data_dir {
        Some(p) => p,
        None => error_exit("missing required argument: --data"),
    };
    let sql_path = match sql_file {
        Some(p) => p,
        None => error_exit("missing required argument: --sql-file"),
    };

    if !data_path.exists() {
        error_exit(&format!(
            "load error: data directory '{}' does not exist",
            data_path.display()
        ));
    }

    let threads = threads
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        })
        .max(1);

    let sql_str = match std::fs::read_to_string(&sql_path) {
        Ok(s) => s,
        Err(e) => error_exit(&format!(
            "error: failed to read sql file '{}': {}",
            sql_path.display(),
            e
        )),
    };

    let hostname = get_hostname();
    let git_commit = get_git_commit();

    let load_start = Instant::now();
    let catalog = match morsel_exec::catalog::load_parquet_dir(&data_path, threads) {
        Ok(c) => c,
        Err(e) => error_exit(&format!("load error: {}", e)),
    };
    let load_ms = load_start.elapsed().as_secs_f64() * 1000.0;

    let stats: std::collections::HashMap<String, usize> = catalog
        .tables
        .iter()
        .map(|(k, v)| (k.clone(), v.rows))
        .collect();

    if explain {
        let plan = run_stage("plan", || {
            let unopt = morsel_sql::plan_sql(&sql_str, &catalog)?;
            morsel_sql::optimize(unopt, &stats)
        });
        print_plan(&plan, 0);
        std::process::exit(0);
    }

    let mut last_result: Option<(Schema, Vec<Batch>)> = None;

    for _ in 0..repeat {
        let plan_start = Instant::now();
        let plan = run_stage("plan", || {
            let unopt = morsel_sql::plan_sql(&sql_str, &catalog)?;
            morsel_sql::optimize(unopt, &stats)
        });
        let plan_ms = plan_start.elapsed().as_secs_f64() * 1000.0;

        let exec_start = Instant::now();
        let batches = run_stage("exec", || morsel_exec::execute(&plan, &catalog, threads));
        let exec_ms = exec_start.elapsed().as_secs_f64() * 1000.0;

        let rows: usize = batches.iter().map(|b| b.len).sum();
        let schema = plan.schema().unwrap_or_else(|_| {
            batches
                .first()
                .map(|b| (*b.schema).clone())
                .unwrap_or_default()
        });

        if let Some(timing_path) = &timing_json {
            let timing_line = TimingLine {
                load_ms,
                plan_ms,
                execute_ms: exec_ms,
                rows,
                threads,
                query_file: &sql_path.to_string_lossy(),
                data_dir: &data_path.to_string_lossy(),
                git_commit: &git_commit,
                hostname: &hostname,
            };
            if let Err(e) = append_timing_line(timing_path, &timing_line) {
                error_exit(&format!("error: failed to write timing json: {}", e));
            }
        }

        last_result = Some((schema, batches));
    }

    let (schema, batches) = match last_result {
        Some(res) => res,
        None => error_exit("error: no execution result produced"),
    };

    if let Some(out_path) = &out_file {
        if let Some(parent) = out_path.parent() {
            if !parent.as_os_str().is_empty() {
                let _ = std::fs::create_dir_all(parent);
            }
        }
        let file = match std::fs::File::create(out_path) {
            Ok(f) => f,
            Err(e) => error_exit(&format!(
                "error: failed to create output file '{}': {}",
                out_path.display(),
                e
            )),
        };
        let mut writer = std::io::BufWriter::new(file);
        if let Err(e) = write_csv_output(&schema, &batches, &mut writer) {
            error_exit(&format!("error: failed to write output file: {}", e));
        }
    } else {
        let stdout = std::io::stdout();
        let mut writer = std::io::BufWriter::new(stdout.lock());
        if let Err(e) = write_csv_output(&schema, &batches, &mut writer) {
            error_exit(&format!("error: failed to write output to stdout: {}", e));
        }
    }
}
