#![forbid(unsafe_code)]

use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use morsel_exec::catalog::load_parquet_dir;
use morsel_fuzz::drop::DropMask;
use morsel_fuzz::generator::generate_plan;
use morsel_fuzz::runner::run_seed;
use morsel_fuzz::sampler::ValueSampler;

fn parse_seed_range(s: &str) -> Result<(u64, u64)> {
    if let Some((start_str, end_str)) = s.split_once("..") {
        let start: u64 = start_str
            .parse()
            .with_context(|| format!("invalid start seed: {}", start_str))?;
        let end: u64 = end_str
            .parse()
            .with_context(|| format!("invalid end seed: {}", end_str))?;
        Ok((start, end))
    } else {
        let seed: u64 = s
            .parse()
            .with_context(|| format!("invalid single seed: {}", s))?;
        Ok((seed, seed + 1))
    }
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!(
            "Usage: morsel-fuzz run --data DIR --seeds START..END --out DIR --threads T [--drop MASK]"
        );
        eprintln!("       morsel-fuzz list-drops --data DIR --seed SEED");
        std::process::exit(1);
    }

    match args[1].as_str() {
        "run" => {
            let mut data_dir: Option<PathBuf> = None;
            let mut seeds_str: Option<String> = None;
            let mut out_dir: Option<PathBuf> = None;
            let mut threads: usize = 1;
            let mut drop_mask = DropMask::default();

            let mut i = 2;
            while i < args.len() {
                match args[i].as_str() {
                    "--data" => {
                        i += 1;
                        if i < args.len() {
                            data_dir = Some(PathBuf::from(&args[i]));
                        }
                    }
                    "--seeds" => {
                        i += 1;
                        if i < args.len() {
                            seeds_str = Some(args[i].clone());
                        }
                    }
                    "--out" => {
                        i += 1;
                        if i < args.len() {
                            out_dir = Some(PathBuf::from(&args[i]));
                        }
                    }
                    "--threads" => {
                        i += 1;
                        if i < args.len() {
                            threads = args[i].parse().unwrap_or(1);
                        }
                    }
                    "--drop" => {
                        i += 1;
                        if i < args.len() {
                            drop_mask = DropMask::parse(&args[i]);
                        }
                    }
                    other => {
                        bail!("unknown argument: {}", other);
                    }
                }
                i += 1;
            }

            let data_dir = data_dir.ok_or_else(|| anyhow::anyhow!("missing --data"))?;
            let seeds_str = seeds_str.ok_or_else(|| anyhow::anyhow!("missing --seeds"))?;
            let out_dir = out_dir.ok_or_else(|| anyhow::anyhow!("missing --out"))?;

            std::fs::create_dir_all(&out_dir)?;

            let (start_seed, end_seed) = parse_seed_range(&seeds_str)?;

            // Load catalog once
            let catalog = load_parquet_dir(&data_dir, threads)?;
            let sampler = ValueSampler::new(&catalog);

            for seed in start_seed..end_seed {
                run_seed(seed, &catalog, &sampler, &drop_mask, &out_dir, threads)?;
            }
        }
        "list-drops" => {
            let mut data_dir: Option<PathBuf> = None;
            let mut seed: Option<u64> = None;

            let mut i = 2;
            while i < args.len() {
                match args[i].as_str() {
                    "--data" => {
                        i += 1;
                        if i < args.len() {
                            data_dir = Some(PathBuf::from(&args[i]));
                        }
                    }
                    "--seed" => {
                        i += 1;
                        if i < args.len() {
                            seed = Some(args[i].parse()?);
                        }
                    }
                    other => {
                        bail!("unknown argument: {}", other);
                    }
                }
                i += 1;
            }

            let data_dir = data_dir.ok_or_else(|| anyhow::anyhow!("missing --data"))?;
            let seed = seed.ok_or_else(|| anyhow::anyhow!("missing --seed"))?;

            let catalog = load_parquet_dir(&data_dir, 1)?;
            let sampler = ValueSampler::new(&catalog);
            let drop_mask = DropMask::default();

            let plan = generate_plan(seed, &catalog, &sampler, &drop_mask)?;
            println!("{}", plan.droppable_pieces.join(","));
        }
        other => {
            bail!("unknown command: {}", other);
        }
    }

    Ok(())
}
