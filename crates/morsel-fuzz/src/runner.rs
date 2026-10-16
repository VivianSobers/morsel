use std::path::Path;

use anyhow::Result;
use morsel_exec::catalog::Catalog;
use morsel_exec::execute::execute;

use crate::csv::write_csv;
use crate::drop::DropMask;
use crate::generator::generate_plan;
use crate::sampler::ValueSampler;

pub fn run_seed(
    seed: u64,
    catalog: &Catalog,
    sampler: &ValueSampler,
    drop_mask: &DropMask,
    out_dir: &Path,
    threads: usize,
) -> Result<()> {
    let fuzz_plan = generate_plan(seed, catalog, sampler, drop_mask)?;

    let sql_path = out_dir.join(format!("{}.sql", seed));
    let txt_path = out_dir.join(format!("{}.txt", seed));
    let shape_path = out_dir.join(format!("{}.shape", seed));
    let csv_path = out_dir.join(format!("{}.csv", seed));
    let err_path = out_dir.join(format!("{}.err", seed));

    std::fs::write(&sql_path, &fuzz_plan.sql)?;
    std::fs::write(&txt_path, format!("{:#?}", fuzz_plan.plan))?;
    std::fs::write(&shape_path, &fuzz_plan.shape)?;

    // Remove any leftover csv/err files for this seed
    let _ = std::fs::remove_file(&csv_path);
    let _ = std::fs::remove_file(&err_path);

    let plan_schema = match fuzz_plan.plan.schema() {
        Ok(s) => s,
        Err(e) => {
            std::fs::write(&err_path, format!("Schema error: {:#}", e))?;
            return Ok(());
        }
    };

    let exec_res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        execute(&fuzz_plan.plan, catalog, threads)
    }));

    match exec_res {
        Ok(Ok(batches)) => {
            write_csv(&batches, &plan_schema, &csv_path)?;
        }
        Ok(Err(e)) => {
            std::fs::write(&err_path, format!("Engine error: {:#}", e))?;
        }
        Err(payload) => {
            let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "Panic with unknown payload".to_string()
            };
            std::fs::write(&err_path, format!("Panic: {}", msg))?;
        }
    }

    Ok(())
}
