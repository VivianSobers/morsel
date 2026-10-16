use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use anyhow::{bail, Result};
use morsel_core::{Batch, Column, Schema};
use morsel_plan::LogicalPlan;

use crate::catalog::Catalog;
use crate::pipeline::{execute_streaming_batch, PipelineOp};
use crate::scheduler::create_morsels;

pub fn apply_limit(batches: Vec<Batch>, n: usize) -> Vec<Batch> {
    let mut remaining = n;
    let mut result = Vec::new();
    for batch in batches {
        if remaining == 0 {
            break;
        }
        if batch.len <= remaining {
            remaining -= batch.len;
            result.push(batch);
        } else {
            result.push(batch.slice(0, remaining));
            break;
        }
    }
    result
}

pub fn execute(plan: &LogicalPlan, catalog: &Catalog, threads: usize) -> Result<Vec<Batch>> {
    match plan {
        LogicalPlan::Limit { input, n } => {
            if let LogicalPlan::Sort {
                input: sort_input,
                keys,
            } = input.as_ref()
            {
                crate::sort::execute_sort(sort_input, keys, catalog, threads, Some(*n))
            } else {
                let batches = execute(input, catalog, threads)?;
                Ok(apply_limit(batches, *n))
            }
        }
        LogicalPlan::Scan { .. } | LogicalPlan::Filter { .. } | LogicalPlan::Project { .. } => {
            execute_scan_pipeline(plan, catalog, threads)
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggs,
        } => crate::aggregate::execute_aggregate(input, group_by, aggs, catalog, threads),
        LogicalPlan::Join {
            left,
            right,
            join_type,
            on,
            filter,
        } => crate::join::execute_join(
            left,
            right,
            *join_type,
            on,
            filter.as_ref(),
            catalog,
            threads,
        ),
        LogicalPlan::CrossJoin { left, right } => {
            crate::join::execute_cross_join(left, right, catalog, threads)
        }
        LogicalPlan::Sort { input, keys } => {
            crate::sort::execute_sort(input, keys, catalog, threads, None)
        }
    }
}

fn extract_scan_pipeline<'a>(
    plan: &'a LogicalPlan,
    ops: &mut Vec<PipelineOp>,
) -> Result<(&'a str, &'a [usize], &'a Schema)> {
    match plan {
        LogicalPlan::Project { input, exprs } => {
            let schema = Arc::new(plan.schema()?);
            ops.push(PipelineOp::Project {
                exprs: exprs.clone(),
                schema,
            });
            extract_scan_pipeline(input, ops)
        }
        LogicalPlan::Filter { input, predicate } => {
            ops.push(PipelineOp::Filter(predicate.clone()));
            extract_scan_pipeline(input, ops)
        }
        LogicalPlan::Scan {
            table,
            columns,
            table_schema,
        } => Ok((table.as_str(), columns.as_slice(), table_schema)),
        other => bail!("unsupported operator in scan pipeline: {:?}", other),
    }
}

fn execute_scan_pipeline(
    plan: &LogicalPlan,
    catalog: &Catalog,
    threads: usize,
) -> Result<Vec<Batch>> {
    let mut ops = Vec::new();
    let (table_name, columns, _table_schema) = extract_scan_pipeline(plan, &mut ops)?;
    ops.reverse();

    let table = catalog
        .get_table(table_name)
        .ok_or_else(|| anyhow::anyhow!("table not found: {}", table_name))?;

    let scan_fields = columns
        .iter()
        .map(|&idx| table.schema.fields[idx].clone())
        .collect();
    let scan_schema = Arc::new(Schema {
        fields: scan_fields,
    });

    let morsels = create_morsels(&table.batches);
    if morsels.is_empty() {
        return Ok(Vec::new());
    }

    let num_morsels = morsels.len();
    let num_threads = threads.max(1);
    let queue = Arc::new(Mutex::new(VecDeque::from(morsels)));
    let results = Arc::new(Mutex::new(vec![Vec::new(); num_morsels]));
    let error_cell = Arc::new(Mutex::new(None));

    std::thread::scope(|s| {
        for _ in 0..num_threads {
            let q = Arc::clone(&queue);
            let res = Arc::clone(&results);
            let err_cell = Arc::clone(&error_cell);
            let ops_ref = &ops;
            let scan_schema_ref = &scan_schema;

            s.spawn(move || loop {
                if err_cell.lock().unwrap().is_some() {
                    break;
                }
                let morsel = {
                    let mut locked_q = q.lock().unwrap();
                    locked_q.pop_front()
                };
                let morsel = match morsel {
                    Some(m) => m,
                    None => break,
                };

                let mut morsel_results = Vec::new();
                for base_batch in &morsel.batches {
                    let scan_cols: Vec<Arc<Column>> = columns
                        .iter()
                        .map(|&idx| Arc::clone(&base_batch.columns[idx]))
                        .collect();
                    let scan_batch = Batch::new(Arc::clone(scan_schema_ref), scan_cols);
                    match execute_streaming_batch(scan_batch, ops_ref) {
                        Ok(Some(out_batch)) => {
                            morsel_results.push(out_batch);
                        }
                        Ok(None) => {}
                        Err(e) => {
                            let mut locked_err = err_cell.lock().unwrap();
                            if locked_err.is_none() {
                                *locked_err = Some(e);
                            }
                            break;
                        }
                    }
                }
                let mut locked_res = res.lock().unwrap();
                locked_res[morsel.id] = morsel_results;
            });
        }
    });

    if let Some(err) = Arc::try_unwrap(error_cell).unwrap().into_inner().unwrap() {
        return Err(err);
    }

    let all_results: Vec<Batch> = Arc::try_unwrap(results)
        .unwrap()
        .into_inner()
        .unwrap()
        .into_iter()
        .flatten()
        .collect();

    Ok(all_results)
}
