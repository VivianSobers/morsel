use std::sync::Arc;

use anyhow::Result;
use morsel_core::{Batch, Schema};
use morsel_plan::Expr;

use crate::eval::{eval, eval_predicate};

#[derive(Clone, Debug)]
pub enum PipelineOp {
    Filter(Expr),
    Project {
        exprs: Vec<(Expr, String)>,
        schema: Arc<Schema>,
    },
}

pub fn execute_streaming_batch(batch: Batch, ops: &[PipelineOp]) -> Result<Option<Batch>> {
    let mut current = batch;
    for op in ops {
        match op {
            PipelineOp::Filter(pred) => {
                let sel = eval_predicate(pred, &current)?;
                if sel.is_empty() {
                    return Ok(None);
                }
                if sel.len() < current.len {
                    let cols = current
                        .columns
                        .iter()
                        .map(|c| Arc::new(c.take(&sel)))
                        .collect();
                    current = Batch::new(Arc::clone(&current.schema), cols);
                }
            }
            PipelineOp::Project { exprs, schema } => {
                let mut cols = Vec::with_capacity(exprs.len());
                for (expr, _) in exprs {
                    let col = eval(expr, &current)?;
                    cols.push(Arc::new(col));
                }
                current = Batch::new(Arc::clone(schema), cols);
            }
        }
    }
    Ok(Some(current))
}
