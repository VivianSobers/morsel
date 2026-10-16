use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use morsel_core::{Batch, Column, Scalar, Schema, BATCH_ROWS};
use morsel_plan::{Expr, JoinType, LogicalPlan};

use crate::aggregate::{to_key, ScalarKey};
use crate::catalog::Catalog;
use crate::eval::{eval, eval_predicate};
use crate::execute::execute;
use crate::scheduler::{create_morsels, Morsel};

#[derive(Clone, Copy, Debug)]
pub struct RowLoc {
    pub batch_idx: u32,
    pub row_idx: u32,
}

struct BatchBuilder {
    schema: Arc<Schema>,
    columns: Vec<Vec<Scalar>>,
    batches: Vec<Batch>,
}

impl BatchBuilder {
    fn new(schema: Arc<Schema>) -> Self {
        let num_cols = schema.fields.len();
        Self {
            schema,
            columns: vec![Vec::with_capacity(BATCH_ROWS); num_cols],
            batches: Vec::new(),
        }
    }

    fn push_row(&mut self, row: impl Iterator<Item = Scalar>) {
        for (col_idx, val) in row.enumerate() {
            self.columns[col_idx].push(val);
        }
        if self.columns[0].len() >= BATCH_ROWS {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.columns.is_empty() || self.columns[0].is_empty() {
            return;
        }
        let mut cols = Vec::with_capacity(self.schema.fields.len());
        for (i, field) in self.schema.fields.iter().enumerate() {
            let col = Column::from_scalars(field.dtype, &self.columns[i]);
            cols.push(Arc::new(col));
            self.columns[i].clear();
        }
        self.batches
            .push(Batch::new(Arc::clone(&self.schema), cols));
    }

    fn finish(mut self) -> Vec<Batch> {
        self.flush();
        self.batches
    }
}

fn test_filter_pair(
    filter: &Expr,
    left_batch: &Batch,
    l_idx: usize,
    right_batches: &[Batch],
    loc: RowLoc,
    combined_schema: &Arc<Schema>,
) -> Result<bool> {
    let mut cols = Vec::with_capacity(combined_schema.fields.len());
    for col in &left_batch.columns {
        let s = col.get(l_idx);
        cols.push(Arc::new(Column::from_scalars(col.dtype, &[s])));
    }
    for col in &right_batches[loc.batch_idx as usize].columns {
        let s = col.get(loc.row_idx as usize);
        cols.push(Arc::new(Column::from_scalars(col.dtype, &[s])));
    }
    let b = Batch::new(Arc::clone(combined_schema), cols);
    let sel = eval_predicate(filter, &b)?;
    Ok(!sel.is_empty())
}

pub fn execute_join(
    left: &LogicalPlan,
    right: &LogicalPlan,
    join_type: JoinType,
    on: &[(Expr, Expr)],
    filter: Option<&Expr>,
    catalog: &Catalog,
    threads: usize,
) -> Result<Vec<Batch>> {
    let right_batches = execute(right, catalog, threads)?;
    let left_batches = execute(left, catalog, threads)?;
    let left_schema = left.schema()?;
    let right_schema = right.schema()?;

    // Build combined schema for residual filter evaluation
    let mut combined_fields = left_schema.fields.clone();
    combined_fields.extend(right_schema.fields.clone());
    let combined_schema = Arc::new(Schema {
        fields: combined_fields,
    });

    // Build output schema
    let out_schema = Arc::new(match join_type {
        JoinType::Semi | JoinType::Anti => left_schema.clone(),
        JoinType::Inner => Schema {
            fields: {
                let mut f = left_schema.fields.clone();
                f.extend(right_schema.fields.clone());
                f
            },
        },
        JoinType::Left => Schema {
            fields: {
                let mut f = left_schema.fields.clone();
                for mut rf in right_schema.fields.clone() {
                    rf.nullable = true;
                    f.push(rf);
                }
                f
            },
        },
    });

    // Materialize build side hash table
    let mut hash_table: HashMap<Vec<ScalarKey>, Vec<RowLoc>> = HashMap::new();
    for (b_idx, r_batch) in right_batches.iter().enumerate() {
        let r_key_cols: Vec<Column> = on
            .iter()
            .map(|(_, r_expr)| eval(r_expr, r_batch))
            .collect::<Result<Vec<_>>>()?;

        for r_idx in 0..r_batch.len {
            let mut has_null = false;
            let mut key = Vec::with_capacity(r_key_cols.len());
            for col in &r_key_cols {
                if col.is_null(r_idx) {
                    has_null = true;
                    break;
                }
                key.push(to_key(&col.get(r_idx)));
            }
            if !has_null {
                hash_table.entry(key).or_default().push(RowLoc {
                    batch_idx: b_idx as u32,
                    row_idx: r_idx as u32,
                });
            }
        }
    }

    let morsels = create_morsels(&left_batches);
    if morsels.is_empty() {
        return Ok(Vec::new());
    }

    let num_morsels = morsels.len();
    let num_threads = threads.max(1);
    let queue = Arc::new(Mutex::new(VecDeque::from(morsels)));
    let results = Arc::new(Mutex::new(vec![Vec::new(); num_morsels]));
    let error_cell = Arc::new(Mutex::new(None));

    let hash_table = Arc::new(hash_table);
    let right_batches = Arc::new(right_batches);

    std::thread::scope(|s| {
        for _ in 0..num_threads {
            let q = Arc::clone(&queue);
            let res = Arc::clone(&results);
            let err_cell = Arc::clone(&error_cell);
            let ht = Arc::clone(&hash_table);
            let r_batches = Arc::clone(&right_batches);
            let out_sch = Arc::clone(&out_schema);
            let comb_sch = Arc::clone(&combined_schema);
            let l_cols_len = left_schema.fields.len();
            let r_cols_len = right_schema.fields.len();

            s.spawn(move || loop {
                if err_cell.lock().unwrap().is_some() {
                    break;
                }
                let morsel: Option<Morsel> = {
                    let mut locked_q = q.lock().unwrap();
                    locked_q.pop_front()
                };
                let morsel = match morsel {
                    Some(m) => m,
                    None => break,
                };

                let mut builder = BatchBuilder::new(Arc::clone(&out_sch));

                for l_batch in &morsel.batches {
                    let l_key_cols: Vec<Column> = match on
                        .iter()
                        .map(|(l_expr, _)| eval(l_expr, l_batch))
                        .collect::<Result<Vec<_>>>()
                    {
                        Ok(cols) => cols,
                        Err(e) => {
                            let mut locked_err = err_cell.lock().unwrap();
                            if locked_err.is_none() {
                                *locked_err = Some(e);
                            }
                            break;
                        }
                    };

                    for l_idx in 0..l_batch.len {
                        let mut has_null = false;
                        let mut key = Vec::with_capacity(l_key_cols.len());
                        for col in &l_key_cols {
                            if col.is_null(l_idx) {
                                has_null = true;
                                break;
                            }
                            key.push(to_key(&col.get(l_idx)));
                        }

                        let locs: Option<&Vec<RowLoc>> = if has_null { None } else { ht.get(&key) };

                        match join_type {
                            JoinType::Inner => {
                                if let Some(matched_locs) = locs {
                                    for &loc in matched_locs {
                                        if let Some(f) = filter {
                                            match test_filter_pair(
                                                f, l_batch, l_idx, &r_batches, loc, &comb_sch,
                                            ) {
                                                Ok(true) => {}
                                                Ok(false) => continue,
                                                Err(e) => {
                                                    let mut locked_err = err_cell.lock().unwrap();
                                                    if locked_err.is_none() {
                                                        *locked_err = Some(e);
                                                    }
                                                    break;
                                                }
                                            }
                                        }
                                        builder.push_row(
                                            (0..l_cols_len)
                                                .map(|c| l_batch.columns[c].get(l_idx))
                                                .chain((0..r_cols_len).map(|c| {
                                                    r_batches[loc.batch_idx as usize].columns[c]
                                                        .get(loc.row_idx as usize)
                                                })),
                                        );
                                    }
                                }
                            }
                            JoinType::Left => {
                                let mut matched = false;
                                if let Some(matched_locs) = locs {
                                    for &loc in matched_locs {
                                        if let Some(f) = filter {
                                            match test_filter_pair(
                                                f, l_batch, l_idx, &r_batches, loc, &comb_sch,
                                            ) {
                                                Ok(true) => {}
                                                Ok(false) => continue,
                                                Err(e) => {
                                                    let mut locked_err = err_cell.lock().unwrap();
                                                    if locked_err.is_none() {
                                                        *locked_err = Some(e);
                                                    }
                                                    break;
                                                }
                                            }
                                        }
                                        matched = true;
                                        builder.push_row(
                                            (0..l_cols_len)
                                                .map(|c| l_batch.columns[c].get(l_idx))
                                                .chain((0..r_cols_len).map(|c| {
                                                    r_batches[loc.batch_idx as usize].columns[c]
                                                        .get(loc.row_idx as usize)
                                                })),
                                        );
                                    }
                                }
                                if !matched {
                                    builder.push_row(
                                        (0..l_cols_len)
                                            .map(|c| l_batch.columns[c].get(l_idx))
                                            .chain((0..r_cols_len).map(|_| Scalar::Null)),
                                    );
                                }
                            }
                            JoinType::Semi => {
                                let mut matched = false;
                                if let Some(matched_locs) = locs {
                                    if let Some(f) = filter {
                                        for &loc in matched_locs {
                                            match test_filter_pair(
                                                f, l_batch, l_idx, &r_batches, loc, &comb_sch,
                                            ) {
                                                Ok(true) => {
                                                    matched = true;
                                                    break;
                                                }
                                                Ok(false) => {}
                                                Err(e) => {
                                                    let mut locked_err = err_cell.lock().unwrap();
                                                    if locked_err.is_none() {
                                                        *locked_err = Some(e);
                                                    }
                                                    break;
                                                }
                                            }
                                        }
                                    } else if !matched_locs.is_empty() {
                                        matched = true;
                                    }
                                }
                                if matched {
                                    builder.push_row(
                                        (0..l_cols_len).map(|c| l_batch.columns[c].get(l_idx)),
                                    );
                                }
                            }
                            JoinType::Anti => {
                                let mut has_matching_right = false;
                                if let Some(matched_locs) = locs {
                                    if let Some(f) = filter {
                                        for &loc in matched_locs {
                                            match test_filter_pair(
                                                f, l_batch, l_idx, &r_batches, loc, &comb_sch,
                                            ) {
                                                Ok(true) => {
                                                    has_matching_right = true;
                                                    break;
                                                }
                                                Ok(false) => {}
                                                Err(e) => {
                                                    let mut locked_err = err_cell.lock().unwrap();
                                                    if locked_err.is_none() {
                                                        *locked_err = Some(e);
                                                    }
                                                    break;
                                                }
                                            }
                                        }
                                    } else if !matched_locs.is_empty() {
                                        has_matching_right = true;
                                    }
                                }
                                if !has_matching_right {
                                    builder.push_row(
                                        (0..l_cols_len).map(|c| l_batch.columns[c].get(l_idx)),
                                    );
                                }
                            }
                        }
                    }
                }
                res.lock().unwrap()[morsel.id] = builder.finish();
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

pub fn execute_cross_join(
    left: &LogicalPlan,
    right: &LogicalPlan,
    catalog: &Catalog,
    threads: usize,
) -> Result<Vec<Batch>> {
    let left_batches = execute(left, catalog, threads)?;
    let right_batches = execute(right, catalog, threads)?;
    let left_schema = left.schema()?;
    let right_schema = right.schema()?;

    let mut out_fields = left_schema.fields.clone();
    out_fields.extend(right_schema.fields.clone());
    let out_schema = Arc::new(Schema { fields: out_fields });

    let morsels = create_morsels(&left_batches);
    if morsels.is_empty() || right_batches.is_empty() {
        return Ok(Vec::new());
    }

    let num_morsels = morsels.len();
    let num_threads = threads.max(1);
    let queue = Arc::new(Mutex::new(VecDeque::from(morsels)));
    let results = Arc::new(Mutex::new(vec![Vec::new(); num_morsels]));
    let r_batches = Arc::new(right_batches);

    std::thread::scope(|s| {
        for _ in 0..num_threads {
            let q = Arc::clone(&queue);
            let res = Arc::clone(&results);
            let r_ref = Arc::clone(&r_batches);
            let out_sch = Arc::clone(&out_schema);
            let l_cols_len = left_schema.fields.len();
            let r_cols_len = right_schema.fields.len();

            s.spawn(move || loop {
                let morsel = {
                    let mut locked_q = q.lock().unwrap();
                    locked_q.pop_front()
                };
                let morsel = match morsel {
                    Some(m) => m,
                    None => break,
                };

                let mut builder = BatchBuilder::new(Arc::clone(&out_sch));
                for l_batch in &morsel.batches {
                    for l_idx in 0..l_batch.len {
                        for r_batch in r_ref.iter() {
                            for r_idx in 0..r_batch.len {
                                builder.push_row(
                                    (0..l_cols_len)
                                        .map(|c| l_batch.columns[c].get(l_idx))
                                        .chain(
                                            (0..r_cols_len).map(|c| r_batch.columns[c].get(r_idx)),
                                        ),
                                );
                            }
                        }
                    }
                }
                res.lock().unwrap()[morsel.id] = builder.finish();
            });
        }
    });

    let merged: Vec<Batch> = Arc::try_unwrap(results)
        .unwrap()
        .into_inner()
        .unwrap()
        .into_iter()
        .flatten()
        .collect();

    Ok(merged)
}
