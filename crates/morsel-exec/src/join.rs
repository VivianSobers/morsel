use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use ahash::AHashMap;
use anyhow::Result;
use morsel_core::{Batch, Column, DataType, Scalar, Schema, BATCH_ROWS};
use morsel_plan::{Expr, JoinType, LogicalPlan};

use crate::aggregate::{column_get_key, ScalarKey};
use crate::catalog::Catalog;
use crate::eval::{cast_column, eval, eval_predicate};
use crate::execute::execute;
use crate::scheduler::create_morsels;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum JoinKey {
    Single(ScalarKey),
    Two(ScalarKey, ScalarKey),
    Multi(Vec<ScalarKey>),
}

pub fn extract_join_key(cols: &[Column], row_idx: usize) -> Option<JoinKey> {
    match cols.len() {
        1 => {
            if cols[0].is_null(row_idx) {
                None
            } else {
                Some(JoinKey::Single(column_get_key(&cols[0], row_idx)))
            }
        }
        2 => {
            if cols[0].is_null(row_idx) || cols[1].is_null(row_idx) {
                None
            } else {
                Some(JoinKey::Two(
                    column_get_key(&cols[0], row_idx),
                    column_get_key(&cols[1], row_idx),
                ))
            }
        }
        _ => {
            let mut v = Vec::with_capacity(cols.len());
            for col in cols {
                if col.is_null(row_idx) {
                    return None;
                }
                v.push(column_get_key(col, row_idx));
            }
            Some(JoinKey::Multi(v))
        }
    }
}

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

pub fn coerce_join_key_types(t1: &DataType, t2: &DataType) -> Option<DataType> {
    if t1 == t2 {
        return Some(*t1);
    }
    match (t1, t2) {
        (DataType::Int32, DataType::Int64) | (DataType::Int64, DataType::Int32) => {
            Some(DataType::Int64)
        }
        (DataType::Decimal { scale: s1, .. }, DataType::Decimal { scale: s2, .. }) => {
            Some(DataType::Decimal {
                precision: 38,
                scale: *s1.max(s2),
            })
        }
        (DataType::Decimal { scale, .. }, DataType::Int32 | DataType::Int64)
        | (DataType::Int32 | DataType::Int64, DataType::Decimal { scale, .. }) => {
            Some(DataType::Decimal {
                precision: 38,
                scale: *scale,
            })
        }
        _ => None,
    }
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

    let key_types: Vec<DataType> = on
        .iter()
        .map(|(l_expr, r_expr)| {
            let lt = l_expr.data_type(&left_schema)?;
            let rt = r_expr.data_type(&right_schema)?;
            coerce_join_key_types(&lt, &rt).ok_or_else(|| {
                anyhow::anyhow!("cannot join on incompatible types: {:?} and {:?}", lt, rt)
            })
        })
        .collect::<Result<Vec<_>>>()?;

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
    let mut hash_table: AHashMap<JoinKey, Vec<RowLoc>> = AHashMap::new();
    for (b_idx, r_batch) in right_batches.iter().enumerate() {
        let r_key_cols: Vec<Column> = on
            .iter()
            .enumerate()
            .map(|(k, (_, r_expr))| {
                let col = eval(r_expr, r_batch)?;
                if col.dtype != key_types[k] {
                    cast_column(&col, key_types[k])
                } else {
                    Ok(col)
                }
            })
            .collect::<Result<Vec<_>>>()?;

        for r_idx in 0..r_batch.len {
            if let Some(key) = extract_join_key(&r_key_cols, r_idx) {
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
    let next_morsel = AtomicUsize::new(0);
    let cancelled = AtomicBool::new(false);
    let results: Vec<Mutex<Vec<Batch>>> =
        (0..num_morsels).map(|_| Mutex::new(Vec::new())).collect();
    let error_cell = Mutex::new(None);

    std::thread::scope(|s| {
        for _ in 0..num_threads {
            let next_ref = &next_morsel;
            let morsels_ref = &morsels;
            let res_ref = &results;
            let err_cell = &error_cell;
            let cancelled_ref = &cancelled;
            let ht = &hash_table;
            let r_batches = &right_batches;
            let out_sch = &out_schema;
            let comb_sch = &combined_schema;
            let k_types = &key_types;
            let l_cols_len = left_schema.fields.len();
            let r_cols_len = right_schema.fields.len();

            s.spawn(move || loop {
                if cancelled_ref.load(Ordering::Relaxed) {
                    break;
                }
                let idx = next_ref.fetch_add(1, Ordering::Relaxed);
                if idx >= num_morsels {
                    break;
                }
                let morsel = &morsels_ref[idx];

                let mut builder = BatchBuilder::new(Arc::clone(out_sch));

                for l_batch in &morsel.batches {
                    let l_key_cols: Vec<Column> = match on
                        .iter()
                        .enumerate()
                        .map(|(k, (l_expr, _))| {
                            let col = eval(l_expr, l_batch)?;
                            if col.dtype != k_types[k] {
                                cast_column(&col, k_types[k])
                            } else {
                                Ok(col)
                            }
                        })
                        .collect::<Result<Vec<_>>>()
                    {
                        Ok(cols) => cols,
                        Err(e) => {
                            let mut locked_err = err_cell.lock().unwrap();
                            if locked_err.is_none() {
                                *locked_err = Some(e);
                            }
                            cancelled_ref.store(true, Ordering::Relaxed);
                            break;
                        }
                    };

                    for l_idx in 0..l_batch.len {
                        let locs: Option<&Vec<RowLoc>> = match extract_join_key(&l_key_cols, l_idx)
                        {
                            Some(ref key) => ht.get(key),
                            None => None,
                        };

                        match join_type {
                            JoinType::Inner => {
                                if let Some(matched_locs) = locs {
                                    for &loc in matched_locs {
                                        if let Some(f) = filter {
                                            match test_filter_pair(
                                                f, l_batch, l_idx, r_batches, loc, comb_sch,
                                            ) {
                                                Ok(true) => {}
                                                Ok(false) => continue,
                                                Err(e) => {
                                                    let mut locked_err = err_cell.lock().unwrap();
                                                    if locked_err.is_none() {
                                                        *locked_err = Some(e);
                                                    }
                                                    cancelled_ref.store(true, Ordering::Relaxed);
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
                                                f, l_batch, l_idx, r_batches, loc, comb_sch,
                                            ) {
                                                Ok(true) => {}
                                                Ok(false) => continue,
                                                Err(e) => {
                                                    let mut locked_err = err_cell.lock().unwrap();
                                                    if locked_err.is_none() {
                                                        *locked_err = Some(e);
                                                    }
                                                    cancelled_ref.store(true, Ordering::Relaxed);
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
                                                f, l_batch, l_idx, r_batches, loc, comb_sch,
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
                                                    cancelled_ref.store(true, Ordering::Relaxed);
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
                                                f, l_batch, l_idx, r_batches, loc, comb_sch,
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
                                                    cancelled_ref.store(true, Ordering::Relaxed);
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
                *res_ref[morsel.id].lock().unwrap() = builder.finish();
            });
        }
    });

    if let Some(err) = error_cell.into_inner().unwrap() {
        return Err(err);
    }

    let all_results: Vec<Batch> = results
        .into_iter()
        .flat_map(|m| m.into_inner().unwrap())
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
    let next_morsel = AtomicUsize::new(0);
    let results: Vec<Mutex<Vec<Batch>>> =
        (0..num_morsels).map(|_| Mutex::new(Vec::new())).collect();

    std::thread::scope(|s| {
        for _ in 0..num_threads {
            let next_ref = &next_morsel;
            let morsels_ref = &morsels;
            let res_ref = &results;
            let r_ref = &right_batches;
            let out_sch = &out_schema;
            let l_cols_len = left_schema.fields.len();
            let r_cols_len = right_schema.fields.len();

            s.spawn(move || loop {
                let idx = next_ref.fetch_add(1, Ordering::Relaxed);
                if idx >= num_morsels {
                    break;
                }
                let morsel = &morsels_ref[idx];

                let mut builder = BatchBuilder::new(Arc::clone(out_sch));
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
                *res_ref[morsel.id].lock().unwrap() = builder.finish();
            });
        }
    });

    let merged: Vec<Batch> = results
        .into_iter()
        .flat_map(|m| m.into_inner().unwrap())
        .collect();

    Ok(merged)
}
