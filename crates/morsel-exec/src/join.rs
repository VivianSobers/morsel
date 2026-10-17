use std::hash::Hash;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use ahash::AHashMap;
use anyhow::Result;
use morsel_core::{Batch, Column, ColumnData, DataType, Scalar, Schema, BATCH_ROWS};
use morsel_plan::{Expr, JoinType, LogicalPlan};

use crate::aggregate::{column_get_key, ScalarKey};
use crate::catalog::Catalog;
use crate::eval::{cast_column, eval, eval_predicate};
use crate::execute::execute;
use crate::scheduler::create_morsels;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum JoinKey {
    I64(i64),
    I32(i32),
    Date32(i32),
    TwoI64(i64, i64),
    Decimal(Box<(i128, u8)>),
    Str(Box<str>),
    Single(Box<ScalarKey>),
    Two(Box<(ScalarKey, ScalarKey)>),
    Multi(Box<[ScalarKey]>),
}

pub fn extract_join_key(cols: &[Column], row_idx: usize) -> Option<JoinKey> {
    match cols.len() {
        1 => {
            let col = &cols[0];
            if col.is_null(row_idx) {
                return None;
            }
            match &col.data {
                ColumnData::Int64(v) => Some(JoinKey::I64(v[row_idx])),
                ColumnData::Int32(v) => Some(JoinKey::I32(v[row_idx])),
                ColumnData::Date32(v) => Some(JoinKey::Date32(v[row_idx])),
                ColumnData::Decimal(v) => {
                    let scale = match col.dtype {
                        DataType::Decimal { scale, .. } => scale,
                        _ => 0,
                    };
                    Some(JoinKey::Decimal(Box::new((v[row_idx], scale))))
                }
                ColumnData::Utf8 { .. } => Some(JoinKey::Str(col.utf8_value(row_idx).into())),
                _ => Some(JoinKey::Single(Box::new(column_get_key(col, row_idx)))),
            }
        }
        2 => {
            if cols[0].is_null(row_idx) || cols[1].is_null(row_idx) {
                return None;
            }
            if let (ColumnData::Int64(v1), ColumnData::Int64(v2)) = (&cols[0].data, &cols[1].data) {
                return Some(JoinKey::TwoI64(v1[row_idx], v2[row_idx]));
            }
            Some(JoinKey::Two(Box::new((
                column_get_key(&cols[0], row_idx),
                column_get_key(&cols[1], row_idx),
            ))))
        }
        _ => {
            let mut v = Vec::with_capacity(cols.len());
            for col in cols {
                if col.is_null(row_idx) {
                    return None;
                }
                v.push(column_get_key(col, row_idx));
            }
            Some(JoinKey::Multi(v.into_boxed_slice()))
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RowLoc {
    pub batch_idx: u32,
    pub row_idx: u32,
}

#[derive(Clone, Copy, Debug)]
struct HeadTail {
    head: u32,
    tail: u32,
}

pub struct PartitionTable {
    map: AHashMap<JoinKey, HeadTail>,
    locs: Vec<RowLoc>,
    next: Vec<u32>,
}

impl PartitionTable {
    pub fn new() -> Self {
        Self {
            map: AHashMap::new(),
            locs: Vec::new(),
            next: Vec::new(),
        }
    }

    #[inline]
    pub fn get_matches(&self, key: &JoinKey) -> Option<MatchesIter<'_>> {
        self.map.get(key).map(|ht| MatchesIter {
            locs: &self.locs,
            next: &self.next,
            cur: ht.head,
        })
    }
}

impl Default for PartitionTable {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy)]
pub struct MatchesIter<'a> {
    locs: &'a [RowLoc],
    next: &'a [u32],
    cur: u32,
}

impl<'a> Iterator for MatchesIter<'a> {
    type Item = RowLoc;

    #[inline]
    fn next(&mut self) -> Option<RowLoc> {
        if self.cur == u32::MAX {
            None
        } else {
            let idx = self.cur as usize;
            let loc = self.locs[idx];
            self.cur = self.next[idx];
            Some(loc)
        }
    }
}

pub struct ParallelJoinTable {
    partitions: Vec<PartitionTable>,
    mask: usize,
    random_state: ahash::RandomState,
}

impl ParallelJoinTable {
    #[inline]
    fn partition_index(&self, key: &JoinKey) -> usize {
        (self.random_state.hash_one(key) as usize) & self.mask
    }

    #[inline]
    pub fn get_matches(&self, key: &JoinKey) -> Option<MatchesIter<'_>> {
        let p = self.partition_index(key);
        self.partitions[p].get_matches(key)
    }
}

type PartitionInputBuffers = Vec<Mutex<Option<Vec<Vec<(JoinKey, RowLoc)>>>>>;

pub struct BatchBuilder {
    schema: Arc<Schema>,
    columns: Vec<Vec<Scalar>>,
    batches: Vec<Batch>,
}

impl BatchBuilder {
    pub fn new(schema: Arc<Schema>) -> Self {
        let num_cols = schema.fields.len();
        Self {
            schema,
            columns: vec![Vec::with_capacity(1024.min(BATCH_ROWS)); num_cols],
            batches: Vec::new(),
        }
    }

    pub fn push_row(&mut self, row: impl Iterator<Item = Scalar>) {
        for (col_idx, val) in row.enumerate() {
            self.columns[col_idx].push(val);
        }
        if !self.columns.is_empty() && self.columns[0].len() >= BATCH_ROWS {
            self.flush();
        }
    }

    pub fn flush(&mut self) {
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

    pub fn finish(mut self) -> Vec<Batch> {
        self.flush();
        self.batches
    }
}

fn expr_referenced_columns(expr: &Expr, out: &mut Vec<usize>) {
    match expr {
        Expr::Column(idx) => out.push(*idx),
        Expr::Literal(_) => {}
        Expr::Binary { left, right, .. } => {
            expr_referenced_columns(left, out);
            expr_referenced_columns(right, out);
        }
        Expr::Not(e) | Expr::Negate(e) | Expr::IsNull(e) => expr_referenced_columns(e, out),
        Expr::Like { expr, .. } => expr_referenced_columns(expr, out),
        Expr::InList { expr, .. } => expr_referenced_columns(expr, out),
        Expr::Case { whens, else_expr } => {
            for (w, t) in whens {
                expr_referenced_columns(w, out);
                expr_referenced_columns(t, out);
            }
            if let Some(e) = else_expr {
                expr_referenced_columns(e, out);
            }
        }
        Expr::Cast { expr, .. } => expr_referenced_columns(expr, out),
        Expr::Extract { expr, .. } => expr_referenced_columns(expr, out),
        Expr::Substring { expr, .. } => expr_referenced_columns(expr, out),
    }
}

fn expr_is_pure_right(expr: &Expr, l_cols_len: usize) -> bool {
    let mut cols = Vec::new();
    expr_referenced_columns(expr, &mut cols);
    !cols.is_empty() && cols.iter().all(|&c| c >= l_cols_len)
}

fn shift_expr_columns(expr: &Expr, offset: usize) -> Expr {
    match expr {
        Expr::Column(idx) => Expr::Column(idx.saturating_sub(offset)),
        Expr::Literal(s) => Expr::Literal(s.clone()),
        Expr::Binary { op, left, right } => Expr::Binary {
            op: *op,
            left: Box::new(shift_expr_columns(left, offset)),
            right: Box::new(shift_expr_columns(right, offset)),
        },
        Expr::Not(e) => Expr::Not(Box::new(shift_expr_columns(e, offset))),
        Expr::Negate(e) => Expr::Negate(Box::new(shift_expr_columns(e, offset))),
        Expr::IsNull(e) => Expr::IsNull(Box::new(shift_expr_columns(e, offset))),
        Expr::Like {
            expr,
            pattern,
            negated,
        } => Expr::Like {
            expr: Box::new(shift_expr_columns(expr, offset)),
            pattern: pattern.clone(),
            negated: *negated,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(shift_expr_columns(expr, offset)),
            list: list.clone(),
            negated: *negated,
        },
        Expr::Case { whens, else_expr } => Expr::Case {
            whens: whens
                .iter()
                .map(|(w, t)| (shift_expr_columns(w, offset), shift_expr_columns(t, offset)))
                .collect(),
            else_expr: else_expr
                .as_ref()
                .map(|e| Box::new(shift_expr_columns(e, offset))),
        },
        Expr::Cast { expr, to } => Expr::Cast {
            expr: Box::new(shift_expr_columns(expr, offset)),
            to: *to,
        },
        Expr::Extract { field, expr } => Expr::Extract {
            field: *field,
            expr: Box::new(shift_expr_columns(expr, offset)),
        },
        Expr::Substring { expr, start, len } => Expr::Substring {
            expr: Box::new(shift_expr_columns(expr, offset)),
            start: *start,
            len: *len,
        },
    }
}

#[allow(clippy::too_many_arguments)]
fn test_filter_pair(
    filter: &Expr,
    left_batch: &Batch,
    l_idx: usize,
    right_batches: &[Batch],
    loc: RowLoc,
    combined_schema: &Arc<Schema>,
    referenced_cols: &[usize],
    null_cols: &[Arc<Column>],
) -> Result<bool> {
    let mut cols = Vec::with_capacity(combined_schema.fields.len());
    let l_len = left_batch.columns.len();
    for (i, _) in combined_schema.fields.iter().enumerate() {
        if referenced_cols.contains(&i) {
            let s = if i < l_len {
                left_batch.columns[i].get(l_idx)
            } else {
                right_batches[loc.batch_idx as usize].columns[i - l_len].get(loc.row_idx as usize)
            };
            cols.push(Arc::new(Column::from_scalars(
                combined_schema.fields[i].dtype,
                &[s],
            )));
        } else {
            cols.push(Arc::clone(&null_cols[i]));
        }
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
    let left_schema = left.schema()?;
    let right_schema = right.schema()?;
    let l_cols_len = left_schema.fields.len();

    let (right_filter, residual_filter) = match filter {
        Some(f) if expr_is_pure_right(f, l_cols_len) => {
            (Some(shift_expr_columns(f, l_cols_len)), None)
        }
        other => (None, other),
    };
    let mut referenced_cols = Vec::new();
    if let Some(f) = residual_filter {
        expr_referenced_columns(f, &mut referenced_cols);
        referenced_cols.sort_unstable();
        referenced_cols.dedup();
    }

    let mut right_batches = execute(right, catalog, threads)?;

    // Prune unused right columns for Semi and Anti joins immediately to free memory
    if matches!(join_type, JoinType::Semi | JoinType::Anti) {
        let mut needed_right = std::collections::HashSet::new();
        for (_, r_expr) in on {
            let mut cols = Vec::new();
            expr_referenced_columns(r_expr, &mut cols);
            needed_right.extend(cols);
        }
        if let Some(ref rf) = right_filter {
            let mut cols = Vec::new();
            expr_referenced_columns(rf, &mut cols);
            needed_right.extend(cols);
        }
        for &idx in &referenced_cols {
            if idx >= l_cols_len {
                needed_right.insert(idx - l_cols_len);
            }
        }

        for batch in &mut right_batches {
            let len = batch.len;
            let mut new_cols = Vec::with_capacity(batch.columns.len());
            for (c, col) in batch.columns.iter().enumerate() {
                if needed_right.contains(&c) {
                    new_cols.push(Arc::clone(col));
                } else {
                    new_cols.push(Arc::new(Column {
                        dtype: col.dtype,
                        data: ColumnData::Int32(Vec::new()),
                        validity: None,
                    }));
                }
            }
            *batch = Batch {
                schema: Arc::clone(&batch.schema),
                columns: new_cols,
                len,
            };
        }
    }

    let left_batches = execute(left, catalog, threads)?;

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

    let left_total_rows: usize = left_batches.iter().map(|b| b.len).sum();
    let right_total_rows: usize = right_batches.iter().map(|b| b.len).sum();
    let build_on_left = matches!(join_type, JoinType::Inner) && left_total_rows < right_total_rows;

    let (build_batches, build_total_rows) = if build_on_left {
        (&left_batches, left_total_rows)
    } else {
        (&right_batches, right_total_rows)
    };

    let num_threads = threads.max(1);
    let num_partitions = if build_total_rows < 512 {
        1
    } else {
        (num_threads * 4).next_power_of_two().clamp(16, 64)
    };
    let mask = num_partitions - 1;
    let random_state = ahash::RandomState::new();

    let mut partitions = Vec::with_capacity(num_partitions);
    for _ in 0..num_partitions {
        partitions.push(PartitionTable::new());
    }

    if build_total_rows > 0 {
        let num_batches = build_batches.len();
        let chunk_size = 16.max((num_batches / (num_threads * 4)).max(1));
        let next_chunk = AtomicUsize::new(0);
        let cancelled = AtomicBool::new(false);
        let error_cell = Mutex::new(None);

        // Phase 1: Partition build rows in parallel
        let mut thread_buffers: Vec<Vec<Vec<(JoinKey, RowLoc)>>> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..num_threads)
                .map(|_| {
                    let next_chunk_ref = &next_chunk;
                    let cancelled_ref = &cancelled;
                    let err_cell = &error_cell;
                    let b_batches = build_batches.as_slice();
                    let k_types = &key_types;
                    let r_state = &random_state;
                    let r_filt = if build_on_left {
                        None
                    } else {
                        right_filter.as_ref()
                    };

                    s.spawn(move || {
                        let init_cap =
                            (build_total_rows / num_partitions / num_threads).clamp(64, 32768);
                        let mut t_buf: Vec<Vec<(JoinKey, RowLoc)>> = (0..num_partitions)
                            .map(|_| Vec::with_capacity(init_cap))
                            .collect();
                        loop {
                            if cancelled_ref.load(Ordering::Relaxed) {
                                break;
                            }
                            let chunk_start =
                                next_chunk_ref.fetch_add(chunk_size, Ordering::Relaxed);
                            if chunk_start >= num_batches {
                                break;
                            }
                            let chunk_end = (chunk_start + chunk_size).min(num_batches);

                            for (offset, batch) in
                                b_batches[chunk_start..chunk_end].iter().enumerate()
                            {
                                let b_idx = chunk_start + offset;
                                let filter_mask = if let Some(rf) = r_filt {
                                    match eval_predicate(rf, batch) {
                                        Ok(sel) => {
                                            let mut mask = vec![false; batch.len];
                                            for idx in sel {
                                                mask[idx as usize] = true;
                                            }
                                            Some(mask)
                                        }
                                        Err(e) => {
                                            let mut l = err_cell.lock().unwrap();
                                            if l.is_none() {
                                                *l = Some(e);
                                            }
                                            cancelled_ref.store(true, Ordering::Relaxed);
                                            return t_buf;
                                        }
                                    }
                                } else {
                                    None
                                };

                                let key_cols = match on
                                    .iter()
                                    .enumerate()
                                    .map(|(k, (expr_l, expr_r))| {
                                        let expr = if build_on_left { expr_l } else { expr_r };
                                        let col = eval(expr, batch)?;
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
                                        let mut l = err_cell.lock().unwrap();
                                        if l.is_none() {
                                            *l = Some(e);
                                        }
                                        cancelled_ref.store(true, Ordering::Relaxed);
                                        return t_buf;
                                    }
                                };

                                for row_idx in 0..batch.len {
                                    if let Some(ref mask) = filter_mask {
                                        if !mask[row_idx] {
                                            continue;
                                        }
                                    }
                                    if let Some(key) = extract_join_key(&key_cols, row_idx) {
                                        let p = (r_state.hash_one(&key) as usize) & mask;
                                        t_buf[p].push((
                                            key,
                                            RowLoc {
                                                batch_idx: b_idx as u32,
                                                row_idx: row_idx as u32,
                                            },
                                        ));
                                    }
                                }
                            }
                        }
                        t_buf
                    })
                })
                .collect();

            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        if let Some(err) = error_cell.into_inner().unwrap() {
            return Err(err);
        }

        // After Phase 1 build is done, the hash table has been populated with join keys.
        // For Semi and Anti joins, the only right columns ever needed during probe are
        // those referenced by residual_filter. Drop any others now to free more memory!
        if matches!(join_type, JoinType::Semi | JoinType::Anti) {
            let mut probe_needed = std::collections::HashSet::new();
            for &idx in &referenced_cols {
                if idx >= l_cols_len {
                    probe_needed.insert(idx - l_cols_len);
                }
            }
            for batch in &mut right_batches {
                for (c, col) in batch.columns.iter_mut().enumerate() {
                    if !probe_needed.contains(&c) && !col.is_empty() {
                        *col = Arc::new(Column {
                            dtype: col.dtype,
                            data: ColumnData::Int32(Vec::new()),
                            validity: None,
                        });
                    }
                }
            }
        }

        // Phase 2: Build partition tables in parallel
        let partition_inputs: PartitionInputBuffers = (0..num_partitions)
            .map(|p| {
                let bufs: Vec<_> = (0..num_threads)
                    .map(|t| std::mem::take(&mut thread_buffers[t][p]))
                    .collect();
                Mutex::new(Some(bufs))
            })
            .collect();

        let next_partition = AtomicUsize::new(0);
        let mut partition_tables: Vec<Option<PartitionTable>> =
            (0..num_partitions).map(|_| None).collect();

        std::thread::scope(|s| {
            let handles: Vec<_> = (0..num_threads)
                .map(|_| {
                    let next_p_ref = &next_partition;
                    let p_inputs = &partition_inputs;
                    s.spawn(move || {
                        let mut built = Vec::new();
                        loop {
                            let p = next_p_ref.fetch_add(1, Ordering::Relaxed);
                            if p >= num_partitions {
                                break;
                            }
                            let bufs = p_inputs[p].lock().unwrap().take().unwrap();
                            let total_entries: usize = bufs.iter().map(|b| b.len()).sum();
                            let map_cap = (total_entries / 2).max(1);
                            let mut map = AHashMap::with_capacity(map_cap);
                            let mut locs = Vec::with_capacity(total_entries);
                            let mut next = Vec::with_capacity(total_entries);

                            for mut thread_buf in bufs {
                                for (key, loc) in thread_buf.drain(..) {
                                    let entry_idx = locs.len() as u32;
                                    locs.push(loc);
                                    next.push(u32::MAX);

                                    match map.entry(key) {
                                        std::collections::hash_map::Entry::Occupied(mut occ) => {
                                            let ht: &mut HeadTail = occ.get_mut();
                                            next[ht.tail as usize] = entry_idx;
                                            ht.tail = entry_idx;
                                        }
                                        std::collections::hash_map::Entry::Vacant(vac) => {
                                            vac.insert(HeadTail {
                                                head: entry_idx,
                                                tail: entry_idx,
                                            });
                                        }
                                    }
                                }
                                drop(thread_buf);
                            }
                            built.push((p, PartitionTable { map, locs, next }));
                        }
                        built
                    })
                })
                .collect();

            for h in handles {
                let res = h.join().unwrap();
                for (p, table) in res {
                    partition_tables[p] = Some(table);
                }
            }
        });

        partitions = partition_tables
            .into_iter()
            .map(|opt| opt.unwrap_or_else(PartitionTable::new))
            .collect();
    }

    let hash_table = ParallelJoinTable {
        partitions,
        mask,
        random_state,
    };

    let probe_batches = if build_on_left {
        &right_batches
    } else {
        &left_batches
    };

    let morsels = create_morsels(probe_batches);
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

    let null_cols: Vec<Arc<Column>> = combined_schema
        .fields
        .iter()
        .map(|f| Arc::new(Column::from_scalars(f.dtype, &[Scalar::Null])))
        .collect();

    std::thread::scope(|s| {
        for _ in 0..num_threads {
            let next_ref = &next_morsel;
            let morsels_ref = &morsels;
            let res_ref = &results;
            let err_cell = &error_cell;
            let cancelled_ref = &cancelled;
            let ht = &hash_table;
            let l_batches = &left_batches;
            let r_batches = &right_batches;
            let out_sch = &out_schema;
            let comb_sch = &combined_schema;
            let k_types = &key_types;
            let l_cols_len = left_schema.fields.len();
            let r_cols_len = right_schema.fields.len();
            let filter = residual_filter;
            let ref_cols = &referenced_cols;
            let r_filter = right_filter.as_ref();
            let n_cols = &null_cols;

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

                if build_on_left {
                    for r_batch in &morsel.batches {
                        let filter_mask = if let Some(rf) = r_filter {
                            match eval_predicate(rf, r_batch) {
                                Ok(sel) => {
                                    let mut mask = vec![false; r_batch.len];
                                    for idx in sel {
                                        mask[idx as usize] = true;
                                    }
                                    Some(mask)
                                }
                                Err(e) => {
                                    let mut locked_err = err_cell.lock().unwrap();
                                    if locked_err.is_none() {
                                        *locked_err = Some(e);
                                    }
                                    cancelled_ref.store(true, Ordering::Relaxed);
                                    break;
                                }
                            }
                        } else {
                            None
                        };

                        let r_key_cols: Vec<Column> = match on
                            .iter()
                            .enumerate()
                            .map(|(k, (_, r_expr))| {
                                let col = eval(r_expr, r_batch)?;
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

                        for r_idx in 0..r_batch.len {
                            if let Some(ref mask) = filter_mask {
                                if !mask[r_idx] {
                                    continue;
                                }
                            }
                            let locs = match extract_join_key(&r_key_cols, r_idx) {
                                Some(ref key) => ht.get_matches(key),
                                None => None,
                            };

                            if let Some(matched_locs) = locs {
                                for loc in matched_locs {
                                    let l_batch = &l_batches[loc.batch_idx as usize];
                                    let l_idx = loc.row_idx as usize;
                                    if let Some(f) = filter {
                                        match test_filter_pair(
                                            f,
                                            l_batch,
                                            l_idx,
                                            std::slice::from_ref(r_batch),
                                            RowLoc {
                                                batch_idx: 0,
                                                row_idx: r_idx as u32,
                                            },
                                            comb_sch,
                                            ref_cols,
                                            n_cols,
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
                                            .chain(
                                                (0..r_cols_len)
                                                    .map(|c| r_batch.columns[c].get(r_idx)),
                                            ),
                                    );
                                }
                            }
                        }
                    }
                } else {
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
                            let locs = match extract_join_key(&l_key_cols, l_idx) {
                                Some(ref key) => ht.get_matches(key),
                                None => None,
                            };

                            match join_type {
                                JoinType::Inner => {
                                    if let Some(matched_locs) = locs {
                                        for loc in matched_locs {
                                            if let Some(f) = filter {
                                                match test_filter_pair(
                                                    f, l_batch, l_idx, r_batches, loc, comb_sch,
                                                    ref_cols, n_cols,
                                                ) {
                                                    Ok(true) => {}
                                                    Ok(false) => continue,
                                                    Err(e) => {
                                                        let mut locked_err =
                                                            err_cell.lock().unwrap();
                                                        if locked_err.is_none() {
                                                            *locked_err = Some(e);
                                                        }
                                                        cancelled_ref
                                                            .store(true, Ordering::Relaxed);
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
                                        for loc in matched_locs {
                                            if let Some(f) = filter {
                                                match test_filter_pair(
                                                    f, l_batch, l_idx, r_batches, loc, comb_sch,
                                                    ref_cols, n_cols,
                                                ) {
                                                    Ok(true) => {}
                                                    Ok(false) => continue,
                                                    Err(e) => {
                                                        let mut locked_err =
                                                            err_cell.lock().unwrap();
                                                        if locked_err.is_none() {
                                                            *locked_err = Some(e);
                                                        }
                                                        cancelled_ref
                                                            .store(true, Ordering::Relaxed);
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
                                            for loc in matched_locs {
                                                match test_filter_pair(
                                                    f, l_batch, l_idx, r_batches, loc, comb_sch,
                                                    ref_cols, n_cols,
                                                ) {
                                                    Ok(true) => {
                                                        matched = true;
                                                        break;
                                                    }
                                                    Ok(false) => {}
                                                    Err(e) => {
                                                        let mut locked_err =
                                                            err_cell.lock().unwrap();
                                                        if locked_err.is_none() {
                                                            *locked_err = Some(e);
                                                        }
                                                        cancelled_ref
                                                            .store(true, Ordering::Relaxed);
                                                        break;
                                                    }
                                                }
                                            }
                                        } else {
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
                                            for loc in matched_locs {
                                                match test_filter_pair(
                                                    f, l_batch, l_idx, r_batches, loc, comb_sch,
                                                    ref_cols, n_cols,
                                                ) {
                                                    Ok(true) => {
                                                        has_matching_right = true;
                                                        break;
                                                    }
                                                    Ok(false) => {}
                                                    Err(e) => {
                                                        let mut locked_err =
                                                            err_cell.lock().unwrap();
                                                        if locked_err.is_none() {
                                                            *locked_err = Some(e);
                                                        }
                                                        cancelled_ref
                                                            .store(true, Ordering::Relaxed);
                                                        break;
                                                    }
                                                }
                                            }
                                        } else {
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
                }
                let fin = builder.finish();
                *res_ref[morsel.id].lock().unwrap() = fin;
            });
        }
    });

    if let Some(err) = error_cell.into_inner().unwrap() {
        return Err(err);
    }

    drop(hash_table);
    drop(left_batches);
    drop(right_batches);

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
