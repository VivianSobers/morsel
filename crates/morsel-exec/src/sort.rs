use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use morsel_core::{Batch, Column, Scalar, Schema, BATCH_ROWS};
use morsel_plan::{LogicalPlan, SortKey};

use crate::catalog::Catalog;
use crate::eval::{compare_scalars, eval};
use crate::execute::execute;
use crate::scheduler::{create_morsels, Morsel};

#[derive(Clone, Debug)]
pub struct SortRow {
    pub keys: Vec<Scalar>,
    pub values: Vec<Scalar>,
    pub source_id: usize,
}

fn cmp_sort_rows(a: &SortRow, b: &SortRow, keys: &[SortKey]) -> Ordering {
    for (idx, key) in keys.iter().enumerate() {
        let va = &a.keys[idx];
        let vb = &b.keys[idx];
        let a_null = va.is_null();
        let b_null = vb.is_null();
        if a_null && b_null {
            continue;
        }
        if a_null {
            return if key.nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        if b_null {
            return if key.nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }
        let ord = compare_scalars(va, vb).unwrap_or(Ordering::Equal);
        let ord = if key.asc { ord } else { ord.reverse() };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    a.source_id.cmp(&b.source_id)
}

struct MaxHeapRow<'a> {
    row: SortRow,
    keys: &'a [SortKey],
}

impl<'a> PartialEq for MaxHeapRow<'a> {
    fn eq(&self, other: &Self) -> bool {
        cmp_sort_rows(&self.row, &other.row, self.keys) == Ordering::Equal
    }
}

impl<'a> Eq for MaxHeapRow<'a> {}

impl<'a> PartialOrd for MaxHeapRow<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for MaxHeapRow<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_sort_rows(&self.row, &other.row, self.keys)
    }
}

struct MinHeapHead<'a> {
    row: &'a SortRow,
    run_idx: usize,
    cursor: usize,
    keys: &'a [SortKey],
}

impl<'a> PartialEq for MinHeapHead<'a> {
    fn eq(&self, other: &Self) -> bool {
        cmp_sort_rows(self.row, other.row, self.keys) == Ordering::Equal
    }
}

impl<'a> Eq for MinHeapHead<'a> {}

impl<'a> PartialOrd for MinHeapHead<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for MinHeapHead<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_sort_rows(other.row, self.row, self.keys)
    }
}

pub fn execute_sort(
    input: &LogicalPlan,
    keys: &[SortKey],
    catalog: &Catalog,
    threads: usize,
    limit: Option<usize>,
) -> Result<Vec<Batch>> {
    let input_batches = execute(input, catalog, threads)?;
    if input_batches.is_empty() {
        return Ok(Vec::new());
    }
    if let Some(0) = limit {
        return Ok(Vec::new());
    }

    let schema = Arc::new(input.schema()?);
    let morsels = create_morsels(&input_batches);
    let num_threads = threads.max(1);

    // Compute global row offset for each morsel for stable tie-breaking
    let mut morsel_offsets = Vec::with_capacity(morsels.len());
    let mut cur_offset = 0;
    for m in &morsels {
        morsel_offsets.push(cur_offset);
        let m_rows: usize = m.batches.iter().map(|b| b.len).sum();
        cur_offset += m_rows;
    }

    let morsels_with_offsets: Vec<(Morsel, usize)> =
        morsels.into_iter().zip(morsel_offsets).collect();
    let num_morsels = morsels_with_offsets.len();
    let next_morsel = AtomicUsize::new(0);
    let cancelled = AtomicBool::new(false);
    let error_cell = Mutex::new(None);

    if let Some(k) = limit {
        // Top-k execution
        let all_thread_heaps = std::thread::scope(|s| {
            let handles: Vec<_> = (0..num_threads)
                .map(|_| {
                    let next_ref = &next_morsel;
                    let morsels_ref = &morsels_with_offsets;
                    let err_cell = &error_cell;
                    let cancelled_ref = &cancelled;

                    s.spawn(move || {
                        let mut max_heap: BinaryHeap<MaxHeapRow> = BinaryHeap::new();
                        loop {
                            if cancelled_ref.load(AtomicOrdering::Relaxed) {
                                break;
                            }
                            let idx = next_ref.fetch_add(1, AtomicOrdering::Relaxed);
                            if idx >= num_morsels {
                                break;
                            }
                            let (morsel, m_start_row) = &morsels_ref[idx];

                            let mut row_counter = *m_start_row;
                            for batch in &morsel.batches {
                                let key_cols = match keys
                                    .iter()
                                    .map(|k| eval(&k.expr, batch))
                                    .collect::<Result<Vec<_>>>()
                                {
                                    Ok(cols) => cols,
                                    Err(e) => {
                                        let mut locked_err = err_cell.lock().unwrap();
                                        if locked_err.is_none() {
                                            *locked_err = Some(e);
                                        }
                                        cancelled_ref.store(true, AtomicOrdering::Relaxed);
                                        break;
                                    }
                                };

                                for i in 0..batch.len {
                                    let row_keys: Vec<Scalar> =
                                        key_cols.iter().map(|c| c.get(i)).collect();
                                    let row_vals: Vec<Scalar> = (0..batch.columns.len())
                                        .map(|c| batch.columns[c].get(i))
                                        .collect();
                                    let sort_row = SortRow {
                                        keys: row_keys,
                                        values: row_vals,
                                        source_id: row_counter,
                                    };
                                    row_counter += 1;

                                    if max_heap.len() < k {
                                        max_heap.push(MaxHeapRow {
                                            row: sort_row,
                                            keys,
                                        });
                                    } else if cmp_sort_rows(
                                        &sort_row,
                                        &max_heap.peek().unwrap().row,
                                        keys,
                                    ) == Ordering::Less
                                    {
                                        max_heap.pop();
                                        max_heap.push(MaxHeapRow {
                                            row: sort_row,
                                            keys,
                                        });
                                    }
                                }
                            }
                        }

                        max_heap
                            .into_iter()
                            .map(|item| item.row)
                            .collect::<Vec<SortRow>>()
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect::<Vec<_>>()
        });

        if let Some(err) = error_cell.into_inner().unwrap() {
            return Err(err);
        }

        let mut all_top: Vec<SortRow> = all_thread_heaps.into_iter().flatten().collect();

        all_top.sort_by(|a, b| cmp_sort_rows(a, b, keys));
        all_top.truncate(k);

        return rows_to_batches(&all_top, &schema);
    }

    // Full sort with k-way merge of sorted runs
    let all_sorted_runs = std::thread::scope(|s| {
        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let next_ref = &next_morsel;
                let morsels_ref = &morsels_with_offsets;
                let err_cell = &error_cell;
                let cancelled_ref = &cancelled;

                s.spawn(move || {
                    let mut local_runs = Vec::new();
                    loop {
                        if cancelled_ref.load(AtomicOrdering::Relaxed) {
                            break;
                        }
                        let idx = next_ref.fetch_add(1, AtomicOrdering::Relaxed);
                        if idx >= num_morsels {
                            break;
                        }
                        let (morsel, m_start_row) = &morsels_ref[idx];

                        let mut morsel_rows = Vec::new();
                        let mut row_counter = *m_start_row;

                        for batch in &morsel.batches {
                            let key_cols = match keys
                                .iter()
                                .map(|k| eval(&k.expr, batch))
                                .collect::<Result<Vec<_>>>()
                            {
                                Ok(cols) => cols,
                                Err(e) => {
                                    let mut locked_err = err_cell.lock().unwrap();
                                    if locked_err.is_none() {
                                        *locked_err = Some(e);
                                    }
                                    cancelled_ref.store(true, AtomicOrdering::Relaxed);
                                    break;
                                }
                            };

                            for i in 0..batch.len {
                                let row_keys: Vec<Scalar> =
                                    key_cols.iter().map(|c| c.get(i)).collect();
                                let row_vals: Vec<Scalar> = (0..batch.columns.len())
                                    .map(|c| batch.columns[c].get(i))
                                    .collect();
                                morsel_rows.push(SortRow {
                                    keys: row_keys,
                                    values: row_vals,
                                    source_id: row_counter,
                                });
                                row_counter += 1;
                            }
                        }

                        morsel_rows.sort_by(|a, b| cmp_sort_rows(a, b, keys));
                        local_runs.push(morsel_rows);
                    }
                    local_runs
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });

    if let Some(err) = error_cell.into_inner().unwrap() {
        return Err(err);
    }

    let runs: Vec<Vec<SortRow>> = all_sorted_runs.into_iter().flatten().collect();
    if runs.is_empty() {
        return Ok(Vec::new());
    }

    // K-way merge using MinHeapHead
    let mut min_heap = BinaryHeap::new();
    for (run_idx, run) in runs.iter().enumerate() {
        if let Some(head) = run.first() {
            min_heap.push(MinHeapHead {
                row: head,
                run_idx,
                cursor: 0,
                keys,
            });
        }
    }

    let num_cols = schema.fields.len();
    let mut batches = Vec::new();
    let mut cur_cols: Vec<Vec<Scalar>> = vec![Vec::with_capacity(BATCH_ROWS); num_cols];

    while let Some(item) = min_heap.pop() {
        for (col_idx, val) in item.row.values.iter().enumerate() {
            cur_cols[col_idx].push(val.clone());
        }
        if !cur_cols.is_empty() && cur_cols[0].len() >= BATCH_ROWS {
            let mut cols = Vec::with_capacity(num_cols);
            for (i, field) in schema.fields.iter().enumerate() {
                let col = Column::from_scalars(field.dtype, &cur_cols[i]);
                cols.push(Arc::new(col));
                cur_cols[i].clear();
            }
            batches.push(Batch::new(Arc::clone(&schema), cols));
        }

        let next_cursor = item.cursor + 1;
        if next_cursor < runs[item.run_idx].len() {
            min_heap.push(MinHeapHead {
                row: &runs[item.run_idx][next_cursor],
                run_idx: item.run_idx,
                cursor: next_cursor,
                keys,
            });
        }
    }

    if !cur_cols.is_empty() && !cur_cols[0].is_empty() {
        let mut cols = Vec::with_capacity(num_cols);
        for (i, field) in schema.fields.iter().enumerate() {
            let col = Column::from_scalars(field.dtype, &cur_cols[i]);
            cols.push(Arc::new(col));
        }
        batches.push(Batch::new(Arc::clone(&schema), cols));
    }

    Ok(batches)
}

fn rows_to_batches(rows: &[SortRow], schema: &Arc<Schema>) -> Result<Vec<Batch>> {
    let num_cols = schema.fields.len();
    let mut batches = Vec::new();
    let total_rows = rows.len();
    let mut offset = 0;

    while offset < total_rows {
        let chunk_len = (total_rows - offset).min(BATCH_ROWS);
        let slice = &rows[offset..offset + chunk_len];

        let mut cur_cols: Vec<Vec<Scalar>> = vec![Vec::with_capacity(chunk_len); num_cols];
        for row in slice {
            for (c, val) in row.values.iter().enumerate() {
                cur_cols[c].push(val.clone());
            }
        }

        let mut cols = Vec::with_capacity(num_cols);
        for (i, field) in schema.fields.iter().enumerate() {
            let col = Column::from_scalars(field.dtype, &cur_cols[i]);
            cols.push(Arc::new(col));
        }
        batches.push(Batch::new(Arc::clone(schema), cols));
        offset += chunk_len;
    }

    Ok(batches)
}
