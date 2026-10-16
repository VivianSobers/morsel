use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use ahash::AHashSet;
use anyhow::{bail, Result};
use morsel_core::{Batch, Column, DataType, Scalar, BATCH_ROWS};
use morsel_plan::{AggExpr, AggFunc, Expr, LogicalPlan};

use crate::catalog::Catalog;
use crate::eval::eval;
use crate::execute::execute;
use crate::scheduler::{create_morsels, Morsel};

fn pow10(n: u8) -> i128 {
    10i128.pow(n as u32)
}

type AggMap = HashMap<Vec<ScalarKey>, Vec<AggState>>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ScalarKey {
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float64(u64),
    Decimal(i128, u8),
    Date32(i32),
    Utf8(String),
}

pub fn to_key(s: &Scalar) -> ScalarKey {
    match s {
        Scalar::Null => ScalarKey::Null,
        Scalar::Bool(b) => ScalarKey::Bool(*b),
        Scalar::Int32(x) => ScalarKey::Int32(*x),
        Scalar::Int64(x) => ScalarKey::Int64(*x),
        Scalar::Float64(f) => {
            let val = if *f == 0.0 { 0.0 } else { *f };
            ScalarKey::Float64(val.to_bits())
        }
        Scalar::Decimal(v, s) => ScalarKey::Decimal(*v, *s),
        Scalar::Date32(d) => ScalarKey::Date32(*d),
        Scalar::Utf8(st) => ScalarKey::Utf8(st.clone()),
    }
}

pub fn to_scalar(k: &ScalarKey) -> Scalar {
    match k {
        ScalarKey::Null => Scalar::Null,
        ScalarKey::Bool(b) => Scalar::Bool(*b),
        ScalarKey::Int32(x) => Scalar::Int32(*x),
        ScalarKey::Int64(x) => Scalar::Int64(*x),
        ScalarKey::Float64(u) => Scalar::Float64(f64::from_bits(*u)),
        ScalarKey::Decimal(v, s) => Scalar::Decimal(*v, *s),
        ScalarKey::Date32(d) => Scalar::Date32(*d),
        ScalarKey::Utf8(st) => Scalar::Utf8(st.clone()),
    }
}

#[derive(Clone, Debug)]
pub struct AggState {
    pub count: i64,
    pub count_star: i64,
    pub distinct_set: Option<AHashSet<ScalarKey>>,
    pub sum_int: i64,
    pub sum_dec: i128,
    pub sum_float: f64,
    pub has_sum: bool,
    pub decimal_scale: u8,
    pub is_decimal: bool,
    pub is_int: bool,
    pub is_float: bool,
    pub min: Option<Scalar>,
    pub max: Option<Scalar>,
}

impl AggState {
    pub fn new(func: AggFunc, arg_dtype: Option<DataType>) -> Self {
        let mut is_decimal = false;
        let mut decimal_scale = 0;
        let mut is_int = false;
        let mut is_float = false;

        if let Some(dtype) = arg_dtype {
            match dtype {
                DataType::Decimal { scale, .. } => {
                    is_decimal = true;
                    decimal_scale = scale;
                }
                DataType::Int32 | DataType::Int64 => {
                    is_int = true;
                }
                DataType::Float64 => {
                    is_float = true;
                }
                _ => {}
            }
        }

        let distinct_set = if func == AggFunc::CountDistinct {
            Some(AHashSet::new())
        } else {
            None
        };

        Self {
            count: 0,
            count_star: 0,
            distinct_set,
            sum_int: 0,
            sum_dec: 0,
            sum_float: 0.0,
            has_sum: false,
            decimal_scale,
            is_decimal,
            is_int,
            is_float,
            min: None,
            max: None,
        }
    }

    pub fn update(&mut self, func: AggFunc, val: Option<Scalar>) -> Result<()> {
        self.count_star += 1;
        if let Some(scalar) = val {
            if !scalar.is_null() {
                self.count += 1;
                if let Some(ref mut set) = self.distinct_set {
                    set.insert(to_key(&scalar));
                }
                match func {
                    AggFunc::Sum | AggFunc::Avg => match scalar {
                        Scalar::Int32(x) => {
                            self.sum_int += x as i64;
                            self.has_sum = true;
                        }
                        Scalar::Int64(x) => {
                            self.sum_int += x;
                            self.has_sum = true;
                        }
                        Scalar::Decimal(x, _) => {
                            self.sum_dec += x;
                            self.has_sum = true;
                        }
                        Scalar::Float64(x) => {
                            self.sum_float += x;
                            self.has_sum = true;
                        }
                        other => bail!("unsupported sum/avg type: {:?}", other),
                    },
                    AggFunc::Min => {
                        if self.min.is_none()
                            || compare_scalars(&scalar, self.min.as_ref().unwrap())?
                                == std::cmp::Ordering::Less
                        {
                            self.min = Some(scalar);
                        }
                    }
                    AggFunc::Max => {
                        if self.max.is_none()
                            || compare_scalars(&scalar, self.max.as_ref().unwrap())?
                                == std::cmp::Ordering::Greater
                        {
                            self.max = Some(scalar);
                        }
                    }
                    AggFunc::Count | AggFunc::CountStar | AggFunc::CountDistinct => {}
                }
            }
        }
        Ok(())
    }

    pub fn merge(&mut self, other: AggState) {
        self.count += other.count;
        self.count_star += other.count_star;
        if let (Some(set), Some(other_set)) = (&mut self.distinct_set, other.distinct_set) {
            set.extend(other_set);
        }
        self.sum_int += other.sum_int;
        self.sum_dec += other.sum_dec;
        self.sum_float += other.sum_float;
        self.has_sum |= other.has_sum;
        if let Some(other_min) = other.min {
            if self.min.is_none()
                || compare_scalars(&other_min, self.min.as_ref().unwrap()).unwrap()
                    == std::cmp::Ordering::Less
            {
                self.min = Some(other_min);
            }
        }
        if let Some(other_max) = other.max {
            if self.max.is_none()
                || compare_scalars(&other_max, self.max.as_ref().unwrap()).unwrap()
                    == std::cmp::Ordering::Greater
            {
                self.max = Some(other_max);
            }
        }
    }

    pub fn finalize(&self, func: AggFunc) -> Scalar {
        match func {
            AggFunc::Count => Scalar::Int64(self.count),
            AggFunc::CountStar => Scalar::Int64(self.count_star),
            AggFunc::CountDistinct => {
                Scalar::Int64(self.distinct_set.as_ref().map_or(0, |s| s.len() as i64))
            }
            AggFunc::Sum => {
                if !self.has_sum {
                    Scalar::Null
                } else if self.is_decimal {
                    Scalar::Decimal(self.sum_dec, self.decimal_scale)
                } else if self.is_int {
                    Scalar::Int64(self.sum_int)
                } else {
                    Scalar::Float64(self.sum_float)
                }
            }
            AggFunc::Avg => {
                if self.count == 0 {
                    Scalar::Null
                } else {
                    let sum_f64 = if self.is_decimal {
                        self.sum_dec as f64 / pow10(self.decimal_scale) as f64
                    } else if self.is_int {
                        self.sum_int as f64
                    } else {
                        self.sum_float
                    };
                    Scalar::Float64(sum_f64 / self.count as f64)
                }
            }
            AggFunc::Min => self.min.clone().unwrap_or(Scalar::Null),
            AggFunc::Max => self.max.clone().unwrap_or(Scalar::Null),
        }
    }
}

fn compare_scalars(a: &Scalar, b: &Scalar) -> Result<std::cmp::Ordering> {
    match (a, b) {
        (Scalar::Int32(x), Scalar::Int32(y)) => Ok(x.cmp(y)),
        (Scalar::Int64(x), Scalar::Int64(y)) => Ok(x.cmp(y)),
        (Scalar::Int32(x), Scalar::Int64(y)) => Ok((*x as i64).cmp(y)),
        (Scalar::Int64(x), Scalar::Int32(y)) => Ok(x.cmp(&(*y as i64))),
        (Scalar::Float64(x), Scalar::Float64(y)) => {
            Ok(x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal))
        }
        (Scalar::Date32(x), Scalar::Date32(y)) => Ok(x.cmp(y)),
        (Scalar::Utf8(x), Scalar::Utf8(y)) => Ok(x.cmp(y)),
        (Scalar::Bool(x), Scalar::Bool(y)) => Ok(x.cmp(y)),
        (Scalar::Decimal(v1, s1), Scalar::Decimal(v2, s2)) => {
            let target_s = (*s1).max(*s2);
            let r1 = v1 * pow10(target_s - s1);
            let r2 = v2 * pow10(target_s - s2);
            Ok(r1.cmp(&r2))
        }
        _ => bail!("cannot compare {:?} and {:?}", a, b),
    }
}

pub fn execute_aggregate(
    input: &LogicalPlan,
    group_by: &[(Expr, String)],
    aggs: &[(AggExpr, String)],
    catalog: &Catalog,
    threads: usize,
) -> Result<Vec<Batch>> {
    let input_batches = execute(input, catalog, threads)?;
    let in_schema = input.schema()?;

    let agg_arg_types: Vec<Option<DataType>> = aggs
        .iter()
        .map(|(agg, _)| {
            agg.arg
                .as_ref()
                .map(|e| e.data_type(&in_schema))
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;

    let morsels = create_morsels(&input_batches);
    let num_threads = threads.max(1);

    let queue = Arc::new(Mutex::new(VecDeque::from(morsels)));
    let thread_maps: Arc<Mutex<Vec<AggMap>>> = Arc::new(Mutex::new(Vec::new()));
    let error_cell = Arc::new(Mutex::new(None));

    std::thread::scope(|s| {
        for _ in 0..num_threads {
            let q = Arc::clone(&queue);
            let t_maps = Arc::clone(&thread_maps);
            let err_cell = Arc::clone(&error_cell);
            let arg_types = &agg_arg_types;

            s.spawn(move || {
                let mut local_map: HashMap<Vec<ScalarKey>, Vec<AggState>> = HashMap::new();
                loop {
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

                    for batch in &morsel.batches {
                        let group_cols: Vec<Column> = match group_by
                            .iter()
                            .map(|(e, _)| eval(e, batch))
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

                        let agg_cols: Vec<Option<Column>> = match aggs
                            .iter()
                            .map(|(agg, _)| agg.arg.as_ref().map(|e| eval(e, batch)).transpose())
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

                        for row_idx in 0..batch.len {
                            let key: Vec<ScalarKey> =
                                group_cols.iter().map(|c| to_key(&c.get(row_idx))).collect();

                            let states = local_map.entry(key).or_insert_with(|| {
                                aggs.iter()
                                    .enumerate()
                                    .map(|(idx, (agg, _))| AggState::new(agg.func, arg_types[idx]))
                                    .collect()
                            });

                            for (agg_idx, (agg, _)) in aggs.iter().enumerate() {
                                let val = agg_cols[agg_idx].as_ref().map(|c| c.get(row_idx));
                                if let Err(e) = states[agg_idx].update(agg.func, val) {
                                    let mut locked_err = err_cell.lock().unwrap();
                                    if locked_err.is_none() {
                                        *locked_err = Some(e);
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }
                t_maps.lock().unwrap().push(local_map);
            });
        }
    });

    if let Some(err) = Arc::try_unwrap(error_cell).unwrap().into_inner().unwrap() {
        return Err(err);
    }

    // Merge maps across threads
    let maps = Arc::try_unwrap(thread_maps).unwrap().into_inner().unwrap();
    let mut merged_map: HashMap<Vec<ScalarKey>, Vec<AggState>> = HashMap::new();

    for map in maps {
        for (key, states) in map {
            match merged_map.entry(key) {
                std::collections::hash_map::Entry::Occupied(mut occ) => {
                    let cur = occ.get_mut();
                    for (i, st) in states.into_iter().enumerate() {
                        cur[i].merge(st);
                    }
                }
                std::collections::hash_map::Entry::Vacant(vac) => {
                    vac.insert(states);
                }
            }
        }
    }

    // If global aggregate and input was empty: create one row
    if group_by.is_empty() && merged_map.is_empty() {
        let default_states: Vec<AggState> = aggs
            .iter()
            .enumerate()
            .map(|(idx, (agg, _))| AggState::new(agg.func, agg_arg_types[idx]))
            .collect();
        merged_map.insert(Vec::new(), default_states);
    }

    // Build output batches
    let out_plan = LogicalPlan::Aggregate {
        input: Box::new(input.clone()),
        group_by: group_by.to_vec(),
        aggs: aggs.to_vec(),
    };
    let out_schema = Arc::new(out_plan.schema()?);

    if merged_map.is_empty() {
        return Ok(Vec::new());
    }

    // Sort keys for deterministic output
    let mut entries: Vec<(Vec<ScalarKey>, Vec<AggState>)> = merged_map.into_iter().collect();
    // Deterministic sort:
    entries.sort_by(|a, b| {
        for (ka, kb) in a.0.iter().zip(b.0.iter()) {
            let sa = to_scalar(ka);
            let sb = to_scalar(kb);
            let ord = compare_scalars(&sa, &sb).unwrap_or(std::cmp::Ordering::Equal);
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });

    let total_rows = entries.len();
    let num_group_cols = group_by.len();
    let num_agg_cols = aggs.len();
    let total_cols = num_group_cols + num_agg_cols;

    let mut batches = Vec::new();
    let mut offset = 0;

    while offset < total_rows {
        let chunk_len = (total_rows - offset).min(BATCH_ROWS);
        let slice = &entries[offset..offset + chunk_len];

        let mut col_scalars: Vec<Vec<Scalar>> = vec![Vec::with_capacity(chunk_len); total_cols];
        for (key, states) in slice {
            for (g_idx, k) in key.iter().enumerate() {
                col_scalars[g_idx].push(to_scalar(k));
            }
            for (agg_idx, (agg, _)) in aggs.iter().enumerate() {
                let s = states[agg_idx].finalize(agg.func);
                col_scalars[num_group_cols + agg_idx].push(s);
            }
        }

        let mut columns = Vec::with_capacity(total_cols);
        for (col_idx, field) in out_schema.fields.iter().enumerate() {
            let col = Column::from_scalars(field.dtype, &col_scalars[col_idx]);
            columns.push(Arc::new(col));
        }

        batches.push(Batch::new(Arc::clone(&out_schema), columns));
        offset += chunk_len;
    }

    Ok(batches)
}
