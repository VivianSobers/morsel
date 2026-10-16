use std::collections::HashMap;

use morsel_core::{DataType, Scalar};
use morsel_exec::catalog::Catalog;
use rand::rngs::StdRng;
use rand::Rng;

pub struct ValueSampler {
    // (lowercase_table_name, col_idx) -> list of sample scalars
    samples: HashMap<(String, usize), Vec<Scalar>>,
}

impl ValueSampler {
    pub fn new(catalog: &Catalog) -> Self {
        let mut samples = HashMap::new();

        for (tbl_name, table) in &catalog.tables {
            let lower_name = tbl_name.to_lowercase();
            let col_count = table.schema.fields.len();

            for col_idx in 0..col_count {
                let mut distinct_vals: Vec<Scalar> = Vec::new();
                'batch_loop: for batch in &table.batches {
                    let col = &batch.columns[col_idx];
                    for row_idx in 0..col.len() {
                        if !col.is_null(row_idx) {
                            let s = col.get(row_idx);
                            if !distinct_vals.contains(&s) {
                                distinct_vals.push(s);
                                if distinct_vals.len() >= 50 {
                                    break 'batch_loop;
                                }
                            }
                        }
                    }
                }
                samples.insert((lower_name.clone(), col_idx), distinct_vals);
            }
        }

        Self { samples }
    }

    pub fn sample_value(&self, table: &str, col_idx: usize, rng: &mut StdRng) -> Option<Scalar> {
        let key = (table.to_lowercase(), col_idx);
        if let Some(vals) = self.samples.get(&key) {
            if !vals.is_empty() {
                let idx = rng.gen_range(0..vals.len());
                return Some(vals[idx].clone());
            }
        }
        None
    }

    pub fn sample_distinct_values(
        &self,
        table: &str,
        col_idx: usize,
        count: usize,
        rng: &mut StdRng,
    ) -> Vec<Scalar> {
        let key = (table.to_lowercase(), col_idx);
        if let Some(vals) = self.samples.get(&key) {
            if !vals.is_empty() {
                let n = count.min(vals.len());
                let mut indices: Vec<usize> = (0..vals.len()).collect();
                // partial shuffle
                for i in 0..n {
                    let j = rng.gen_range(i..vals.len());
                    indices.swap(i, j);
                }
                return indices[0..n].iter().map(|&i| vals[i].clone()).collect();
            }
        }
        Vec::new()
    }

    pub fn sample_literal(
        &self,
        table: &str,
        col_idx: usize,
        dtype: DataType,
        rng: &mut StdRng,
    ) -> Scalar {
        if let Some(s) = self.sample_value(table, col_idx, rng) {
            return s;
        }

        match dtype {
            DataType::Bool => Scalar::Bool(rng.gen()),
            DataType::Int32 => Scalar::Int32(rng.gen_range(1..100)),
            DataType::Int64 => Scalar::Int64(rng.gen_range(1..1000)),
            DataType::Float64 => Scalar::Float64(rng.gen_range(1.0..1000.0)),
            DataType::Decimal { scale, .. } => {
                let factor = 10i128.pow(scale as u32);
                let val = (rng.gen_range(1..1000) as i128) * factor;
                Scalar::Decimal(val, scale)
            }
            DataType::Date32 => Scalar::Date32(rng.gen_range(8000..10500)),
            DataType::Utf8 => Scalar::Utf8("SAMPLE".to_string()),
        }
    }
}
