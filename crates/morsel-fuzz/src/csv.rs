use std::fs::File;
use std::io::Write;
use std::path::Path;

use anyhow::Result;
use morsel_core::{Batch, Scalar, Schema};

pub fn scalar_to_csv_string(s: &Scalar) -> String {
    match s {
        Scalar::Null => "".to_string(),
        Scalar::Bool(b) => {
            if *b {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        Scalar::Int32(x) => x.to_string(),
        Scalar::Int64(x) => x.to_string(),
        Scalar::Float64(f) => format!("{}", f),
        Scalar::Decimal(v, scale) => {
            if *scale == 0 {
                v.to_string()
            } else {
                let sign = if *v < 0 { "-" } else { "" };
                let v_abs = v.abs();
                let div = 10i128.pow(*scale as u32);
                let whole = v_abs / div;
                let frac = v_abs % div;
                format!(
                    "{}{}.{:0width$}",
                    sign,
                    whole,
                    frac,
                    width = *scale as usize
                )
            }
        }
        Scalar::Date32(days) => {
            let secs = *days as i64 * 86400;
            if let Some(dt) = chrono::DateTime::from_timestamp(secs, 0) {
                dt.format("%Y-%m-%d").to_string()
            } else {
                days.to_string()
            }
        }
        Scalar::Utf8(st) => {
            if st.contains(',') || st.contains('"') || st.contains('\n') || st.contains('\r') {
                format!("\"{}\"", st.replace('"', "\"\""))
            } else {
                st.clone()
            }
        }
    }
}

pub fn write_csv(batches: &[Batch], schema: &Schema, out_path: &Path) -> Result<()> {
    let mut file = File::create(out_path)?;
    let header = schema
        .fields
        .iter()
        .map(|f| f.name.clone())
        .collect::<Vec<_>>()
        .join(",");
    writeln!(file, "{}", header)?;

    for batch in batches {
        for row_idx in 0..batch.len {
            let mut row_strs = Vec::with_capacity(batch.columns.len());
            for col in &batch.columns {
                let s = col.get(row_idx);
                row_strs.push(scalar_to_csv_string(&s));
            }
            writeln!(file, "{}", row_strs.join(","))?;
        }
    }

    Ok(())
}
