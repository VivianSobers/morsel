use morsel_core::{Column, ColumnData, DataType, Scalar};
use morsel_plan::BinOp;

fn combine_validity(
    val1: Option<&[bool]>,
    val2: Option<&[bool]>,
    len: usize,
) -> (Option<Vec<bool>>, bool) {
    match (val1, val2) {
        (None, None) => (None, false),
        (Some(v1), None) => {
            let has_null = v1.iter().any(|&b| !b);
            if has_null {
                (Some(v1.to_vec()), true)
            } else {
                (None, false)
            }
        }
        (None, Some(v2)) => {
            let has_null = v2.iter().any(|&b| !b);
            if has_null {
                (Some(v2.to_vec()), true)
            } else {
                (None, false)
            }
        }
        (Some(v1), Some(v2)) => {
            let mut val = Vec::with_capacity(len);
            let mut has_null = false;
            for i in 0..len {
                let b = v1[i] && v2[i];
                if !b {
                    has_null = true;
                }
                val.push(b);
            }
            if has_null {
                (Some(val), true)
            } else {
                (None, false)
            }
        }
    }
}

fn finalize_result(mut data: Vec<bool>, validity_opt: Option<Vec<bool>>, has_null: bool) -> Column {
    if has_null {
        if let Some(ref val) = validity_opt {
            for (i, &valid) in val.iter().enumerate() {
                if !valid {
                    data[i] = false;
                }
            }
        }
    }
    Column {
        dtype: DataType::Bool,
        data: ColumnData::Bool(data),
        validity: if has_null { validity_opt } else { None },
    }
}

fn cmp_slice_slice<T: PartialOrd + Copy>(op: BinOp, s1: &[T], s2: &[T]) -> Option<Vec<bool>> {
    let len = s1.len();
    let mut data = Vec::with_capacity(len);
    match op {
        BinOp::Eq => {
            for i in 0..len {
                data.push(s1[i] == s2[i]);
            }
        }
        BinOp::NotEq => {
            for i in 0..len {
                data.push(s1[i] != s2[i]);
            }
        }
        BinOp::Lt => {
            for i in 0..len {
                data.push(s1[i] < s2[i]);
            }
        }
        BinOp::LtEq => {
            for i in 0..len {
                data.push(s1[i] <= s2[i]);
            }
        }
        BinOp::Gt => {
            for i in 0..len {
                data.push(s1[i] > s2[i]);
            }
        }
        BinOp::GtEq => {
            for i in 0..len {
                data.push(s1[i] >= s2[i]);
            }
        }
        _ => return None,
    }
    Some(data)
}

fn cmp_slice_lit<T: PartialOrd + Copy>(op: BinOp, s: &[T], lit: T) -> Option<Vec<bool>> {
    let len = s.len();
    let mut data = Vec::with_capacity(len);
    match op {
        BinOp::Eq => {
            for &a in s {
                data.push(a == lit);
            }
        }
        BinOp::NotEq => {
            for &a in s {
                data.push(a != lit);
            }
        }
        BinOp::Lt => {
            for &a in s {
                data.push(a < lit);
            }
        }
        BinOp::LtEq => {
            for &a in s {
                data.push(a <= lit);
            }
        }
        BinOp::Gt => {
            for &a in s {
                data.push(a > lit);
            }
        }
        BinOp::GtEq => {
            for &a in s {
                data.push(a >= lit);
            }
        }
        _ => return None,
    }
    Some(data)
}

fn cmp_f64_slice_slice(op: BinOp, s1: &[f64], s2: &[f64]) -> Option<Vec<bool>> {
    let len = s1.len();
    let mut data = Vec::with_capacity(len);
    for i in 0..len {
        let a = s1[i];
        let b = s2[i];
        let r = if a.is_nan() || b.is_nan() {
            op == BinOp::NotEq
        } else {
            match op {
                BinOp::Eq => a == b,
                BinOp::NotEq => a != b,
                BinOp::Lt => a < b,
                BinOp::LtEq => a <= b,
                BinOp::Gt => a > b,
                BinOp::GtEq => a >= b,
                _ => return None,
            }
        };
        data.push(r);
    }
    Some(data)
}

fn cmp_f64_slice_lit(op: BinOp, s: &[f64], lit: f64) -> Option<Vec<bool>> {
    let len = s.len();
    let mut data = Vec::with_capacity(len);
    if lit.is_nan() {
        let r = op == BinOp::NotEq;
        data.resize(len, r);
    } else {
        for &a in s {
            let r = if a.is_nan() {
                op == BinOp::NotEq
            } else {
                match op {
                    BinOp::Eq => a == lit,
                    BinOp::NotEq => a != lit,
                    BinOp::Lt => a < lit,
                    BinOp::LtEq => a <= lit,
                    BinOp::Gt => a > lit,
                    BinOp::GtEq => a >= lit,
                    _ => return None,
                }
            };
            data.push(r);
        }
    }
    Some(data)
}

fn cmp_utf8_slice_slice(
    op: BinOp,
    o1: &[u32],
    b1: &[u8],
    o2: &[u32],
    b2: &[u8],
    len: usize,
) -> Option<Vec<bool>> {
    let mut data = Vec::with_capacity(len);
    match op {
        BinOp::Eq => {
            for i in 0..len {
                let s1 = &b1[o1[i] as usize..o1[i + 1] as usize];
                let s2 = &b2[o2[i] as usize..o2[i + 1] as usize];
                data.push(s1 == s2);
            }
        }
        BinOp::NotEq => {
            for i in 0..len {
                let s1 = &b1[o1[i] as usize..o1[i + 1] as usize];
                let s2 = &b2[o2[i] as usize..o2[i + 1] as usize];
                data.push(s1 != s2);
            }
        }
        _ => return None,
    }
    Some(data)
}

fn cmp_utf8_slice_lit(
    op: BinOp,
    o: &[u32],
    b: &[u8],
    lit_bytes: &[u8],
    len: usize,
) -> Option<Vec<bool>> {
    let mut data = Vec::with_capacity(len);
    match op {
        BinOp::Eq => {
            for i in 0..len {
                let s = &b[o[i] as usize..o[i + 1] as usize];
                data.push(s == lit_bytes);
            }
        }
        BinOp::NotEq => {
            for i in 0..len {
                let s = &b[o[i] as usize..o[i + 1] as usize];
                data.push(s != lit_bytes);
            }
        }
        _ => return None,
    }
    Some(data)
}

pub fn eval_comparison_col_col_fast(op: BinOp, left: &Column, right: &Column) -> Option<Column> {
    let len = left.len();
    if len != right.len() {
        return None;
    }

    let data = match (&left.data, &right.data) {
        (ColumnData::Int32(s1), ColumnData::Int32(s2)) => cmp_slice_slice(op, s1, s2)?,
        (ColumnData::Int64(s1), ColumnData::Int64(s2)) => cmp_slice_slice(op, s1, s2)?,
        (ColumnData::Float64(s1), ColumnData::Float64(s2)) => cmp_f64_slice_slice(op, s1, s2)?,
        (ColumnData::Date32(s1), ColumnData::Date32(s2)) => cmp_slice_slice(op, s1, s2)?,
        (ColumnData::Decimal(s1), ColumnData::Decimal(s2)) => {
            let scale1 = match left.dtype {
                DataType::Decimal { scale, .. } => scale,
                _ => 0,
            };
            let scale2 = match right.dtype {
                DataType::Decimal { scale, .. } => scale,
                _ => 0,
            };
            if scale1 != scale2 {
                return None;
            }
            cmp_slice_slice(op, s1, s2)?
        }
        (
            ColumnData::Utf8 {
                offsets: o1,
                bytes: b1,
            },
            ColumnData::Utf8 {
                offsets: o2,
                bytes: b2,
            },
        ) => cmp_utf8_slice_slice(op, o1, b1, o2, b2, len)?,
        _ => return None,
    };

    let (validity, has_null) =
        combine_validity(left.validity.as_deref(), right.validity.as_deref(), len);
    Some(finalize_result(data, validity, has_null))
}

pub fn eval_comparison_col_lit_fast(op: BinOp, col: &Column, lit: &Scalar) -> Option<Column> {
    let len = col.len();
    if matches!(lit, Scalar::Null) {
        return Some(Column {
            dtype: DataType::Bool,
            data: ColumnData::Bool(vec![false; len]),
            validity: Some(vec![false; len]),
        });
    }

    let data = match (&col.data, lit) {
        (ColumnData::Int32(s), Scalar::Int32(lit_val)) => cmp_slice_lit(op, s, *lit_val)?,
        (ColumnData::Int64(s), Scalar::Int64(lit_val)) => cmp_slice_lit(op, s, *lit_val)?,
        (ColumnData::Float64(s), Scalar::Float64(lit_val)) => cmp_f64_slice_lit(op, s, *lit_val)?,
        (ColumnData::Date32(s), Scalar::Date32(lit_val)) => cmp_slice_lit(op, s, *lit_val)?,
        (ColumnData::Decimal(s), Scalar::Decimal(lit_val, lit_scale)) => {
            let col_scale = match col.dtype {
                DataType::Decimal { scale, .. } => scale,
                _ => 0,
            };
            if col_scale != *lit_scale {
                return None;
            }
            cmp_slice_lit(op, s, *lit_val)?
        }
        (ColumnData::Utf8 { offsets, bytes }, Scalar::Utf8(lit_str)) => {
            cmp_utf8_slice_lit(op, offsets, bytes, lit_str.as_bytes(), len)?
        }
        _ => return None,
    };

    let has_null = col.validity.as_ref().is_some_and(|v| v.iter().any(|&b| !b));
    let validity = if has_null { col.validity.clone() } else { None };
    Some(finalize_result(data, validity, has_null))
}

pub fn eval_comparison_lit_col_fast(op: BinOp, lit: &Scalar, col: &Column) -> Option<Column> {
    let flipped_op = match op {
        BinOp::Eq => BinOp::Eq,
        BinOp::NotEq => BinOp::NotEq,
        BinOp::Lt => BinOp::Gt,
        BinOp::LtEq => BinOp::GtEq,
        BinOp::Gt => BinOp::Lt,
        BinOp::GtEq => BinOp::LtEq,
        _ => return None,
    };
    eval_comparison_col_lit_fast(flipped_op, col, lit)
}
