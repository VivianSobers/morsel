use anyhow::Result;
use morsel_core::{Column, ColumnData, DataType, Scalar};
use morsel_plan::BinOp;

fn checked_pow10(n: u8) -> Result<i128> {
    10i128
        .checked_pow(n as u32)
        .ok_or_else(|| anyhow::anyhow!("decimal power overflow: 10^{}", n))
}

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

fn zero_nulls_i64(data: &mut [i64], validity: Option<&[bool]>) {
    if let Some(val) = validity {
        for (i, &v) in val.iter().enumerate() {
            if !v {
                data[i] = 0;
            }
        }
    }
}

fn zero_nulls_f64(data: &mut [f64], validity: Option<&[bool]>) {
    if let Some(val) = validity {
        for (i, &v) in val.iter().enumerate() {
            if !v {
                data[i] = 0.0;
            }
        }
    }
}

pub fn eval_arithmetic_col_col_fast(
    op: BinOp,
    left: &Column,
    right: &Column,
) -> Result<Option<Column>> {
    let len = left.len();
    if len != right.len() {
        return Ok(None);
    }
    if !matches!(op, BinOp::Add | BinOp::Sub | BinOp::Mul) {
        return Ok(None);
    }

    match (&left.data, &right.data) {
        (ColumnData::Int64(s1), ColumnData::Int64(s2)) => {
            let (validity, has_null) =
                combine_validity(left.validity.as_deref(), right.validity.as_deref(), len);
            let mut data = Vec::with_capacity(len);
            match op {
                BinOp::Add => {
                    for i in 0..len {
                        data.push(s1[i] + s2[i]);
                    }
                }
                BinOp::Sub => {
                    for i in 0..len {
                        data.push(s1[i] - s2[i]);
                    }
                }
                BinOp::Mul => {
                    for i in 0..len {
                        data.push(s1[i] * s2[i]);
                    }
                }
                _ => unreachable!(),
            }
            if has_null {
                zero_nulls_i64(&mut data, validity.as_deref());
            }
            Ok(Some(Column {
                dtype: DataType::Int64,
                data: ColumnData::Int64(data),
                validity: if has_null { validity } else { None },
            }))
        }
        (ColumnData::Float64(s1), ColumnData::Float64(s2)) => {
            let (validity, has_null) =
                combine_validity(left.validity.as_deref(), right.validity.as_deref(), len);
            let mut data = Vec::with_capacity(len);
            match op {
                BinOp::Add => {
                    for i in 0..len {
                        data.push(s1[i] + s2[i]);
                    }
                }
                BinOp::Sub => {
                    for i in 0..len {
                        data.push(s1[i] - s2[i]);
                    }
                }
                BinOp::Mul => {
                    for i in 0..len {
                        data.push(s1[i] * s2[i]);
                    }
                }
                _ => unreachable!(),
            }
            if has_null {
                zero_nulls_f64(&mut data, validity.as_deref());
            }
            Ok(Some(Column {
                dtype: DataType::Float64,
                data: ColumnData::Float64(data),
                validity: if has_null { validity } else { None },
            }))
        }
        (ColumnData::Decimal(v1), ColumnData::Decimal(v2)) => {
            let s1 = match left.dtype {
                DataType::Decimal { scale, .. } => scale,
                _ => 0,
            };
            let s2 = match right.dtype {
                DataType::Decimal { scale, .. } => scale,
                _ => 0,
            };

            let (validity, has_null) =
                combine_validity(left.validity.as_deref(), right.validity.as_deref(), len);
            let mut data = Vec::with_capacity(len);

            match op {
                BinOp::Add => {
                    let out_scale = s1.max(s2);
                    let p1 = checked_pow10(out_scale - s1)?;
                    let p2 = checked_pow10(out_scale - s2)?;
                    for i in 0..len {
                        if has_null && validity.as_ref().is_some_and(|v| !v[i]) {
                            data.push(0);
                            continue;
                        }
                        let r1 = v1[i]
                            .checked_mul(p1)
                            .ok_or_else(|| anyhow::anyhow!("decimal addition overflow"))?;
                        let r2 = v2[i]
                            .checked_mul(p2)
                            .ok_or_else(|| anyhow::anyhow!("decimal addition overflow"))?;
                        let sum = r1
                            .checked_add(r2)
                            .ok_or_else(|| anyhow::anyhow!("decimal addition overflow"))?;
                        data.push(sum);
                    }
                    Ok(Some(Column {
                        dtype: DataType::Decimal {
                            precision: 38,
                            scale: out_scale,
                        },
                        data: ColumnData::Decimal(data),
                        validity: if has_null { validity } else { None },
                    }))
                }
                BinOp::Sub => {
                    let out_scale = s1.max(s2);
                    let p1 = checked_pow10(out_scale - s1)?;
                    let p2 = checked_pow10(out_scale - s2)?;
                    for i in 0..len {
                        if has_null && validity.as_ref().is_some_and(|v| !v[i]) {
                            data.push(0);
                            continue;
                        }
                        let r1 = v1[i]
                            .checked_mul(p1)
                            .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                        let r2 = v2[i]
                            .checked_mul(p2)
                            .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                        let diff = r1
                            .checked_sub(r2)
                            .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                        data.push(diff);
                    }
                    Ok(Some(Column {
                        dtype: DataType::Decimal {
                            precision: 38,
                            scale: out_scale,
                        },
                        data: ColumnData::Decimal(data),
                        validity: if has_null { validity } else { None },
                    }))
                }
                BinOp::Mul => {
                    let out_scale = s1 + s2;
                    for i in 0..len {
                        if has_null && validity.as_ref().is_some_and(|v| !v[i]) {
                            data.push(0);
                            continue;
                        }
                        let prod = v1[i]
                            .checked_mul(v2[i])
                            .ok_or_else(|| anyhow::anyhow!("decimal multiplication overflow"))?;
                        data.push(prod);
                    }
                    Ok(Some(Column {
                        dtype: DataType::Decimal {
                            precision: 38,
                            scale: out_scale,
                        },
                        data: ColumnData::Decimal(data),
                        validity: if has_null { validity } else { None },
                    }))
                }
                _ => unreachable!(),
            }
        }
        _ => Ok(None),
    }
}

pub fn eval_arithmetic_col_lit_fast(
    op: BinOp,
    col: &Column,
    lit: &Scalar,
) -> Result<Option<Column>> {
    let len = col.len();
    if !matches!(op, BinOp::Add | BinOp::Sub | BinOp::Mul) {
        return Ok(None);
    }
    if matches!(lit, Scalar::Null) {
        let out_dtype = match &col.dtype {
            DataType::Decimal { scale, .. } => DataType::Decimal {
                precision: 38,
                scale: *scale,
            },
            other => *other,
        };
        let dummy_data = match out_dtype {
            DataType::Int64 => ColumnData::Int64(vec![0; len]),
            DataType::Float64 => ColumnData::Float64(vec![0.0; len]),
            DataType::Decimal { .. } => ColumnData::Decimal(vec![0; len]),
            DataType::Int32 => ColumnData::Int32(vec![0; len]),
            _ => return Ok(None),
        };
        return Ok(Some(Column {
            dtype: out_dtype,
            data: dummy_data,
            validity: Some(vec![false; len]),
        }));
    }

    let has_null = col.validity.as_ref().is_some_and(|v| v.iter().any(|&b| !b));
    let validity = if has_null { col.validity.clone() } else { None };

    match (&col.data, lit) {
        (ColumnData::Int64(s), Scalar::Int64(lit_val)) => {
            let mut data = Vec::with_capacity(len);
            let c = *lit_val;
            match op {
                BinOp::Add => {
                    for &x in s {
                        data.push(x + c);
                    }
                }
                BinOp::Sub => {
                    for &x in s {
                        data.push(x - c);
                    }
                }
                BinOp::Mul => {
                    for &x in s {
                        data.push(x * c);
                    }
                }
                _ => unreachable!(),
            }
            if has_null {
                zero_nulls_i64(&mut data, validity.as_deref());
            }
            Ok(Some(Column {
                dtype: DataType::Int64,
                data: ColumnData::Int64(data),
                validity,
            }))
        }
        (ColumnData::Int64(s), Scalar::Int32(lit_val)) => {
            let mut data = Vec::with_capacity(len);
            let c = *lit_val as i64;
            match op {
                BinOp::Add => {
                    for &x in s {
                        data.push(x + c);
                    }
                }
                BinOp::Sub => {
                    for &x in s {
                        data.push(x - c);
                    }
                }
                BinOp::Mul => {
                    for &x in s {
                        data.push(x * c);
                    }
                }
                _ => unreachable!(),
            }
            if has_null {
                zero_nulls_i64(&mut data, validity.as_deref());
            }
            Ok(Some(Column {
                dtype: DataType::Int64,
                data: ColumnData::Int64(data),
                validity,
            }))
        }
        (ColumnData::Float64(s), Scalar::Float64(lit_val)) => {
            let mut data = Vec::with_capacity(len);
            let c = *lit_val;
            match op {
                BinOp::Add => {
                    for &x in s {
                        data.push(x + c);
                    }
                }
                BinOp::Sub => {
                    for &x in s {
                        data.push(x - c);
                    }
                }
                BinOp::Mul => {
                    for &x in s {
                        data.push(x * c);
                    }
                }
                _ => unreachable!(),
            }
            if has_null {
                zero_nulls_f64(&mut data, validity.as_deref());
            }
            Ok(Some(Column {
                dtype: DataType::Float64,
                data: ColumnData::Float64(data),
                validity,
            }))
        }
        (ColumnData::Decimal(v), Scalar::Decimal(lit_v, lit_scale)) => {
            let col_scale = match col.dtype {
                DataType::Decimal { scale, .. } => scale,
                _ => 0,
            };
            let mut data = Vec::with_capacity(len);
            match op {
                BinOp::Add => {
                    let out_scale = col_scale.max(*lit_scale);
                    let p_col = checked_pow10(out_scale - col_scale)?;
                    let p_lit = checked_pow10(out_scale - lit_scale)?;
                    let scaled_lit = lit_v
                        .checked_mul(p_lit)
                        .ok_or_else(|| anyhow::anyhow!("decimal addition overflow"))?;
                    for i in 0..len {
                        if has_null && validity.as_ref().is_some_and(|val| !val[i]) {
                            data.push(0);
                            continue;
                        }
                        let r1 = v[i]
                            .checked_mul(p_col)
                            .ok_or_else(|| anyhow::anyhow!("decimal addition overflow"))?;
                        let sum = r1
                            .checked_add(scaled_lit)
                            .ok_or_else(|| anyhow::anyhow!("decimal addition overflow"))?;
                        data.push(sum);
                    }
                    Ok(Some(Column {
                        dtype: DataType::Decimal {
                            precision: 38,
                            scale: out_scale,
                        },
                        data: ColumnData::Decimal(data),
                        validity,
                    }))
                }
                BinOp::Sub => {
                    let out_scale = col_scale.max(*lit_scale);
                    let p_col = checked_pow10(out_scale - col_scale)?;
                    let p_lit = checked_pow10(out_scale - lit_scale)?;
                    let scaled_lit = lit_v
                        .checked_mul(p_lit)
                        .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                    for i in 0..len {
                        if has_null && validity.as_ref().is_some_and(|val| !val[i]) {
                            data.push(0);
                            continue;
                        }
                        let r1 = v[i]
                            .checked_mul(p_col)
                            .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                        let diff = r1
                            .checked_sub(scaled_lit)
                            .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                        data.push(diff);
                    }
                    Ok(Some(Column {
                        dtype: DataType::Decimal {
                            precision: 38,
                            scale: out_scale,
                        },
                        data: ColumnData::Decimal(data),
                        validity,
                    }))
                }
                BinOp::Mul => {
                    let out_scale = col_scale + lit_scale;
                    let c = *lit_v;
                    for i in 0..len {
                        if has_null && validity.as_ref().is_some_and(|val| !val[i]) {
                            data.push(0);
                            continue;
                        }
                        let prod = v[i]
                            .checked_mul(c)
                            .ok_or_else(|| anyhow::anyhow!("decimal multiplication overflow"))?;
                        data.push(prod);
                    }
                    Ok(Some(Column {
                        dtype: DataType::Decimal {
                            precision: 38,
                            scale: out_scale,
                        },
                        data: ColumnData::Decimal(data),
                        validity,
                    }))
                }
                _ => unreachable!(),
            }
        }
        (ColumnData::Decimal(v), Scalar::Int32(x)) => {
            let int_val = *x as i128;
            let col_scale = match col.dtype {
                DataType::Decimal { scale, .. } => scale,
                _ => 0,
            };
            let mut data = Vec::with_capacity(len);
            match op {
                BinOp::Add => {
                    let p = checked_pow10(col_scale)?;
                    let scaled = int_val
                        .checked_mul(p)
                        .ok_or_else(|| anyhow::anyhow!("decimal addition overflow"))?;
                    for i in 0..len {
                        if has_null && validity.as_ref().is_some_and(|val| !val[i]) {
                            data.push(0);
                            continue;
                        }
                        let sum = v[i]
                            .checked_add(scaled)
                            .ok_or_else(|| anyhow::anyhow!("decimal addition overflow"))?;
                        data.push(sum);
                    }
                    Ok(Some(Column {
                        dtype: col.dtype,
                        data: ColumnData::Decimal(data),
                        validity,
                    }))
                }
                BinOp::Sub => {
                    let p = checked_pow10(col_scale)?;
                    let scaled = int_val
                        .checked_mul(p)
                        .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                    for i in 0..len {
                        if has_null && validity.as_ref().is_some_and(|val| !val[i]) {
                            data.push(0);
                            continue;
                        }
                        let diff = v[i]
                            .checked_sub(scaled)
                            .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                        data.push(diff);
                    }
                    Ok(Some(Column {
                        dtype: col.dtype,
                        data: ColumnData::Decimal(data),
                        validity,
                    }))
                }
                BinOp::Mul => {
                    for i in 0..len {
                        if has_null && validity.as_ref().is_some_and(|val| !val[i]) {
                            data.push(0);
                            continue;
                        }
                        let prod = v[i]
                            .checked_mul(int_val)
                            .ok_or_else(|| anyhow::anyhow!("decimal multiplication overflow"))?;
                        data.push(prod);
                    }
                    Ok(Some(Column {
                        dtype: col.dtype,
                        data: ColumnData::Decimal(data),
                        validity,
                    }))
                }
                _ => unreachable!(),
            }
        }
        (ColumnData::Decimal(v), Scalar::Int64(x)) => {
            let int_val = *x as i128;
            let col_scale = match col.dtype {
                DataType::Decimal { scale, .. } => scale,
                _ => 0,
            };
            let mut data = Vec::with_capacity(len);
            match op {
                BinOp::Add => {
                    let p = checked_pow10(col_scale)?;
                    let scaled = int_val
                        .checked_mul(p)
                        .ok_or_else(|| anyhow::anyhow!("decimal addition overflow"))?;
                    for i in 0..len {
                        if has_null && validity.as_ref().is_some_and(|val| !val[i]) {
                            data.push(0);
                            continue;
                        }
                        let sum = v[i]
                            .checked_add(scaled)
                            .ok_or_else(|| anyhow::anyhow!("decimal addition overflow"))?;
                        data.push(sum);
                    }
                    Ok(Some(Column {
                        dtype: col.dtype,
                        data: ColumnData::Decimal(data),
                        validity,
                    }))
                }
                BinOp::Sub => {
                    let p = checked_pow10(col_scale)?;
                    let scaled = int_val
                        .checked_mul(p)
                        .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                    for i in 0..len {
                        if has_null && validity.as_ref().is_some_and(|val| !val[i]) {
                            data.push(0);
                            continue;
                        }
                        let diff = v[i]
                            .checked_sub(scaled)
                            .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                        data.push(diff);
                    }
                    Ok(Some(Column {
                        dtype: col.dtype,
                        data: ColumnData::Decimal(data),
                        validity,
                    }))
                }
                BinOp::Mul => {
                    for i in 0..len {
                        if has_null && validity.as_ref().is_some_and(|val| !val[i]) {
                            data.push(0);
                            continue;
                        }
                        let prod = v[i]
                            .checked_mul(int_val)
                            .ok_or_else(|| anyhow::anyhow!("decimal multiplication overflow"))?;
                        data.push(prod);
                    }
                    Ok(Some(Column {
                        dtype: col.dtype,
                        data: ColumnData::Decimal(data),
                        validity,
                    }))
                }
                _ => unreachable!(),
            }
        }
        (ColumnData::Int64(s), Scalar::Decimal(lit_v, lit_scale)) => {
            let p = checked_pow10(*lit_scale)?;
            let mut data = Vec::with_capacity(len);
            match op {
                BinOp::Add => {
                    for i in 0..len {
                        if has_null && validity.as_ref().is_some_and(|val| !val[i]) {
                            data.push(0);
                            continue;
                        }
                        let scaled = (s[i] as i128)
                            .checked_mul(p)
                            .ok_or_else(|| anyhow::anyhow!("decimal addition overflow"))?;
                        let sum = scaled
                            .checked_add(*lit_v)
                            .ok_or_else(|| anyhow::anyhow!("decimal addition overflow"))?;
                        data.push(sum);
                    }
                    Ok(Some(Column {
                        dtype: DataType::Decimal {
                            precision: 38,
                            scale: *lit_scale,
                        },
                        data: ColumnData::Decimal(data),
                        validity,
                    }))
                }
                BinOp::Sub => {
                    for i in 0..len {
                        if has_null && validity.as_ref().is_some_and(|val| !val[i]) {
                            data.push(0);
                            continue;
                        }
                        let scaled = (s[i] as i128)
                            .checked_mul(p)
                            .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                        let diff = scaled
                            .checked_sub(*lit_v)
                            .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                        data.push(diff);
                    }
                    Ok(Some(Column {
                        dtype: DataType::Decimal {
                            precision: 38,
                            scale: *lit_scale,
                        },
                        data: ColumnData::Decimal(data),
                        validity,
                    }))
                }
                BinOp::Mul => {
                    for i in 0..len {
                        if has_null && validity.as_ref().is_some_and(|val| !val[i]) {
                            data.push(0);
                            continue;
                        }
                        let prod = (s[i] as i128)
                            .checked_mul(*lit_v)
                            .ok_or_else(|| anyhow::anyhow!("decimal multiplication overflow"))?;
                        data.push(prod);
                    }
                    Ok(Some(Column {
                        dtype: DataType::Decimal {
                            precision: 38,
                            scale: *lit_scale,
                        },
                        data: ColumnData::Decimal(data),
                        validity,
                    }))
                }
                _ => unreachable!(),
            }
        }
        (ColumnData::Int32(s), Scalar::Decimal(lit_v, lit_scale)) => {
            let p = checked_pow10(*lit_scale)?;
            let mut data = Vec::with_capacity(len);
            match op {
                BinOp::Add => {
                    for i in 0..len {
                        if has_null && validity.as_ref().is_some_and(|val| !val[i]) {
                            data.push(0);
                            continue;
                        }
                        let scaled = (s[i] as i128)
                            .checked_mul(p)
                            .ok_or_else(|| anyhow::anyhow!("decimal addition overflow"))?;
                        let sum = scaled
                            .checked_add(*lit_v)
                            .ok_or_else(|| anyhow::anyhow!("decimal addition overflow"))?;
                        data.push(sum);
                    }
                    Ok(Some(Column {
                        dtype: DataType::Decimal {
                            precision: 38,
                            scale: *lit_scale,
                        },
                        data: ColumnData::Decimal(data),
                        validity,
                    }))
                }
                BinOp::Sub => {
                    for i in 0..len {
                        if has_null && validity.as_ref().is_some_and(|val| !val[i]) {
                            data.push(0);
                            continue;
                        }
                        let scaled = (s[i] as i128)
                            .checked_mul(p)
                            .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                        let diff = scaled
                            .checked_sub(*lit_v)
                            .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                        data.push(diff);
                    }
                    Ok(Some(Column {
                        dtype: DataType::Decimal {
                            precision: 38,
                            scale: *lit_scale,
                        },
                        data: ColumnData::Decimal(data),
                        validity,
                    }))
                }
                BinOp::Mul => {
                    for i in 0..len {
                        if has_null && validity.as_ref().is_some_and(|val| !val[i]) {
                            data.push(0);
                            continue;
                        }
                        let prod = (s[i] as i128)
                            .checked_mul(*lit_v)
                            .ok_or_else(|| anyhow::anyhow!("decimal multiplication overflow"))?;
                        data.push(prod);
                    }
                    Ok(Some(Column {
                        dtype: DataType::Decimal {
                            precision: 38,
                            scale: *lit_scale,
                        },
                        data: ColumnData::Decimal(data),
                        validity,
                    }))
                }
                _ => unreachable!(),
            }
        }
        _ => Ok(None),
    }
}

pub fn eval_arithmetic_lit_col_fast(
    op: BinOp,
    lit: &Scalar,
    col: &Column,
) -> Result<Option<Column>> {
    match op {
        BinOp::Add | BinOp::Mul => eval_arithmetic_col_lit_fast(op, col, lit),
        BinOp::Sub => {
            let len = col.len();
            if matches!(lit, Scalar::Null) {
                return eval_arithmetic_col_lit_fast(op, col, lit);
            }
            let has_null = col.validity.as_ref().is_some_and(|v| v.iter().any(|&b| !b));
            let validity = if has_null { col.validity.clone() } else { None };

            match (&col.data, lit) {
                (ColumnData::Int64(s), Scalar::Int64(lit_val)) => {
                    let c = *lit_val;
                    let mut data = Vec::with_capacity(len);
                    for &x in s {
                        data.push(c - x);
                    }
                    Ok(Some(Column {
                        dtype: DataType::Int64,
                        data: ColumnData::Int64(data),
                        validity,
                    }))
                }
                (ColumnData::Float64(s), Scalar::Float64(lit_val)) => {
                    let c = *lit_val;
                    let mut data = Vec::with_capacity(len);
                    for &x in s {
                        data.push(c - x);
                    }
                    Ok(Some(Column {
                        dtype: DataType::Float64,
                        data: ColumnData::Float64(data),
                        validity,
                    }))
                }
                (ColumnData::Decimal(v), Scalar::Decimal(lit_v, lit_scale)) => {
                    let col_scale = match col.dtype {
                        DataType::Decimal { scale, .. } => scale,
                        _ => 0,
                    };
                    let out_scale = col_scale.max(*lit_scale);
                    let p_col = checked_pow10(out_scale - col_scale)?;
                    let p_lit = checked_pow10(out_scale - lit_scale)?;
                    let scaled_lit = lit_v
                        .checked_mul(p_lit)
                        .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                    let mut data = Vec::with_capacity(len);
                    for i in 0..len {
                        if has_null && validity.as_ref().is_some_and(|val| !val[i]) {
                            data.push(0);
                            continue;
                        }
                        let r1 = v[i]
                            .checked_mul(p_col)
                            .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                        let diff = scaled_lit
                            .checked_sub(r1)
                            .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                        data.push(diff);
                    }
                    Ok(Some(Column {
                        dtype: DataType::Decimal {
                            precision: 38,
                            scale: out_scale,
                        },
                        data: ColumnData::Decimal(data),
                        validity,
                    }))
                }
                (ColumnData::Decimal(v), Scalar::Int32(x)) => {
                    let int_val = *x as i128;
                    let col_scale = match col.dtype {
                        DataType::Decimal { scale, .. } => scale,
                        _ => 0,
                    };
                    let p = checked_pow10(col_scale)?;
                    let scaled = int_val
                        .checked_mul(p)
                        .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                    let mut data = Vec::with_capacity(len);
                    for i in 0..len {
                        if has_null && validity.as_ref().is_some_and(|val| !val[i]) {
                            data.push(0);
                            continue;
                        }
                        let diff = scaled
                            .checked_sub(v[i])
                            .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                        data.push(diff);
                    }
                    Ok(Some(Column {
                        dtype: col.dtype,
                        data: ColumnData::Decimal(data),
                        validity,
                    }))
                }
                (ColumnData::Decimal(v), Scalar::Int64(x)) => {
                    let int_val = *x as i128;
                    let col_scale = match col.dtype {
                        DataType::Decimal { scale, .. } => scale,
                        _ => 0,
                    };
                    let p = checked_pow10(col_scale)?;
                    let scaled = int_val
                        .checked_mul(p)
                        .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                    let mut data = Vec::with_capacity(len);
                    for i in 0..len {
                        if has_null && validity.as_ref().is_some_and(|val| !val[i]) {
                            data.push(0);
                            continue;
                        }
                        let diff = scaled
                            .checked_sub(v[i])
                            .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                        data.push(diff);
                    }
                    Ok(Some(Column {
                        dtype: col.dtype,
                        data: ColumnData::Decimal(data),
                        validity,
                    }))
                }
                _ => Ok(None),
            }
        }
        _ => Ok(None),
    }
}
