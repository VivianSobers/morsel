use anyhow::{bail, Result};
use chrono::Datelike;
use morsel_core::{Batch, Column, ColumnData, DataType, Scalar};
use morsel_plan::{binary_op_type, BinOp, DateField, Expr};
use regex::Regex;

fn checked_pow10(n: u8) -> Result<i128> {
    10i128
        .checked_pow(n as u32)
        .ok_or_else(|| anyhow::anyhow!("decimal power overflow: 10^{}", n))
}

fn pow10(n: u8) -> i128 {
    checked_pow10(n).unwrap_or(i128::MAX)
}

fn like_to_regex(pattern: &str) -> Result<Regex> {
    let mut regex_str = String::from("^");
    for ch in pattern.chars() {
        match ch {
            '%' => regex_str.push_str(".*"),
            '_' => regex_str.push('.'),
            '.' | '^' | '$' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '\\' => {
                regex_str.push('\\');
                regex_str.push(ch);
            }
            c => regex_str.push(c),
        }
    }
    regex_str.push('$');
    Regex::new(&regex_str).map_err(|e| anyhow::anyhow!("invalid like pattern: {}", e))
}

fn date32_to_year(days: i32) -> i64 {
    let secs = days as i64 * 86400;
    if let Some(dt) = chrono::DateTime::from_timestamp(secs, 0) {
        dt.year() as i64
    } else {
        // Fallback for extreme dates
        (1970 + days / 365) as i64
    }
}

pub fn eval_predicate(expr: &Expr, batch: &Batch) -> Result<Vec<u32>> {
    let col = eval(expr, batch)?;
    let mut selected = Vec::new();
    if let ColumnData::Bool(ref v) = col.data {
        match &col.validity {
            Some(val) => {
                for (i, &b) in v.iter().enumerate() {
                    if val[i] && b {
                        selected.push(i as u32);
                    }
                }
            }
            None => {
                for (i, &b) in v.iter().enumerate() {
                    if b {
                        selected.push(i as u32);
                    }
                }
            }
        }
    }
    Ok(selected)
}

pub fn eval(expr: &Expr, batch: &Batch) -> Result<Column> {
    let row_count = batch.len;
    match expr {
        Expr::Column(idx) => {
            let col = batch
                .columns
                .get(*idx)
                .ok_or_else(|| anyhow::anyhow!("column index {} out of bounds", idx))?;
            Ok((**col).clone())
        }
        Expr::Literal(scalar) => {
            let (dtype, data, validity) = match scalar {
                Scalar::Null => (
                    DataType::Int32,
                    ColumnData::Int32(vec![0; row_count]),
                    Some(vec![false; row_count]),
                ),
                Scalar::Bool(b) => (DataType::Bool, ColumnData::Bool(vec![*b; row_count]), None),
                Scalar::Int32(x) => (
                    DataType::Int32,
                    ColumnData::Int32(vec![*x; row_count]),
                    None,
                ),
                Scalar::Int64(x) => (
                    DataType::Int64,
                    ColumnData::Int64(vec![*x; row_count]),
                    None,
                ),
                Scalar::Float64(f) => (
                    DataType::Float64,
                    ColumnData::Float64(vec![*f; row_count]),
                    None,
                ),
                Scalar::Decimal(x, scale) => (
                    DataType::Decimal {
                        precision: 38,
                        scale: *scale,
                    },
                    ColumnData::Decimal(vec![*x; row_count]),
                    None,
                ),
                Scalar::Date32(d) => (
                    DataType::Date32,
                    ColumnData::Date32(vec![*d; row_count]),
                    None,
                ),
                Scalar::Utf8(st) => {
                    let b = st.as_bytes();
                    let len = b.len() as u32;
                    let mut offsets = Vec::with_capacity(row_count + 1);
                    offsets.push(0);
                    let mut bytes = Vec::with_capacity(row_count * b.len());
                    for i in 0..row_count {
                        bytes.extend_from_slice(b);
                        offsets.push((i + 1) as u32 * len);
                    }
                    (DataType::Utf8, ColumnData::Utf8 { offsets, bytes }, None)
                }
            };
            Ok(Column {
                dtype,
                data,
                validity,
            })
        }
        Expr::Binary { op, left, right } => {
            let left_col = eval(left, batch)?;
            let right_col = eval(right, batch)?;
            eval_binary(*op, &left_col, &right_col, row_count)
        }
        Expr::Not(sub) => {
            let col = eval(sub, batch)?;
            let mut validity = Vec::with_capacity(row_count);
            let mut data = Vec::with_capacity(row_count);
            let mut has_null = false;
            for i in 0..row_count {
                if col.is_null(i) {
                    has_null = true;
                    validity.push(false);
                    data.push(false);
                } else {
                    validity.push(true);
                    let b = match col.get(i) {
                        Scalar::Bool(b) => b,
                        _ => bail!("NOT requires boolean column"),
                    };
                    data.push(!b);
                }
            }
            Ok(Column {
                dtype: DataType::Bool,
                data: ColumnData::Bool(data),
                validity: if has_null { Some(validity) } else { None },
            })
        }
        Expr::Negate(sub) => {
            let col = eval(sub, batch)?;
            let mut scalars = Vec::with_capacity(row_count);
            for i in 0..row_count {
                if col.is_null(i) {
                    scalars.push(Scalar::Null);
                } else {
                    let s = match col.get(i) {
                        Scalar::Int32(x) => Scalar::Int32(-x),
                        Scalar::Int64(x) => Scalar::Int64(-x),
                        Scalar::Float64(x) => Scalar::Float64(-x),
                        Scalar::Decimal(x, s) => Scalar::Decimal(-x, s),
                        other => bail!("cannot negate {:?}", other),
                    };
                    scalars.push(s);
                }
            }
            Ok(Column::from_scalars(col.dtype, &scalars))
        }
        Expr::IsNull(sub) => {
            let col = eval(sub, batch)?;
            let data = (0..row_count).map(|i| col.is_null(i)).collect();
            Ok(Column {
                dtype: DataType::Bool,
                data: ColumnData::Bool(data),
                validity: None,
            })
        }
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let col = eval(expr, batch)?;
            let regex = like_to_regex(pattern)?;
            let mut data = Vec::with_capacity(row_count);
            let mut validity = Vec::with_capacity(row_count);
            let mut has_null = false;

            for i in 0..row_count {
                if col.is_null(i) {
                    has_null = true;
                    validity.push(false);
                    data.push(false);
                } else {
                    validity.push(true);
                    let s = col.utf8_value(i);
                    let is_match = regex.is_match(s);
                    data.push(if *negated { !is_match } else { is_match });
                }
            }
            Ok(Column {
                dtype: DataType::Bool,
                data: ColumnData::Bool(data),
                validity: if has_null { Some(validity) } else { None },
            })
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let col = eval(expr, batch)?;
            let mut data = Vec::with_capacity(row_count);
            let mut validity = Vec::with_capacity(row_count);
            let mut has_null = false;

            for i in 0..row_count {
                if col.is_null(i) {
                    has_null = true;
                    validity.push(false);
                    data.push(false);
                } else {
                    validity.push(true);
                    let val = col.get(i);
                    let found = list.iter().any(|item| item == &val);
                    data.push(if *negated { !found } else { found });
                }
            }
            Ok(Column {
                dtype: DataType::Bool,
                data: ColumnData::Bool(data),
                validity: if has_null { Some(validity) } else { None },
            })
        }
        Expr::Case { whens, else_expr } => {
            let mut when_conds = Vec::with_capacity(whens.len());
            let mut when_thens = Vec::with_capacity(whens.len());
            for (cond, then) in whens {
                when_conds.push(eval(cond, batch)?);
                when_thens.push(eval(then, batch)?);
            }
            let else_col = if let Some(el) = else_expr {
                Some(eval(el, batch)?)
            } else {
                None
            };

            let out_dtype = if let Some(first_then) = when_thens.first() {
                first_then.dtype
            } else if let Some(ref el) = else_col {
                el.dtype
            } else {
                DataType::Int32
            };

            let mut scalars = Vec::with_capacity(row_count);
            for i in 0..row_count {
                let mut matched = false;
                for (branch_idx, cond_col) in when_conds.iter().enumerate() {
                    if !cond_col.is_null(i) && cond_col.get(i) == Scalar::Bool(true) {
                        scalars.push(when_thens[branch_idx].get(i));
                        matched = true;
                        break;
                    }
                }
                if !matched {
                    if let Some(ref el) = else_col {
                        scalars.push(el.get(i));
                    } else {
                        scalars.push(Scalar::Null);
                    }
                }
            }
            Ok(Column::from_scalars(out_dtype, &scalars))
        }
        Expr::Cast { expr, to } => {
            let col = eval(expr, batch)?;
            cast_column(&col, *to)
        }
        Expr::Extract {
            field: DateField::Year,
            expr,
        } => {
            let col = eval(expr, batch)?;
            let mut scalars = Vec::with_capacity(row_count);
            for i in 0..row_count {
                if col.is_null(i) {
                    scalars.push(Scalar::Null);
                } else {
                    let days = match col.get(i) {
                        Scalar::Date32(d) => d,
                        _ => bail!("Extract Year requires Date32 column"),
                    };
                    scalars.push(Scalar::Int64(date32_to_year(days)));
                }
            }
            Ok(Column::from_scalars(DataType::Int64, &scalars))
        }
        Expr::Substring { expr, start, len } => {
            let col = eval(expr, batch)?;
            let mut scalars = Vec::with_capacity(row_count);
            for i in 0..row_count {
                if col.is_null(i) {
                    scalars.push(Scalar::Null);
                } else {
                    let s = col.utf8_value(i);
                    let chars: Vec<char> = s.chars().collect();
                    let s_idx = if *start > 0 { (*start - 1) as usize } else { 0 };
                    let len_c = (*len).max(0) as usize;
                    let e_idx = (s_idx + len_c).min(chars.len());
                    let sub: String = if s_idx < chars.len() {
                        chars[s_idx..e_idx].iter().collect()
                    } else {
                        String::new()
                    };
                    scalars.push(Scalar::Utf8(sub));
                }
            }
            Ok(Column::from_scalars(DataType::Utf8, &scalars))
        }
    }
}

pub fn cast_column(col: &Column, to: DataType) -> Result<Column> {
    if col.dtype == to {
        return Ok(col.clone());
    }
    let len = col.len();
    let mut scalars = Vec::with_capacity(len);
    for i in 0..len {
        if col.is_null(i) {
            scalars.push(Scalar::Null);
        } else {
            let val = col.get(i);
            let casted = match (val, to) {
                (Scalar::Int32(x), DataType::Decimal { scale, .. }) => {
                    let p = checked_pow10(scale)?;
                    let v = (x as i128)
                        .checked_mul(p)
                        .ok_or_else(|| anyhow::anyhow!("decimal cast overflow"))?;
                    Scalar::Decimal(v, scale)
                }
                (Scalar::Int64(x), DataType::Decimal { scale, .. }) => {
                    let p = checked_pow10(scale)?;
                    let v = (x as i128)
                        .checked_mul(p)
                        .ok_or_else(|| anyhow::anyhow!("decimal cast overflow"))?;
                    Scalar::Decimal(v, scale)
                }
                (
                    Scalar::Decimal(x, s),
                    DataType::Decimal {
                        scale: target_s, ..
                    },
                ) => {
                    if target_s >= s {
                        let p = checked_pow10(target_s - s)?;
                        let v = x
                            .checked_mul(p)
                            .ok_or_else(|| anyhow::anyhow!("decimal cast overflow"))?;
                        Scalar::Decimal(v, target_s)
                    } else {
                        let p = checked_pow10(s - target_s)?;
                        Scalar::Decimal(x / p, target_s)
                    }
                }
                (Scalar::Int32(x), DataType::Int64) => Scalar::Int64(x as i64),
                (Scalar::Int64(x), DataType::Int32) => Scalar::Int32(x as i32),
                (Scalar::Float64(x), DataType::Float64) => Scalar::Float64(x),
                (Scalar::Decimal(x, s), DataType::Float64) => {
                    Scalar::Float64(x as f64 / pow10(s) as f64)
                }
                (Scalar::Int32(x), DataType::Float64) => Scalar::Float64(x as f64),
                (Scalar::Int64(x), DataType::Float64) => Scalar::Float64(x as f64),
                (Scalar::Utf8(s), DataType::Utf8) => Scalar::Utf8(s),
                (v, target_type) => bail!("unsupported cast from {:?} to {:?}", v, target_type),
            };
            scalars.push(casted);
        }
    }
    Ok(Column::from_scalars(to, &scalars))
}

fn eval_binary(op: BinOp, left: &Column, right: &Column, len: usize) -> Result<Column> {
    match op {
        BinOp::Eq | BinOp::NotEq | BinOp::Lt | BinOp::LtEq | BinOp::Gt | BinOp::GtEq => {
            eval_comparison(op, left, right, len)
        }
        BinOp::And => eval_and(left, right, len),
        BinOp::Or => eval_or(left, right, len),
        BinOp::Div => eval_div(left, right, len),
        BinOp::Add | BinOp::Sub | BinOp::Mul => eval_arithmetic(op, left, right, len),
    }
}

fn eval_and(left: &Column, right: &Column, len: usize) -> Result<Column> {
    let mut data = Vec::with_capacity(len);
    let mut validity = Vec::with_capacity(len);
    let mut has_null = false;

    for i in 0..len {
        let a_null = left.is_null(i);
        let b_null = right.is_null(i);
        let a_val = !a_null && matches!(left.get(i), Scalar::Bool(true));
        let b_val = !b_null && matches!(right.get(i), Scalar::Bool(true));

        if !a_null && !a_val {
            // false AND anything is false
            validity.push(true);
            data.push(false);
        } else if !b_null && !b_val {
            // anything AND false is false
            validity.push(true);
            data.push(false);
        } else if !a_null && !b_null {
            validity.push(true);
            data.push(a_val && b_val);
        } else {
            has_null = true;
            validity.push(false);
            data.push(false);
        }
    }

    Ok(Column {
        dtype: DataType::Bool,
        data: ColumnData::Bool(data),
        validity: if has_null { Some(validity) } else { None },
    })
}

fn eval_or(left: &Column, right: &Column, len: usize) -> Result<Column> {
    let mut data = Vec::with_capacity(len);
    let mut validity = Vec::with_capacity(len);
    let mut has_null = false;

    for i in 0..len {
        let a_null = left.is_null(i);
        let b_null = right.is_null(i);
        let a_val = !a_null && matches!(left.get(i), Scalar::Bool(true));
        let b_val = !b_null && matches!(right.get(i), Scalar::Bool(true));

        if !a_null && a_val {
            // true OR anything is true
            validity.push(true);
            data.push(true);
        } else if !b_null && b_val {
            // anything OR true is true
            validity.push(true);
            data.push(true);
        } else if !a_null && !b_null {
            validity.push(true);
            data.push(a_val || b_val);
        } else {
            has_null = true;
            validity.push(false);
            data.push(false);
        }
    }

    Ok(Column {
        dtype: DataType::Bool,
        data: ColumnData::Bool(data),
        validity: if has_null { Some(validity) } else { None },
    })
}

fn eval_comparison(op: BinOp, left: &Column, right: &Column, len: usize) -> Result<Column> {
    let mut data = Vec::with_capacity(len);
    let mut validity = Vec::with_capacity(len);
    let mut has_null = false;

    for i in 0..len {
        if left.is_null(i) || right.is_null(i) {
            has_null = true;
            validity.push(false);
            data.push(false);
        } else {
            validity.push(true);
            let cmp = compare_scalars(&left.get(i), &right.get(i))?;
            let res = match op {
                BinOp::Eq => cmp == std::cmp::Ordering::Equal,
                BinOp::NotEq => cmp != std::cmp::Ordering::Equal,
                BinOp::Lt => cmp == std::cmp::Ordering::Less,
                BinOp::LtEq => cmp == std::cmp::Ordering::Less || cmp == std::cmp::Ordering::Equal,
                BinOp::Gt => cmp == std::cmp::Ordering::Greater,
                BinOp::GtEq => {
                    cmp == std::cmp::Ordering::Greater || cmp == std::cmp::Ordering::Equal
                }
                _ => unreachable!(),
            };
            data.push(res);
        }
    }

    Ok(Column {
        dtype: DataType::Bool,
        data: ColumnData::Bool(data),
        validity: if has_null { Some(validity) } else { None },
    })
}

pub fn compare_decimals(v1: i128, s1: u8, v2: i128, s2: u8) -> Result<std::cmp::Ordering> {
    if s1 == s2 {
        return Ok(v1.cmp(&v2));
    }
    let target_s = s1.max(s2);
    if let (Ok(p1), Ok(p2)) = (checked_pow10(target_s - s1), checked_pow10(target_s - s2)) {
        if let (Some(r1), Some(r2)) = (v1.checked_mul(p1), v2.checked_mul(p2)) {
            return Ok(r1.cmp(&r2));
        }
    }
    match (v1.cmp(&0), v2.cmp(&0)) {
        (std::cmp::Ordering::Less, std::cmp::Ordering::Greater | std::cmp::Ordering::Equal) => {
            return Ok(std::cmp::Ordering::Less);
        }
        (std::cmp::Ordering::Greater | std::cmp::Ordering::Equal, std::cmp::Ordering::Less) => {
            return Ok(std::cmp::Ordering::Greater);
        }
        (std::cmp::Ordering::Equal, std::cmp::Ordering::Equal) => {
            return Ok(std::cmp::Ordering::Equal);
        }
        _ => {}
    }
    let negative = v1 < 0;
    let (u1, u2) = (v1.abs(), v2.abs());

    let (w1, f1, f1_scale) = if s1 <= 38 {
        let p = checked_pow10(s1)?;
        (u1 / p, u1 % p, s1)
    } else {
        (0, u1, s1)
    };
    let (w2, f2, f2_scale) = if s2 <= 38 {
        let p = checked_pow10(s2)?;
        (u2 / p, u2 % p, s2)
    } else {
        (0, u2, s2)
    };

    let ord = if w1 != w2 {
        w1.cmp(&w2)
    } else {
        let target_frac_s = f1_scale.max(f2_scale);
        let p1 = checked_pow10(target_frac_s - f1_scale)?;
        let p2 = checked_pow10(target_frac_s - f2_scale)?;
        let rf1 = f1
            .checked_mul(p1)
            .ok_or_else(|| anyhow::anyhow!("decimal comparison overflow"))?;
        let rf2 = f2
            .checked_mul(p2)
            .ok_or_else(|| anyhow::anyhow!("decimal comparison overflow"))?;
        rf1.cmp(&rf2)
    };

    Ok(if negative { ord.reverse() } else { ord })
}

pub fn compare_scalars(a: &Scalar, b: &Scalar) -> Result<std::cmp::Ordering> {
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
        (Scalar::Decimal(v1, s1), Scalar::Decimal(v2, s2)) => compare_decimals(*v1, *s1, *v2, *s2),
        _ => bail!("cannot compare {:?} and {:?}", a, b),
    }
}

fn eval_div(left: &Column, right: &Column, len: usize) -> Result<Column> {
    let mut data = Vec::with_capacity(len);
    let mut validity = Vec::with_capacity(len);
    let mut has_null = false;

    for i in 0..len {
        if left.is_null(i) || right.is_null(i) {
            has_null = true;
            validity.push(false);
            data.push(0.0);
        } else {
            let l_f64 = scalar_to_f64(&left.get(i))?;
            let r_f64 = scalar_to_f64(&right.get(i))?;
            validity.push(true);
            data.push(l_f64 / r_f64);
        }
    }

    Ok(Column {
        dtype: DataType::Float64,
        data: ColumnData::Float64(data),
        validity: if has_null { Some(validity) } else { None },
    })
}

fn scalar_to_f64(s: &Scalar) -> Result<f64> {
    match s {
        Scalar::Float64(x) => Ok(*x),
        Scalar::Int32(x) => Ok(*x as f64),
        Scalar::Int64(x) => Ok(*x as f64),
        Scalar::Decimal(x, scale) => Ok(*x as f64 / pow10(*scale) as f64),
        other => bail!("cannot convert {:?} to f64", other),
    }
}

fn eval_arithmetic(op: BinOp, left: &Column, right: &Column, len: usize) -> Result<Column> {
    let mut scalars = Vec::with_capacity(len);
    for i in 0..len {
        if left.is_null(i) || right.is_null(i) {
            scalars.push(Scalar::Null);
        } else {
            let l_val = left.get(i);
            let r_val = right.get(i);
            let res = match (l_val, r_val) {
                // Decimal op Decimal
                (Scalar::Decimal(v1, s1), Scalar::Decimal(v2, s2)) => match op {
                    BinOp::Add => {
                        let s = s1.max(s2);
                        let p1 = checked_pow10(s - s1)?;
                        let p2 = checked_pow10(s - s2)?;
                        let r1 = v1
                            .checked_mul(p1)
                            .ok_or_else(|| anyhow::anyhow!("decimal addition overflow"))?;
                        let r2 = v2
                            .checked_mul(p2)
                            .ok_or_else(|| anyhow::anyhow!("decimal addition overflow"))?;
                        let sum = r1
                            .checked_add(r2)
                            .ok_or_else(|| anyhow::anyhow!("decimal addition overflow"))?;
                        Scalar::Decimal(sum, s)
                    }
                    BinOp::Sub => {
                        let s = s1.max(s2);
                        let p1 = checked_pow10(s - s1)?;
                        let p2 = checked_pow10(s - s2)?;
                        let r1 = v1
                            .checked_mul(p1)
                            .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                        let r2 = v2
                            .checked_mul(p2)
                            .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                        let diff = r1
                            .checked_sub(r2)
                            .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                        Scalar::Decimal(diff, s)
                    }
                    BinOp::Mul => {
                        let s = s1 + s2;
                        let prod = v1
                            .checked_mul(v2)
                            .ok_or_else(|| anyhow::anyhow!("decimal multiplication overflow"))?;
                        Scalar::Decimal(prod, s)
                    }
                    _ => unreachable!(),
                },
                // Integer op Decimal
                (Scalar::Int32(x), Scalar::Decimal(v, s)) => match op {
                    BinOp::Add => {
                        let p = checked_pow10(s)?;
                        let scaled = (x as i128)
                            .checked_mul(p)
                            .ok_or_else(|| anyhow::anyhow!("decimal addition overflow"))?;
                        let sum = scaled
                            .checked_add(v)
                            .ok_or_else(|| anyhow::anyhow!("decimal addition overflow"))?;
                        Scalar::Decimal(sum, s)
                    }
                    BinOp::Sub => {
                        let p = checked_pow10(s)?;
                        let scaled = (x as i128)
                            .checked_mul(p)
                            .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                        let diff = scaled
                            .checked_sub(v)
                            .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                        Scalar::Decimal(diff, s)
                    }
                    BinOp::Mul => {
                        let prod = (x as i128)
                            .checked_mul(v)
                            .ok_or_else(|| anyhow::anyhow!("decimal multiplication overflow"))?;
                        Scalar::Decimal(prod, s)
                    }
                    _ => unreachable!(),
                },
                (Scalar::Decimal(v, s), Scalar::Int32(x)) => match op {
                    BinOp::Add => {
                        let p = checked_pow10(s)?;
                        let scaled = (x as i128)
                            .checked_mul(p)
                            .ok_or_else(|| anyhow::anyhow!("decimal addition overflow"))?;
                        let sum = v
                            .checked_add(scaled)
                            .ok_or_else(|| anyhow::anyhow!("decimal addition overflow"))?;
                        Scalar::Decimal(sum, s)
                    }
                    BinOp::Sub => {
                        let p = checked_pow10(s)?;
                        let scaled = (x as i128)
                            .checked_mul(p)
                            .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                        let diff = v
                            .checked_sub(scaled)
                            .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                        Scalar::Decimal(diff, s)
                    }
                    BinOp::Mul => {
                        let prod = v
                            .checked_mul(x as i128)
                            .ok_or_else(|| anyhow::anyhow!("decimal multiplication overflow"))?;
                        Scalar::Decimal(prod, s)
                    }
                    _ => unreachable!(),
                },
                (Scalar::Int64(x), Scalar::Decimal(v, s)) => match op {
                    BinOp::Add => {
                        let p = checked_pow10(s)?;
                        let scaled = (x as i128)
                            .checked_mul(p)
                            .ok_or_else(|| anyhow::anyhow!("decimal addition overflow"))?;
                        let sum = scaled
                            .checked_add(v)
                            .ok_or_else(|| anyhow::anyhow!("decimal addition overflow"))?;
                        Scalar::Decimal(sum, s)
                    }
                    BinOp::Sub => {
                        let p = checked_pow10(s)?;
                        let scaled = (x as i128)
                            .checked_mul(p)
                            .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                        let diff = scaled
                            .checked_sub(v)
                            .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                        Scalar::Decimal(diff, s)
                    }
                    BinOp::Mul => {
                        let prod = (x as i128)
                            .checked_mul(v)
                            .ok_or_else(|| anyhow::anyhow!("decimal multiplication overflow"))?;
                        Scalar::Decimal(prod, s)
                    }
                    _ => unreachable!(),
                },
                (Scalar::Decimal(v, s), Scalar::Int64(x)) => match op {
                    BinOp::Add => {
                        let p = checked_pow10(s)?;
                        let scaled = (x as i128)
                            .checked_mul(p)
                            .ok_or_else(|| anyhow::anyhow!("decimal addition overflow"))?;
                        let sum = v
                            .checked_add(scaled)
                            .ok_or_else(|| anyhow::anyhow!("decimal addition overflow"))?;
                        Scalar::Decimal(sum, s)
                    }
                    BinOp::Sub => {
                        let p = checked_pow10(s)?;
                        let scaled = (x as i128)
                            .checked_mul(p)
                            .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                        let diff = v
                            .checked_sub(scaled)
                            .ok_or_else(|| anyhow::anyhow!("decimal subtraction overflow"))?;
                        Scalar::Decimal(diff, s)
                    }
                    BinOp::Mul => {
                        let prod = v
                            .checked_mul(x as i128)
                            .ok_or_else(|| anyhow::anyhow!("decimal multiplication overflow"))?;
                        Scalar::Decimal(prod, s)
                    }
                    _ => unreachable!(),
                },
                // Integer op Integer
                (Scalar::Int32(x), Scalar::Int32(y)) => match op {
                    BinOp::Add => Scalar::Int32(x + y),
                    BinOp::Sub => Scalar::Int32(x - y),
                    BinOp::Mul => Scalar::Int32(x * y),
                    _ => unreachable!(),
                },
                (Scalar::Int64(x), Scalar::Int64(y)) => match op {
                    BinOp::Add => Scalar::Int64(x + y),
                    BinOp::Sub => Scalar::Int64(x - y),
                    BinOp::Mul => Scalar::Int64(x * y),
                    _ => unreachable!(),
                },
                (Scalar::Int32(x), Scalar::Int64(y)) => match op {
                    BinOp::Add => Scalar::Int64(x as i64 + y),
                    BinOp::Sub => Scalar::Int64(x as i64 - y),
                    BinOp::Mul => Scalar::Int64(x as i64 * y),
                    _ => unreachable!(),
                },
                (Scalar::Int64(x), Scalar::Int32(y)) => match op {
                    BinOp::Add => Scalar::Int64(x + y as i64),
                    BinOp::Sub => Scalar::Int64(x - y as i64),
                    BinOp::Mul => Scalar::Int64(x * y as i64),
                    _ => unreachable!(),
                },
                // Float op Any Number / Any Number op Float
                (ref left_val @ Scalar::Float64(_), ref right_val)
                | (ref left_val, ref right_val @ Scalar::Float64(_)) => {
                    let x = scalar_to_f64(left_val)?;
                    let y = scalar_to_f64(right_val)?;
                    match op {
                        BinOp::Add => Scalar::Float64(x + y),
                        BinOp::Sub => Scalar::Float64(x - y),
                        BinOp::Mul => Scalar::Float64(x * y),
                        _ => unreachable!(),
                    }
                }
                // Date32 op Integer
                (Scalar::Date32(d), Scalar::Int32(n)) => match op {
                    BinOp::Add => Scalar::Date32(d + n),
                    BinOp::Sub => Scalar::Date32(d - n),
                    _ => bail!("unsupported op on Date32 and Int32"),
                },
                (Scalar::Date32(d), Scalar::Int64(n)) => match op {
                    BinOp::Add => Scalar::Date32(d + n as i32),
                    BinOp::Sub => Scalar::Date32(d - n as i32),
                    _ => bail!("unsupported op on Date32 and Int64"),
                },
                (Scalar::Date32(d1), Scalar::Date32(d2)) if op == BinOp::Sub => {
                    Scalar::Int32(d1 - d2)
                }
                (l, r) => bail!("unsupported arithmetic op {:?} on {:?} and {:?}", op, l, r),
            };
            scalars.push(res);
        }
    }

    let out_dtype = binary_op_type(op, &left.dtype, &right.dtype)?;

    Ok(Column::from_scalars(out_dtype, &scalars))
}
