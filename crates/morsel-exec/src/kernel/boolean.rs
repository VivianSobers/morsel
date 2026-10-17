use morsel_core::{Column, ColumnData, DataType};

pub fn eval_not_fast(col: &Column) -> Option<Column> {
    if let ColumnData::Bool(v) = &col.data {
        let len = v.len();
        let mut data = Vec::with_capacity(len);
        for &b in v {
            data.push(!b);
        }
        let has_null = col
            .validity
            .as_ref()
            .is_some_and(|val| val.iter().any(|&b| !b));
        let validity = if has_null { col.validity.clone() } else { None };
        if has_null {
            if let Some(ref val) = validity {
                for (i, &valid) in val.iter().enumerate() {
                    if !valid {
                        data[i] = false;
                    }
                }
            }
        }
        Some(Column {
            dtype: DataType::Bool,
            data: ColumnData::Bool(data),
            validity,
        })
    } else {
        None
    }
}

pub fn eval_and_fast(left: &Column, right: &Column) -> Option<Column> {
    match (&left.data, &right.data) {
        (ColumnData::Bool(v1), ColumnData::Bool(v2)) => {
            let len = v1.len();
            if len != v2.len() {
                return None;
            }
            if left.validity.is_none() && right.validity.is_none() {
                let mut data = Vec::with_capacity(len);
                for i in 0..len {
                    data.push(v1[i] && v2[i]);
                }
                return Some(Column {
                    dtype: DataType::Bool,
                    data: ColumnData::Bool(data),
                    validity: None,
                });
            }

            let mut data = Vec::with_capacity(len);
            let mut validity = Vec::with_capacity(len);
            let mut has_null = false;
            let val1 = left.validity.as_deref();
            let val2 = right.validity.as_deref();

            for i in 0..len {
                let a_null = val1.is_some_and(|v| !v[i]);
                let b_null = val2.is_some_and(|v| !v[i]);
                let a = v1[i];
                let b = v2[i];

                if (!a_null && !a) || (!b_null && !b) {
                    validity.push(true);
                    data.push(false);
                } else if !a_null && !b_null {
                    validity.push(true);
                    data.push(a && b);
                } else {
                    has_null = true;
                    validity.push(false);
                    data.push(false);
                }
            }

            Some(Column {
                dtype: DataType::Bool,
                data: ColumnData::Bool(data),
                validity: if has_null { Some(validity) } else { None },
            })
        }
        _ => None,
    }
}

pub fn eval_or_fast(left: &Column, right: &Column) -> Option<Column> {
    match (&left.data, &right.data) {
        (ColumnData::Bool(v1), ColumnData::Bool(v2)) => {
            let len = v1.len();
            if len != v2.len() {
                return None;
            }
            if left.validity.is_none() && right.validity.is_none() {
                let mut data = Vec::with_capacity(len);
                for i in 0..len {
                    data.push(v1[i] || v2[i]);
                }
                return Some(Column {
                    dtype: DataType::Bool,
                    data: ColumnData::Bool(data),
                    validity: None,
                });
            }

            let mut data = Vec::with_capacity(len);
            let mut validity = Vec::with_capacity(len);
            let mut has_null = false;
            let val1 = left.validity.as_deref();
            let val2 = right.validity.as_deref();

            for i in 0..len {
                let a_null = val1.is_some_and(|v| !v[i]);
                let b_null = val2.is_some_and(|v| !v[i]);
                let a = v1[i];
                let b = v2[i];

                if (!a_null && a) || (!b_null && b) {
                    validity.push(true);
                    data.push(true);
                } else if !a_null && !b_null {
                    validity.push(true);
                    data.push(a || b);
                } else {
                    has_null = true;
                    validity.push(false);
                    data.push(false);
                }
            }

            Some(Column {
                dtype: DataType::Bool,
                data: ColumnData::Bool(data),
                validity: if has_null { Some(validity) } else { None },
            })
        }
        _ => None,
    }
}
