use crate::types::{DataType, Scalar};

#[derive(Clone, Debug, PartialEq)]
pub enum ColumnData {
    Bool(Vec<bool>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Float64(Vec<f64>),
    Decimal(Vec<i128>),
    Date32(Vec<i32>),
    Utf8 { offsets: Vec<u32>, bytes: Vec<u8> },
}

#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    pub dtype: DataType,
    pub data: ColumnData,
    pub validity: Option<Vec<bool>>,
}

impl Column {
    pub fn len(&self) -> usize {
        match &self.data {
            ColumnData::Bool(v) => v.len(),
            ColumnData::Int32(v) => v.len(),
            ColumnData::Int64(v) => v.len(),
            ColumnData::Float64(v) => v.len(),
            ColumnData::Decimal(v) => v.len(),
            ColumnData::Date32(v) => v.len(),
            ColumnData::Utf8 { offsets, .. } => {
                if offsets.is_empty() {
                    0
                } else {
                    offsets.len() - 1
                }
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_null(&self, i: usize) -> bool {
        match &self.validity {
            Some(v) => !v[i],
            None => false,
        }
    }

    pub fn get(&self, i: usize) -> Scalar {
        if self.is_null(i) {
            return Scalar::Null;
        }
        match &self.data {
            ColumnData::Bool(v) => Scalar::Bool(v[i]),
            ColumnData::Int32(v) => Scalar::Int32(v[i]),
            ColumnData::Int64(v) => Scalar::Int64(v[i]),
            ColumnData::Float64(v) => Scalar::Float64(v[i]),
            ColumnData::Decimal(v) => {
                if let DataType::Decimal { scale, .. } = self.dtype {
                    Scalar::Decimal(v[i], scale)
                } else {
                    Scalar::Decimal(v[i], 0)
                }
            }
            ColumnData::Date32(v) => Scalar::Date32(v[i]),
            ColumnData::Utf8 { .. } => Scalar::Utf8(self.utf8_value(i).to_string()),
        }
    }

    pub fn utf8_value(&self, i: usize) -> &str {
        match &self.data {
            ColumnData::Utf8 { offsets, bytes } => {
                let start = offsets[i] as usize;
                let end = offsets[i + 1] as usize;
                std::str::from_utf8(&bytes[start..end]).expect("valid utf8")
            }
            _ => panic!("Column is not Utf8"),
        }
    }

    pub fn from_scalars(dtype: DataType, values: &[Scalar]) -> Column {
        let has_nulls = values.iter().any(|v| matches!(v, Scalar::Null));
        let validity = if has_nulls {
            Some(values.iter().map(|v| !matches!(v, Scalar::Null)).collect())
        } else {
            None
        };
        let data = match dtype {
            DataType::Bool => {
                let v = values
                    .iter()
                    .map(|s| match s {
                        Scalar::Bool(b) => *b,
                        Scalar::Null => false,
                        _ => panic!("mismatched scalar for Bool: {:?}", s),
                    })
                    .collect();
                ColumnData::Bool(v)
            }
            DataType::Int32 => {
                let v = values
                    .iter()
                    .map(|s| match s {
                        Scalar::Int32(x) => *x,
                        Scalar::Null => 0,
                        _ => panic!("mismatched scalar for Int32: {:?}", s),
                    })
                    .collect();
                ColumnData::Int32(v)
            }
            DataType::Int64 => {
                let v = values
                    .iter()
                    .map(|s| match s {
                        Scalar::Int64(x) => *x,
                        Scalar::Null => 0,
                        _ => panic!("mismatched scalar for Int64: {:?}", s),
                    })
                    .collect();
                ColumnData::Int64(v)
            }
            DataType::Float64 => {
                let v = values
                    .iter()
                    .map(|s| match s {
                        Scalar::Float64(x) => *x,
                        Scalar::Null => 0.0,
                        _ => panic!("mismatched scalar for Float64: {:?}", s),
                    })
                    .collect();
                ColumnData::Float64(v)
            }
            DataType::Decimal { .. } => {
                let v = values
                    .iter()
                    .map(|s| match s {
                        Scalar::Decimal(x, _) => *x,
                        Scalar::Null => 0,
                        _ => panic!("mismatched scalar for Decimal: {:?}", s),
                    })
                    .collect();
                ColumnData::Decimal(v)
            }
            DataType::Date32 => {
                let v = values
                    .iter()
                    .map(|s| match s {
                        Scalar::Date32(x) => *x,
                        Scalar::Null => 0,
                        _ => panic!("mismatched scalar for Date32: {:?}", s),
                    })
                    .collect();
                ColumnData::Date32(v)
            }
            DataType::Utf8 => {
                let mut offsets = Vec::with_capacity(values.len() + 1);
                let mut bytes = Vec::new();
                offsets.push(0);
                for s in values {
                    match s {
                        Scalar::Utf8(st) => {
                            bytes.extend_from_slice(st.as_bytes());
                            offsets.push(bytes.len() as u32);
                        }
                        Scalar::Null => {
                            offsets.push(bytes.len() as u32);
                        }
                        _ => panic!("mismatched scalar for Utf8: {:?}", s),
                    }
                }
                ColumnData::Utf8 { offsets, bytes }
            }
        };
        Column {
            dtype,
            data,
            validity,
        }
    }

    pub fn take(&self, indices: &[u32]) -> Column {
        let validity = self
            .validity
            .as_ref()
            .map(|val| indices.iter().map(|&idx| val[idx as usize]).collect());
        let data = match &self.data {
            ColumnData::Bool(v) => {
                ColumnData::Bool(indices.iter().map(|&idx| v[idx as usize]).collect())
            }
            ColumnData::Int32(v) => {
                ColumnData::Int32(indices.iter().map(|&idx| v[idx as usize]).collect())
            }
            ColumnData::Int64(v) => {
                ColumnData::Int64(indices.iter().map(|&idx| v[idx as usize]).collect())
            }
            ColumnData::Float64(v) => {
                ColumnData::Float64(indices.iter().map(|&idx| v[idx as usize]).collect())
            }
            ColumnData::Decimal(v) => {
                ColumnData::Decimal(indices.iter().map(|&idx| v[idx as usize]).collect())
            }
            ColumnData::Date32(v) => {
                ColumnData::Date32(indices.iter().map(|&idx| v[idx as usize]).collect())
            }
            ColumnData::Utf8 { .. } => {
                let mut offsets = Vec::with_capacity(indices.len() + 1);
                let mut bytes = Vec::new();
                offsets.push(0);
                for &idx in indices {
                    let s = self.utf8_value(idx as usize);
                    bytes.extend_from_slice(s.as_bytes());
                    offsets.push(bytes.len() as u32);
                }
                ColumnData::Utf8 { offsets, bytes }
            }
        };
        Column {
            dtype: self.dtype,
            data,
            validity,
        }
    }

    pub fn slice(&self, offset: usize, len: usize) -> Column {
        let end = offset + len;
        assert!(end <= self.len(), "slice out of bounds");
        let validity = self.validity.as_ref().map(|val| val[offset..end].to_vec());
        let data = match &self.data {
            ColumnData::Bool(v) => ColumnData::Bool(v[offset..end].to_vec()),
            ColumnData::Int32(v) => ColumnData::Int32(v[offset..end].to_vec()),
            ColumnData::Int64(v) => ColumnData::Int64(v[offset..end].to_vec()),
            ColumnData::Float64(v) => ColumnData::Float64(v[offset..end].to_vec()),
            ColumnData::Decimal(v) => ColumnData::Decimal(v[offset..end].to_vec()),
            ColumnData::Date32(v) => ColumnData::Date32(v[offset..end].to_vec()),
            ColumnData::Utf8 { offsets, bytes } => {
                let start_byte = offsets[offset] as usize;
                let end_byte = offsets[end] as usize;
                let new_bytes = bytes[start_byte..end_byte].to_vec();
                let new_offsets = offsets[offset..=end]
                    .iter()
                    .map(|&o| o - (start_byte as u32))
                    .collect();
                ColumnData::Utf8 {
                    offsets: new_offsets,
                    bytes: new_bytes,
                }
            }
        };
        Column {
            dtype: self.dtype,
            data,
            validity,
        }
    }
}
