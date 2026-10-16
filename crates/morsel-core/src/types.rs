pub const BATCH_ROWS: usize = 2048;
pub const MORSEL_ROWS: usize = 122_880;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DataType {
    Bool,
    Int32,
    Int64,
    Float64,
    Decimal { precision: u8, scale: u8 },
    Date32,
    Utf8,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Scalar {
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    Decimal(i128, u8),
    Date32(i32),
    Utf8(String),
}

impl Scalar {
    pub fn is_null(&self) -> bool {
        matches!(self, Scalar::Null)
    }

    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Scalar::Null => None,
            Scalar::Bool(_) => Some(DataType::Bool),
            Scalar::Int32(_) => Some(DataType::Int32),
            Scalar::Int64(_) => Some(DataType::Int64),
            Scalar::Float64(_) => Some(DataType::Float64),
            Scalar::Decimal(_, scale) => Some(DataType::Decimal {
                precision: 38,
                scale: *scale,
            }),
            Scalar::Date32(_) => Some(DataType::Date32),
            Scalar::Utf8(_) => Some(DataType::Utf8),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Field {
    pub name: String,
    pub dtype: DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    pub fn empty() -> Self {
        Self { fields: Vec::new() }
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn field(&self, i: usize) -> &Field {
        &self.fields[i]
    }

    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.fields
            .iter()
            .position(|f| f.name.eq_ignore_ascii_case(name))
    }
}
