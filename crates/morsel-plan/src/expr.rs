use anyhow::{bail, Result};
use morsel_core::{DataType, Scalar, Schema};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DateField {
    Year,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Column(usize),
    Literal(Scalar),
    Binary {
        op: BinOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Not(Box<Expr>),
    Negate(Box<Expr>),
    IsNull(Box<Expr>),
    Like {
        expr: Box<Expr>,
        pattern: String,
        negated: bool,
    },
    InList {
        expr: Box<Expr>,
        list: Vec<Scalar>,
        negated: bool,
    },
    Case {
        whens: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },
    Cast {
        expr: Box<Expr>,
        to: DataType,
    },
    Extract {
        field: DateField,
        expr: Box<Expr>,
    },
    Substring {
        expr: Box<Expr>,
        start: i64,
        len: i64,
    },
}

impl Expr {
    pub fn data_type(&self, input: &Schema) -> Result<DataType> {
        match self {
            Expr::Column(idx) => {
                let field = input
                    .fields
                    .get(*idx)
                    .ok_or_else(|| anyhow::anyhow!("column index {} out of bounds", idx))?;
                Ok(field.dtype)
            }
            Expr::Literal(scalar) => match scalar {
                Scalar::Null => bail!("untyped null literal"),
                Scalar::Bool(_) => Ok(DataType::Bool),
                Scalar::Int32(_) => Ok(DataType::Int32),
                Scalar::Int64(_) => Ok(DataType::Int64),
                Scalar::Float64(_) => Ok(DataType::Float64),
                Scalar::Decimal(_, scale) => Ok(DataType::Decimal {
                    precision: 38,
                    scale: *scale,
                }),
                Scalar::Date32(_) => Ok(DataType::Date32),
                Scalar::Utf8(_) => Ok(DataType::Utf8),
            },
            Expr::Binary { op, left, right } => {
                let lt = left.data_type(input)?;
                let rt = right.data_type(input)?;
                binary_op_type(*op, &lt, &rt)
            }
            Expr::Not(_) => Ok(DataType::Bool),
            Expr::Negate(expr) => expr.data_type(input),
            Expr::IsNull(_) => Ok(DataType::Bool),
            Expr::Like { .. } => Ok(DataType::Bool),
            Expr::InList { .. } => Ok(DataType::Bool),
            Expr::Case { whens, else_expr } => {
                let mut unified: Option<DataType> = None;
                for (_, then_expr) in whens {
                    let dt = then_expr.data_type(input)?;
                    unified = match unified {
                        None => Some(dt),
                        Some(cur) => Some(unify_types(&cur, &dt)?),
                    };
                }
                if let Some(else_expr) = else_expr {
                    let dt = else_expr.data_type(input)?;
                    unified = match unified {
                        None => Some(dt),
                        Some(cur) => Some(unify_types(&cur, &dt)?),
                    };
                }
                unified.ok_or_else(|| anyhow::anyhow!("case expression has no branches"))
            }
            Expr::Cast { to, .. } => Ok(*to),
            Expr::Extract { .. } => Ok(DataType::Int64),
            Expr::Substring { .. } => Ok(DataType::Utf8),
        }
    }
}

pub fn unify_types(t1: &DataType, t2: &DataType) -> Result<DataType> {
    if t1 == t2 {
        return Ok(*t1);
    }
    match (t1, t2) {
        (DataType::Float64, _) | (_, DataType::Float64) => Ok(DataType::Float64),
        (
            DataType::Decimal {
                precision: p1,
                scale: s1,
            },
            DataType::Decimal {
                precision: p2,
                scale: s2,
            },
        ) => Ok(DataType::Decimal {
            precision: (*p1).max(*p2).max(38),
            scale: (*s1).max(*s2),
        }),
        (DataType::Decimal { scale, .. }, DataType::Int32 | DataType::Int64)
        | (DataType::Int32 | DataType::Int64, DataType::Decimal { scale, .. }) => {
            Ok(DataType::Decimal {
                precision: 38,
                scale: *scale,
            })
        }
        (DataType::Int64, DataType::Int32) | (DataType::Int32, DataType::Int64) => {
            Ok(DataType::Int64)
        }
        _ => bail!("cannot unify types {:?} and {:?}", t1, t2),
    }
}

pub fn binary_op_type(op: BinOp, lt: &DataType, rt: &DataType) -> Result<DataType> {
    match op {
        BinOp::Eq
        | BinOp::NotEq
        | BinOp::Lt
        | BinOp::LtEq
        | BinOp::Gt
        | BinOp::GtEq
        | BinOp::And
        | BinOp::Or => Ok(DataType::Bool),
        BinOp::Div => Ok(DataType::Float64),
        BinOp::Add | BinOp::Sub => match (lt, rt) {
            (DataType::Decimal { scale: s1, .. }, DataType::Decimal { scale: s2, .. }) => {
                Ok(DataType::Decimal {
                    precision: 38,
                    scale: *s1.max(s2),
                })
            }
            (DataType::Decimal { scale, .. }, DataType::Int32 | DataType::Int64)
            | (DataType::Int32 | DataType::Int64, DataType::Decimal { scale, .. }) => {
                Ok(DataType::Decimal {
                    precision: 38,
                    scale: *scale,
                })
            }
            (DataType::Int64, DataType::Int32 | DataType::Int64)
            | (DataType::Int32, DataType::Int64) => Ok(DataType::Int64),
            (DataType::Int32, DataType::Int32) => Ok(DataType::Int32),
            (DataType::Float64, _) | (_, DataType::Float64) => Ok(DataType::Float64),
            (DataType::Date32, DataType::Int32 | DataType::Int64) => Ok(DataType::Date32),
            (DataType::Int32 | DataType::Int64, DataType::Date32) if op == BinOp::Add => {
                Ok(DataType::Date32)
            }
            (DataType::Date32, DataType::Date32) if op == BinOp::Sub => Ok(DataType::Int32),
            _ => bail!("unsupported binary op {:?} on {:?} and {:?}", op, lt, rt),
        },
        BinOp::Mul => match (lt, rt) {
            (DataType::Decimal { scale: s1, .. }, DataType::Decimal { scale: s2, .. }) => {
                Ok(DataType::Decimal {
                    precision: 38,
                    scale: s1 + s2,
                })
            }
            (DataType::Decimal { scale, .. }, DataType::Int32 | DataType::Int64)
            | (DataType::Int32 | DataType::Int64, DataType::Decimal { scale, .. }) => {
                Ok(DataType::Decimal {
                    precision: 38,
                    scale: *scale,
                })
            }
            (DataType::Int64, DataType::Int32 | DataType::Int64)
            | (DataType::Int32, DataType::Int64) => Ok(DataType::Int64),
            (DataType::Int32, DataType::Int32) => Ok(DataType::Int32),
            (DataType::Float64, _) | (_, DataType::Float64) => Ok(DataType::Float64),
            _ => bail!("unsupported binary op Mul on {:?} and {:?}", lt, rt),
        },
    }
}
