use anyhow::Result;
use morsel_core::{DataType, Field, Schema};

use crate::expr::Expr;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggFunc {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    CountStar,
    CountDistinct,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggExpr {
    pub func: AggFunc,
    pub arg: Option<Expr>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Semi,
    Anti,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SortKey {
    pub expr: Expr,
    pub asc: bool,
    pub nulls_first: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum LogicalPlan {
    Scan {
        table: String,
        columns: Vec<usize>,
        table_schema: Schema,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    Project {
        input: Box<LogicalPlan>,
        exprs: Vec<(Expr, String)>,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<(Expr, String)>,
        aggs: Vec<(AggExpr, String)>,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        join_type: JoinType,
        on: Vec<(Expr, Expr)>,
        filter: Option<Expr>,
    },
    CrossJoin {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    },
    Sort {
        input: Box<LogicalPlan>,
        keys: Vec<SortKey>,
    },
    Limit {
        input: Box<LogicalPlan>,
        n: usize,
    },
}

pub trait SchemaProvider {
    fn table_schema(&self, name: &str) -> Option<Schema>;
}

impl LogicalPlan {
    pub fn schema(&self) -> Result<Schema> {
        match self {
            LogicalPlan::Scan {
                columns,
                table_schema,
                ..
            } => {
                let fields =
                    columns
                        .iter()
                        .map(|&idx| {
                            table_schema.fields.get(idx).cloned().ok_or_else(|| {
                                anyhow::anyhow!("column index {} out of bounds", idx)
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                Ok(Schema { fields })
            }
            LogicalPlan::Filter { input, .. } => input.schema(),
            LogicalPlan::Project { input, exprs } => {
                let in_schema = input.schema()?;
                let mut fields = Vec::with_capacity(exprs.len());
                for (expr, name) in exprs {
                    let dtype = expr.data_type(&in_schema)?;
                    let nullable = match expr {
                        Expr::Column(idx) => in_schema
                            .fields
                            .get(*idx)
                            .map(|f| f.nullable)
                            .unwrap_or(true),
                        _ => true,
                    };
                    fields.push(Field {
                        name: name.clone(),
                        dtype,
                        nullable,
                    });
                }
                Ok(Schema { fields })
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggs,
            } => {
                let in_schema = input.schema()?;
                let mut fields = Vec::with_capacity(group_by.len() + aggs.len());
                for (expr, name) in group_by {
                    let dtype = expr.data_type(&in_schema)?;
                    let nullable = match expr {
                        Expr::Column(idx) => in_schema
                            .fields
                            .get(*idx)
                            .map(|f| f.nullable)
                            .unwrap_or(true),
                        _ => true,
                    };
                    fields.push(Field {
                        name: name.clone(),
                        dtype,
                        nullable,
                    });
                }
                for (agg, name) in aggs {
                    let dtype = match agg.func {
                        AggFunc::Count | AggFunc::CountStar | AggFunc::CountDistinct => {
                            DataType::Int64
                        }
                        AggFunc::Avg => DataType::Float64,
                        AggFunc::Sum => {
                            let arg_ty = agg
                                .arg
                                .as_ref()
                                .ok_or_else(|| anyhow::anyhow!("sum aggregate requires argument"))?
                                .data_type(&in_schema)?;
                            match arg_ty {
                                DataType::Int32 | DataType::Int64 => DataType::Int64,
                                DataType::Decimal { scale, .. } => DataType::Decimal {
                                    precision: 38,
                                    scale,
                                },
                                DataType::Float64 => DataType::Float64,
                                other => anyhow::bail!("unsupported sum type: {:?}", other),
                            }
                        }
                        AggFunc::Min | AggFunc::Max => agg
                            .arg
                            .as_ref()
                            .ok_or_else(|| anyhow::anyhow!("min/max requires argument"))?
                            .data_type(&in_schema)?,
                    };
                    let nullable = !matches!(agg.func, AggFunc::Count | AggFunc::CountStar);
                    fields.push(Field {
                        name: name.clone(),
                        dtype,
                        nullable,
                    });
                }
                Ok(Schema { fields })
            }
            LogicalPlan::Join {
                left,
                right,
                join_type,
                ..
            } => {
                let left_schema = left.schema()?;
                match join_type {
                    JoinType::Semi | JoinType::Anti => Ok(left_schema),
                    JoinType::Inner => {
                        let right_schema = right.schema()?;
                        let mut fields = left_schema.fields;
                        fields.extend(right_schema.fields);
                        Ok(Schema { fields })
                    }
                    JoinType::Left => {
                        let right_schema = right.schema()?;
                        let mut fields = left_schema.fields;
                        for mut f in right_schema.fields {
                            f.nullable = true;
                            fields.push(f);
                        }
                        Ok(Schema { fields })
                    }
                }
            }
            LogicalPlan::CrossJoin { left, right } => {
                let left_schema = left.schema()?;
                let right_schema = right.schema()?;
                let mut fields = left_schema.fields;
                fields.extend(right_schema.fields);
                Ok(Schema { fields })
            }
            LogicalPlan::Sort { input, .. } => input.schema(),
            LogicalPlan::Limit { input, .. } => input.schema(),
        }
    }
}
