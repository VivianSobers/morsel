use anyhow::Result;
use morsel_core::{DataType, Scalar, Schema};
use morsel_plan::{AggExpr, AggFunc, BinOp, DateField, Expr, JoinType, LogicalPlan, SortKey};

use crate::csv::scalar_to_csv_string;

pub fn scalar_to_sql_literal(s: &Scalar) -> String {
    match s {
        Scalar::Null => "NULL".to_string(),
        Scalar::Bool(b) => {
            if *b {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        Scalar::Int32(x) => x.to_string(),
        Scalar::Int64(x) => x.to_string(),
        Scalar::Float64(f) => {
            if f.is_nan() {
                "'NaN'::DOUBLE".to_string()
            } else if f.is_infinite() {
                if *f > 0.0 {
                    "'Infinity'::DOUBLE".to_string()
                } else {
                    "'-Infinity'::DOUBLE".to_string()
                }
            } else {
                format!("{}", f)
            }
        }
        Scalar::Decimal(_v, scale) => {
            let str_val = scalar_to_csv_string(s);
            format!("CAST('{}' AS DECIMAL(38, {}))", str_val, scale)
        }
        Scalar::Date32(_) => {
            let date_str = scalar_to_csv_string(s);
            format!("DATE '{}'", date_str)
        }
        Scalar::Utf8(st) => {
            format!("'{}'", st.replace('\'', "''"))
        }
    }
}

pub fn expr_to_sql(expr: &Expr, col_prefix: Option<&str>) -> String {
    match expr {
        Expr::Column(idx) => {
            if let Some(prefix) = col_prefix {
                format!("{}.c{}", prefix, idx)
            } else {
                format!("c{}", idx)
            }
        }
        Expr::Literal(scalar) => scalar_to_sql_literal(scalar),
        Expr::Binary { op, left, right } => {
            let l_sql = expr_to_sql(left, col_prefix);
            let r_sql = expr_to_sql(right, col_prefix);
            match op {
                BinOp::Add => format!("({} + {})", l_sql, r_sql),
                BinOp::Sub => format!("({} - {})", l_sql, r_sql),
                BinOp::Mul => format!("({} * {})", l_sql, r_sql),
                BinOp::Div => format!("(CAST({} AS DOUBLE) / CAST({} AS DOUBLE))", l_sql, r_sql),
                BinOp::Eq => format!("({} = {})", l_sql, r_sql),
                BinOp::NotEq => format!("({} <> {})", l_sql, r_sql),
                BinOp::Lt => format!("({} < {})", l_sql, r_sql),
                BinOp::LtEq => format!("({} <= {})", l_sql, r_sql),
                BinOp::Gt => format!("({} > {})", l_sql, r_sql),
                BinOp::GtEq => format!("({} >= {})", l_sql, r_sql),
                BinOp::And => format!("({} AND {})", l_sql, r_sql),
                BinOp::Or => format!("({} OR {})", l_sql, r_sql),
            }
        }
        Expr::Not(sub) => format!("(NOT {})", expr_to_sql(sub, col_prefix)),
        Expr::Negate(sub) => format!("(-({}))", expr_to_sql(sub, col_prefix)),
        Expr::IsNull(sub) => format!("({} IS NULL)", expr_to_sql(sub, col_prefix)),
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let e_sql = expr_to_sql(expr, col_prefix);
            let escaped = pattern.replace('\'', "''");
            if *negated {
                format!("({} NOT LIKE '{}')", e_sql, escaped)
            } else {
                format!("({} LIKE '{}')", e_sql, escaped)
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let e_sql = expr_to_sql(expr, col_prefix);
            let items: Vec<String> = list.iter().map(scalar_to_sql_literal).collect();
            let items_sql = if items.is_empty() {
                "NULL".to_string()
            } else {
                items.join(", ")
            };
            if *negated {
                format!("({} NOT IN ({}))", e_sql, items_sql)
            } else {
                format!("({} IN ({}))", e_sql, items_sql)
            }
        }
        Expr::Case { whens, else_expr } => {
            let mut parts = Vec::new();
            parts.push("CASE".to_string());
            for (cond, then) in whens {
                parts.push(format!(
                    "WHEN {} THEN {}",
                    expr_to_sql(cond, col_prefix),
                    expr_to_sql(then, col_prefix)
                ));
            }
            if let Some(el) = else_expr {
                parts.push(format!("ELSE {}", expr_to_sql(el, col_prefix)));
            }
            parts.push("END".to_string());
            parts.join(" ")
        }
        Expr::Cast { expr, to } => {
            let e_sql = expr_to_sql(expr, col_prefix);
            let ty_sql = match to {
                DataType::Bool => "BOOLEAN",
                DataType::Int32 => "INTEGER",
                DataType::Int64 => "BIGINT",
                DataType::Float64 => "DOUBLE",
                DataType::Decimal { scale, .. } => {
                    return format!("CAST({} AS DECIMAL(38, {}))", e_sql, scale)
                }
                DataType::Date32 => "DATE",
                DataType::Utf8 => "VARCHAR",
            };
            format!("CAST({} AS {})", e_sql, ty_sql)
        }
        Expr::Extract {
            field: DateField::Year,
            expr,
        } => format!("EXTRACT(YEAR FROM {})", expr_to_sql(expr, col_prefix)),
        Expr::Substring { expr, start, len } => {
            format!(
                "SUBSTRING({}, {}, {})",
                expr_to_sql(expr, col_prefix),
                start,
                len
            )
        }
    }
}

pub fn agg_to_sql(agg: &AggExpr, col_prefix: Option<&str>) -> String {
    match agg.func {
        AggFunc::CountStar => "COUNT(*)".to_string(),
        AggFunc::Count => {
            if let Some(ref arg) = agg.arg {
                format!("COUNT({})", expr_to_sql(arg, col_prefix))
            } else {
                "COUNT(*)".to_string()
            }
        }
        AggFunc::CountDistinct => {
            if let Some(ref arg) = agg.arg {
                format!("COUNT(DISTINCT {})", expr_to_sql(arg, col_prefix))
            } else {
                "COUNT(*)".to_string()
            }
        }
        AggFunc::Sum => {
            let arg = agg.arg.as_ref().expect("sum arg");
            format!("SUM({})", expr_to_sql(arg, col_prefix))
        }
        AggFunc::Avg => {
            let arg = agg.arg.as_ref().expect("avg arg");
            format!("AVG({})", expr_to_sql(arg, col_prefix))
        }
        AggFunc::Min => {
            let arg = agg.arg.as_ref().expect("min arg");
            format!("MIN({})", expr_to_sql(arg, col_prefix))
        }
        AggFunc::Max => {
            let arg = agg.arg.as_ref().expect("max arg");
            format!("MAX({})", expr_to_sql(arg, col_prefix))
        }
    }
}

struct CteStep {
    name: String,
    sql: String,
}

struct SqlGenerator {
    ctes: Vec<CteStep>,
    counter: usize,
}

impl SqlGenerator {
    fn new() -> Self {
        Self {
            ctes: Vec::new(),
            counter: 0,
        }
    }

    fn next_cte_name(&mut self) -> String {
        let name = format!("step_{}", self.counter);
        self.counter += 1;
        name
    }

    fn build_plan(&mut self, plan: &LogicalPlan) -> Result<(String, Schema)> {
        match plan {
            LogicalPlan::Scan {
                table,
                columns,
                table_schema,
            } => {
                let cte_name = self.next_cte_name();
                let mut select_items = Vec::new();
                let mut out_fields = Vec::new();
                for (out_idx, &col_idx) in columns.iter().enumerate() {
                    let field = &table_schema.fields[col_idx];
                    select_items.push(format!("{} AS c{}", field.name, out_idx));
                    out_fields.push(field.clone());
                }
                let sql = format!("SELECT {} FROM {}", select_items.join(", "), table);
                let schema = Schema { fields: out_fields };
                self.ctes.push(CteStep {
                    name: cte_name.clone(),
                    sql,
                });
                Ok((cte_name, schema))
            }
            LogicalPlan::Filter { input, predicate } => {
                let (in_cte, in_schema) = self.build_plan(input)?;
                let cte_name = self.next_cte_name();
                let cols: Vec<String> = (0..in_schema.fields.len())
                    .map(|i| format!("c{}", i))
                    .collect();
                let pred_sql = expr_to_sql(predicate, None);
                let sql = format!(
                    "SELECT {} FROM {} WHERE {}",
                    cols.join(", "),
                    in_cte,
                    pred_sql
                );
                self.ctes.push(CteStep {
                    name: cte_name.clone(),
                    sql,
                });
                Ok((cte_name, in_schema))
            }
            LogicalPlan::Join {
                left,
                right,
                join_type,
                on,
                filter,
            } => {
                let (l_cte, l_schema) = self.build_plan(left)?;
                let (r_cte, r_schema) = self.build_plan(right)?;
                let cte_name = self.next_cte_name();

                let l_len = l_schema.fields.len();
                let r_len = r_schema.fields.len();

                let mut on_preds = Vec::new();
                for (l_expr, r_expr) in on {
                    let l_sql = expr_to_sql(l_expr, Some(&l_cte));
                    let r_sql = expr_to_sql(r_expr, Some(&r_cte));
                    on_preds.push(format!("{} = {}", l_sql, r_sql));
                }

                if let Some(f) = filter {
                    // In morsel, residual filter indices 0..l_len are left, l_len..l_len+r_len are right
                    let f_sql = filter_to_join_sql(f, &l_cte, &r_cte, l_len);
                    on_preds.push(f_sql);
                }

                let on_clause = if on_preds.is_empty() {
                    "1 = 1".to_string()
                } else {
                    on_preds.join(" AND ")
                };

                let (join_keyword, out_schema, select_items) = match join_type {
                    JoinType::Inner => {
                        let mut fields = l_schema.fields.clone();
                        fields.extend(r_schema.fields.clone());
                        let mut items = Vec::new();
                        for i in 0..l_len {
                            items.push(format!("{}.c{} AS c{}", l_cte, i, i));
                        }
                        for i in 0..r_len {
                            items.push(format!("{}.c{} AS c{}", r_cte, i, l_len + i));
                        }
                        ("INNER JOIN", Schema { fields }, items)
                    }
                    JoinType::Left => {
                        let mut fields = l_schema.fields.clone();
                        for mut rf in r_schema.fields.clone() {
                            rf.nullable = true;
                            fields.push(rf);
                        }
                        let mut items = Vec::new();
                        for i in 0..l_len {
                            items.push(format!("{}.c{} AS c{}", l_cte, i, i));
                        }
                        for i in 0..r_len {
                            items.push(format!("{}.c{} AS c{}", r_cte, i, l_len + i));
                        }
                        ("LEFT OUTER JOIN", Schema { fields }, items)
                    }
                    JoinType::Semi => {
                        let items: Vec<String> = (0..l_len)
                            .map(|i| format!("{}.c{} AS c{}", l_cte, i, i))
                            .collect();
                        ("SEMI JOIN", l_schema.clone(), items)
                    }
                    JoinType::Anti => {
                        let items: Vec<String> = (0..l_len)
                            .map(|i| format!("{}.c{} AS c{}", l_cte, i, i))
                            .collect();
                        ("ANTI JOIN", l_schema.clone(), items)
                    }
                };

                let sql = format!(
                    "SELECT {} FROM {} {} {} ON {}",
                    select_items.join(", "),
                    l_cte,
                    join_keyword,
                    r_cte,
                    on_clause
                );
                self.ctes.push(CteStep {
                    name: cte_name.clone(),
                    sql,
                });
                Ok((cte_name, out_schema))
            }
            LogicalPlan::CrossJoin { left, right } => {
                let (l_cte, l_schema) = self.build_plan(left)?;
                let (r_cte, r_schema) = self.build_plan(right)?;
                let cte_name = self.next_cte_name();

                let l_len = l_schema.fields.len();
                let r_len = r_schema.fields.len();

                let mut fields = l_schema.fields.clone();
                fields.extend(r_schema.fields.clone());

                let mut items = Vec::new();
                for i in 0..l_len {
                    items.push(format!("{}.c{} AS c{}", l_cte, i, i));
                }
                for i in 0..r_len {
                    items.push(format!("{}.c{} AS c{}", r_cte, i, l_len + i));
                }

                let sql = format!(
                    "SELECT {} FROM {} CROSS JOIN {}",
                    items.join(", "),
                    l_cte,
                    r_cte
                );
                let schema = Schema { fields };
                self.ctes.push(CteStep {
                    name: cte_name.clone(),
                    sql,
                });
                Ok((cte_name, schema))
            }
            LogicalPlan::Project { input, exprs } => {
                let (in_cte, _) = self.build_plan(input)?;
                let out_schema = plan.schema()?;
                let cte_name = self.next_cte_name();

                let mut items = Vec::new();
                for (idx, (expr, _name)) in exprs.iter().enumerate() {
                    items.push(format!("{} AS c{}", expr_to_sql(expr, None), idx));
                }

                let sql = format!("SELECT {} FROM {}", items.join(", "), in_cte);
                self.ctes.push(CteStep {
                    name: cte_name.clone(),
                    sql,
                });
                Ok((cte_name, out_schema))
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggs,
            } => {
                let (in_cte, _) = self.build_plan(input)?;
                let out_schema = plan.schema()?;
                let cte_name = self.next_cte_name();

                let mut items = Vec::new();
                let mut gb_clauses = Vec::new();

                for (idx, (expr, _name)) in group_by.iter().enumerate() {
                    let e_sql = expr_to_sql(expr, None);
                    items.push(format!("{} AS c{}", e_sql, idx));
                    gb_clauses.push(e_sql);
                }

                let gb_len = group_by.len();
                for (idx, (agg, _name)) in aggs.iter().enumerate() {
                    let a_sql = agg_to_sql(agg, None);
                    items.push(format!("{} AS c{}", a_sql, gb_len + idx));
                }

                let sql = if gb_clauses.is_empty() {
                    format!("SELECT {} FROM {}", items.join(", "), in_cte)
                } else {
                    format!(
                        "SELECT {} FROM {} GROUP BY {}",
                        items.join(", "),
                        in_cte,
                        gb_clauses.join(", ")
                    )
                };

                self.ctes.push(CteStep {
                    name: cte_name.clone(),
                    sql,
                });
                Ok((cte_name, out_schema))
            }
            LogicalPlan::Sort { input, .. } => {
                // If Sort is encountered inside, just build input
                self.build_plan(input)
            }
            LogicalPlan::Limit { input, .. } => {
                // If Limit is encountered inside, just build input
                self.build_plan(input)
            }
        }
    }
}

fn filter_to_join_sql(expr: &Expr, l_cte: &str, r_cte: &str, l_len: usize) -> String {
    match expr {
        Expr::Column(idx) => {
            if *idx < l_len {
                format!("{}.c{}", l_cte, idx)
            } else {
                format!("{}.c{}", r_cte, idx - l_len)
            }
        }
        Expr::Literal(scalar) => scalar_to_sql_literal(scalar),
        Expr::Binary { op, left, right } => {
            let l_sql = filter_to_join_sql(left, l_cte, r_cte, l_len);
            let r_sql = filter_to_join_sql(right, l_cte, r_cte, l_len);
            match op {
                BinOp::Add => format!("({} + {})", l_sql, r_sql),
                BinOp::Sub => format!("({} - {})", l_sql, r_sql),
                BinOp::Mul => format!("({} * {})", l_sql, r_sql),
                BinOp::Div => format!("(CAST({} AS DOUBLE) / CAST({} AS DOUBLE))", l_sql, r_sql),
                BinOp::Eq => format!("({} = {})", l_sql, r_sql),
                BinOp::NotEq => format!("({} <> {})", l_sql, r_sql),
                BinOp::Lt => format!("({} < {})", l_sql, r_sql),
                BinOp::LtEq => format!("({} <= {})", l_sql, r_sql),
                BinOp::Gt => format!("({} > {})", l_sql, r_sql),
                BinOp::GtEq => format!("({} >= {})", l_sql, r_sql),
                BinOp::And => format!("({} AND {})", l_sql, r_sql),
                BinOp::Or => format!("({} OR {})", l_sql, r_sql),
            }
        }
        Expr::Not(sub) => format!("(NOT {})", filter_to_join_sql(sub, l_cte, r_cte, l_len)),
        Expr::Negate(sub) => format!("(-({}))", filter_to_join_sql(sub, l_cte, r_cte, l_len)),
        Expr::IsNull(sub) => format!("({} IS NULL)", filter_to_join_sql(sub, l_cte, r_cte, l_len)),
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let e_sql = filter_to_join_sql(expr, l_cte, r_cte, l_len);
            let escaped = pattern.replace('\'', "''");
            if *negated {
                format!("({} NOT LIKE '{}')", e_sql, escaped)
            } else {
                format!("({} LIKE '{}')", e_sql, escaped)
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let e_sql = filter_to_join_sql(expr, l_cte, r_cte, l_len);
            let items: Vec<String> = list.iter().map(scalar_to_sql_literal).collect();
            let items_sql = items.join(", ");
            if *negated {
                format!("({} NOT IN ({}))", e_sql, items_sql)
            } else {
                format!("({} IN ({}))", e_sql, items_sql)
            }
        }
        _ => "TRUE".to_string(),
    }
}

pub fn sort_key_to_sql(key: &SortKey) -> String {
    let col = expr_to_sql(&key.expr, None);
    let dir = if key.asc { "ASC" } else { "DESC" };
    let nulls = if key.nulls_first {
        "NULLS FIRST"
    } else {
        "NULLS LAST"
    };
    format!("{} {} {}", col, dir, nulls)
}

pub fn plan_to_sql(plan: &LogicalPlan) -> Result<String> {
    let mut sort_keys = Vec::new();
    let mut limit_n = None;

    let mut current = plan;
    if let LogicalPlan::Limit { input, n } = current {
        limit_n = Some(*n);
        current = input.as_ref();
    }
    if let LogicalPlan::Sort { input, keys } = current {
        sort_keys = keys.clone();
        current = input.as_ref();
    }

    let mut gen = SqlGenerator::new();
    let (final_cte, out_schema) = gen.build_plan(current)?;

    let mut cte_defs = Vec::new();
    for cte in &gen.ctes {
        cte_defs.push(format!("{} AS (\n    {}\n)", cte.name, cte.sql));
    }

    let mut select_items = Vec::new();
    for (i, field) in out_schema.fields.iter().enumerate() {
        select_items.push(format!("c{} AS \"{}\"", i, field.name));
    }

    let mut query = String::new();
    if !cte_defs.is_empty() {
        query.push_str("WITH ");
        query.push_str(&cte_defs.join(",\n"));
        query.push('\n');
    }

    query.push_str(&format!(
        "SELECT {} FROM {}",
        select_items.join(", "),
        final_cte
    ));

    if !sort_keys.is_empty() {
        let order_items: Vec<String> = sort_keys.iter().map(sort_key_to_sql).collect();
        query.push_str(&format!("\nORDER BY {}", order_items.join(", ")));
    }

    if let Some(n) = limit_n {
        query.push_str(&format!("\nLIMIT {}", n));
    }

    Ok(query)
}
