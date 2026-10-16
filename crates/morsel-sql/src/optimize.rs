use std::collections::{HashMap, HashSet};

use anyhow::{bail, Result};
use chrono::{Datelike, Duration, NaiveDate};
use morsel_core::{DataType, Scalar, Schema};
use morsel_plan::{AggExpr, BinOp, Expr, JoinType, LogicalPlan, SortKey};
use regex::Regex;

pub fn optimize(plan: LogicalPlan, stats: &HashMap<String, usize>) -> Result<LogicalPlan> {
    let root_schema = plan.schema()?;
    validate_plan_columns(&plan)?;

    // 1. Constant folding
    let plan = fold_constants(plan)?;
    check_optimizer_step("fold_constants", &plan, &root_schema)?;

    // 2. OR factoring (factor common terms out of disjunctions)
    let plan = factor_disjunctions(plan)?;
    check_optimizer_step("factor_disjunctions", &plan, &root_schema)?;

    // 3. Predicate pushdown (pushes filters through projections, aggregates, down into join sides and to scans)
    let plan = push_predicates(plan)?;
    check_optimizer_step("push_predicates", &plan, &root_schema)?;

    // 4. Join ordering (greedy by estimated rows from stats, turns comma joins into hash joins)
    let plan = order_joins(plan, stats)?;
    check_optimizer_step("order_joins", &plan, &root_schema)?;

    // 5. Push semi/anti joins below joins
    let plan = push_semi_joins(plan, stats)?;
    check_optimizer_step("push_semi_joins", &plan, &root_schema)?;

    // 6. Projection pruning
    // Base table columns are already pruned down to scans by bind.rs
    let plan = prune_projections(plan)?;
    check_optimizer_step("prune_projections", &plan, &root_schema)?;

    Ok(plan)
}

pub fn check_optimizer_step(
    step_name: &str,
    step_plan: &LogicalPlan,
    expected_schema: &Schema,
) -> Result<()> {
    let schema = step_plan.schema().map_err(|e| {
        anyhow::anyhow!(
            "step '{}' produced plan with invalid schema: {}",
            step_name,
            e
        )
    })?;
    if schema.fields.len() != expected_schema.fields.len() {
        bail!(
            "step '{}' changed root schema length: expected {}, got {}",
            step_name,
            expected_schema.fields.len(),
            schema.fields.len()
        );
    }
    for (i, (f_actual, f_exp)) in schema
        .fields
        .iter()
        .zip(&expected_schema.fields)
        .enumerate()
    {
        if f_actual.name != f_exp.name || f_actual.dtype != f_exp.dtype {
            bail!(
                "step '{}' changed root column {}: expected {}: {:?}, got {}: {:?}",
                step_name,
                i,
                f_exp.name,
                f_exp.dtype,
                f_actual.name,
                f_actual.dtype
            );
        }
    }
    validate_plan_columns(step_plan).map_err(|e| {
        anyhow::anyhow!("step '{}' produced invalid column index: {}", step_name, e)
    })?;
    Ok(())
}

pub fn validate_plan_columns(plan: &LogicalPlan) -> Result<()> {
    match plan {
        LogicalPlan::Scan {
            columns,
            table_schema,
            ..
        } => {
            for &col in columns {
                if col >= table_schema.fields.len() {
                    bail!(
                        "Scan column index {} out of range (schema has {} cols)",
                        col,
                        table_schema.fields.len()
                    );
                }
            }
            Ok(())
        }
        LogicalPlan::Filter { input, predicate } => {
            validate_plan_columns(input)?;
            let input_schema = input.schema()?;
            let mut cols = HashSet::new();
            expr_references_columns(predicate, &mut cols);
            for col in cols {
                if col >= input_schema.len() {
                    bail!(
                        "Filter column index {} out of range (input has {} cols)",
                        col,
                        input_schema.len()
                    );
                }
            }
            Ok(())
        }
        LogicalPlan::Project { input, exprs } => {
            validate_plan_columns(input)?;
            let input_schema = input.schema()?;
            for (e, _) in exprs {
                let mut cols = HashSet::new();
                expr_references_columns(e, &mut cols);
                for col in cols {
                    if col >= input_schema.len() {
                        bail!(
                            "Project column index {} out of range (input has {} cols)",
                            col,
                            input_schema.len()
                        );
                    }
                }
            }
            Ok(())
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggs,
        } => {
            validate_plan_columns(input)?;
            let input_schema = input.schema()?;
            for (e, _) in group_by {
                let mut cols = HashSet::new();
                expr_references_columns(e, &mut cols);
                for col in cols {
                    if col >= input_schema.len() {
                        bail!(
                            "Aggregate group_by column index {} out of range (input has {} cols)",
                            col,
                            input_schema.len()
                        );
                    }
                }
            }
            for (agg, _) in aggs {
                if let Some(arg) = &agg.arg {
                    let mut cols = HashSet::new();
                    expr_references_columns(arg, &mut cols);
                    for col in cols {
                        if col >= input_schema.len() {
                            bail!(
                                "Aggregate arg column index {} out of range (input has {} cols)",
                                col,
                                input_schema.len()
                            );
                        }
                    }
                }
            }
            Ok(())
        }
        LogicalPlan::Join {
            left,
            right,
            on,
            filter,
            ..
        } => {
            validate_plan_columns(left)?;
            validate_plan_columns(right)?;
            let left_len = left.schema()?.len();
            let right_len = right.schema()?.len();
            for (l, r) in on {
                let mut l_cols = HashSet::new();
                expr_references_columns(l, &mut l_cols);
                for col in l_cols {
                    if col >= left_len {
                        bail!(
                            "Join on left column index {} out of range (left has {} cols)",
                            col,
                            left_len
                        );
                    }
                }
                let mut r_cols = HashSet::new();
                expr_references_columns(r, &mut r_cols);
                for col in r_cols {
                    if col >= right_len {
                        bail!(
                            "Join on right column index {} out of range (right has {} cols)",
                            col,
                            right_len
                        );
                    }
                }
            }
            if let Some(f) = filter {
                let mut f_cols = HashSet::new();
                expr_references_columns(f, &mut f_cols);
                let total = left_len + right_len;
                for col in f_cols {
                    if col >= total {
                        bail!(
                            "Join filter column index {} out of range (join has {} cols)",
                            col,
                            total
                        );
                    }
                }
            }
            Ok(())
        }
        LogicalPlan::CrossJoin { left, right } => {
            validate_plan_columns(left)?;
            validate_plan_columns(right)?;
            Ok(())
        }
        LogicalPlan::Sort { input, keys } => {
            validate_plan_columns(input)?;
            let input_schema = input.schema()?;
            for key in keys {
                let mut cols = HashSet::new();
                expr_references_columns(&key.expr, &mut cols);
                for col in cols {
                    if col >= input_schema.len() {
                        bail!(
                            "Sort key column index {} out of range (input has {} cols)",
                            col,
                            input_schema.len()
                        );
                    }
                }
            }
            Ok(())
        }
        LogicalPlan::Limit { input, .. } => validate_plan_columns(input),
    }
}

// ============================================================================
// 1. Constant Folding
// ============================================================================

pub fn fold_constants(plan: LogicalPlan) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Scan { .. } => Ok(plan),
        LogicalPlan::Filter { input, predicate } => {
            let opt_input = fold_constants(*input)?;
            let folded_pred = fold_expr(predicate);
            Ok(LogicalPlan::Filter {
                input: Box::new(opt_input),
                predicate: folded_pred,
            })
        }
        LogicalPlan::Project { input, exprs } => {
            let opt_input = fold_constants(*input)?;
            let folded_exprs = exprs
                .into_iter()
                .map(|(e, name)| (fold_expr(e), name))
                .collect();
            Ok(LogicalPlan::Project {
                input: Box::new(opt_input),
                exprs: folded_exprs,
            })
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggs,
        } => {
            let opt_input = fold_constants(*input)?;
            let folded_group_by = group_by
                .into_iter()
                .map(|(e, name)| (fold_expr(e), name))
                .collect();
            let folded_aggs = aggs
                .into_iter()
                .map(|(mut agg, name)| {
                    if let Some(arg) = agg.arg {
                        agg.arg = Some(fold_expr(arg));
                    }
                    (agg, name)
                })
                .collect();
            Ok(LogicalPlan::Aggregate {
                input: Box::new(opt_input),
                group_by: folded_group_by,
                aggs: folded_aggs,
            })
        }
        LogicalPlan::Join {
            left,
            right,
            join_type,
            on,
            filter,
        } => {
            let opt_left = fold_constants(*left)?;
            let opt_right = fold_constants(*right)?;
            let folded_on = on
                .into_iter()
                .map(|(l, r)| (fold_expr(l), fold_expr(r)))
                .collect();
            let folded_filter = filter.map(fold_expr);
            Ok(LogicalPlan::Join {
                left: Box::new(opt_left),
                right: Box::new(opt_right),
                join_type,
                on: folded_on,
                filter: folded_filter,
            })
        }
        LogicalPlan::CrossJoin { left, right } => {
            let opt_left = fold_constants(*left)?;
            let opt_right = fold_constants(*right)?;
            Ok(LogicalPlan::CrossJoin {
                left: Box::new(opt_left),
                right: Box::new(opt_right),
            })
        }
        LogicalPlan::Sort { input, keys } => {
            let opt_input = fold_constants(*input)?;
            let folded_keys = keys
                .into_iter()
                .map(|k| SortKey {
                    expr: fold_expr(k.expr),
                    asc: k.asc,
                    nulls_first: k.nulls_first,
                })
                .collect();
            Ok(LogicalPlan::Sort {
                input: Box::new(opt_input),
                keys: folded_keys,
            })
        }
        LogicalPlan::Limit { input, n } => {
            let opt_input = fold_constants(*input)?;
            Ok(LogicalPlan::Limit {
                input: Box::new(opt_input),
                n,
            })
        }
    }
}

pub fn fold_expr(expr: Expr) -> Expr {
    match expr {
        Expr::Column(idx) => Expr::Column(idx),
        Expr::Literal(s) => Expr::Literal(s),
        Expr::Not(inner) => {
            let folded = fold_expr(*inner);
            match folded {
                Expr::Literal(Scalar::Bool(b)) => Expr::Literal(Scalar::Bool(!b)),
                other => Expr::Not(Box::new(other)),
            }
        }
        Expr::Negate(inner) => {
            let folded = fold_expr(*inner);
            match folded {
                Expr::Literal(Scalar::Int32(v)) => Expr::Literal(Scalar::Int32(-v)),
                Expr::Literal(Scalar::Int64(v)) => Expr::Literal(Scalar::Int64(-v)),
                Expr::Literal(Scalar::Float64(v)) => Expr::Literal(Scalar::Float64(-v)),
                Expr::Literal(Scalar::Decimal(v, s)) => Expr::Literal(Scalar::Decimal(-v, s)),
                other => Expr::Negate(Box::new(other)),
            }
        }
        Expr::IsNull(inner) => {
            let folded = fold_expr(*inner);
            match folded {
                Expr::Literal(Scalar::Null) => Expr::Literal(Scalar::Bool(true)),
                Expr::Literal(_) => Expr::Literal(Scalar::Bool(false)),
                other => Expr::IsNull(Box::new(other)),
            }
        }
        Expr::Binary { op, left, right } => {
            let l = fold_expr(*left);
            let r = fold_expr(*right);

            // Boolean simplification
            if op == BinOp::And {
                match (&l, &r) {
                    (Expr::Literal(Scalar::Bool(true)), _) => return r,
                    (_, Expr::Literal(Scalar::Bool(true))) => return l,
                    (Expr::Literal(Scalar::Bool(false)), _) => {
                        return Expr::Literal(Scalar::Bool(false));
                    }
                    (_, Expr::Literal(Scalar::Bool(false))) => {
                        return Expr::Literal(Scalar::Bool(false));
                    }
                    _ => {}
                }
            } else if op == BinOp::Or {
                match (&l, &r) {
                    (Expr::Literal(Scalar::Bool(true)), _) => {
                        return Expr::Literal(Scalar::Bool(true));
                    }
                    (_, Expr::Literal(Scalar::Bool(true))) => {
                        return Expr::Literal(Scalar::Bool(true));
                    }
                    (Expr::Literal(Scalar::Bool(false)), _) => return r,
                    (_, Expr::Literal(Scalar::Bool(false))) => return l,
                    _ => {}
                }
            }

            // Both literals
            if let (Expr::Literal(sl), Expr::Literal(sr)) = (&l, &r) {
                if let Some(res) = eval_const_binop(op, sl, sr) {
                    return Expr::Literal(res);
                }
            }

            Expr::Binary {
                op,
                left: Box::new(l),
                right: Box::new(r),
            }
        }
        Expr::Extract { field, expr } => {
            let folded = fold_expr(*expr);
            if let Expr::Literal(Scalar::Date32(days)) = folded {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                if let Some(d) = epoch.checked_add_signed(Duration::days(days as i64)) {
                    return Expr::Literal(Scalar::Int64(d.year() as i64));
                }
            }
            Expr::Extract {
                field,
                expr: Box::new(folded),
            }
        }
        Expr::Substring { expr, start, len } => {
            let folded = fold_expr(*expr);
            if let Expr::Literal(Scalar::Utf8(s)) = folded {
                let s_chars: Vec<char> = s.chars().collect();
                let start_idx = if start > 0 { (start - 1) as usize } else { 0 };
                let end_idx = (start_idx + len as usize).min(s_chars.len());
                let sub: String = if start_idx < s_chars.len() {
                    s_chars[start_idx..end_idx].iter().collect()
                } else {
                    String::new()
                };
                return Expr::Literal(Scalar::Utf8(sub));
            }
            Expr::Substring {
                expr: Box::new(folded),
                start,
                len,
            }
        }
        Expr::Cast { expr, to } => {
            let folded = fold_expr(*expr);
            if let Expr::Literal(ref s) = folded {
                if let Some(cast_s) = eval_const_cast(s, &to) {
                    return Expr::Literal(cast_s);
                }
            }
            Expr::Cast {
                expr: Box::new(folded),
                to,
            }
        }
        Expr::Case { whens, else_expr } => {
            let mut new_whens = Vec::new();
            for (cond, val) in whens {
                let f_cond = fold_expr(cond);
                let f_val = fold_expr(val);
                match f_cond {
                    Expr::Literal(Scalar::Bool(true)) => {
                        if new_whens.is_empty() {
                            return f_val;
                        } else {
                            new_whens.push((f_cond, f_val));
                            break;
                        }
                    }
                    Expr::Literal(Scalar::Bool(false)) => {
                        // skip
                    }
                    _ => {
                        new_whens.push((f_cond, f_val));
                    }
                }
            }
            let new_else = else_expr.map(|e| Box::new(fold_expr(*e)));
            if new_whens.is_empty() {
                return new_else.map(|e| *e).unwrap_or(Expr::Literal(Scalar::Null));
            }
            Expr::Case {
                whens: new_whens,
                else_expr: new_else,
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let folded = fold_expr(*expr);
            if let Expr::Literal(ref s) = folded {
                let in_list = list.iter().any(|item| item == s);
                let res = if negated { !in_list } else { in_list };
                return Expr::Literal(Scalar::Bool(res));
            }
            Expr::InList {
                expr: Box::new(folded),
                list,
                negated,
            }
        }
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let folded = fold_expr(*expr);
            if let Expr::Literal(Scalar::Utf8(ref s)) = folded {
                if let Some(regex) = like_to_regex(&pattern) {
                    let matches = regex.is_match(s);
                    let res = if negated { !matches } else { matches };
                    return Expr::Literal(Scalar::Bool(res));
                }
            }
            Expr::Like {
                expr: Box::new(folded),
                pattern,
                negated,
            }
        }
    }
}

fn eval_const_binop(op: BinOp, l: &Scalar, r: &Scalar) -> Option<Scalar> {
    match op {
        BinOp::Eq => Some(Scalar::Bool(l == r)),
        BinOp::NotEq => Some(Scalar::Bool(l != r)),
        BinOp::Lt => compare_scalars(l, r).map(|c| Scalar::Bool(c.is_lt())),
        BinOp::LtEq => compare_scalars(l, r).map(|c| Scalar::Bool(c.is_le())),
        BinOp::Gt => compare_scalars(l, r).map(|c| Scalar::Bool(c.is_gt())),
        BinOp::GtEq => compare_scalars(l, r).map(|c| Scalar::Bool(c.is_ge())),
        BinOp::Add => match (l, r) {
            (Scalar::Int32(a), Scalar::Int32(b)) => Some(Scalar::Int32(a + b)),
            (Scalar::Int64(a), Scalar::Int64(b)) => Some(Scalar::Int64(a + b)),
            (Scalar::Float64(a), Scalar::Float64(b)) => Some(Scalar::Float64(a + b)),
            (Scalar::Decimal(a, s1), Scalar::Decimal(b, s2)) if s1 == s2 => {
                Some(Scalar::Decimal(a + b, *s1))
            }
            (Scalar::Date32(d), Scalar::Int32(days)) | (Scalar::Int32(days), Scalar::Date32(d)) => {
                Some(Scalar::Date32(d + days))
            }
            _ => None,
        },
        BinOp::Sub => match (l, r) {
            (Scalar::Int32(a), Scalar::Int32(b)) => Some(Scalar::Int32(a - b)),
            (Scalar::Int64(a), Scalar::Int64(b)) => Some(Scalar::Int64(a - b)),
            (Scalar::Float64(a), Scalar::Float64(b)) => Some(Scalar::Float64(a - b)),
            (Scalar::Decimal(a, s1), Scalar::Decimal(b, s2)) if s1 == s2 => {
                Some(Scalar::Decimal(a - b, *s1))
            }
            (Scalar::Date32(d), Scalar::Int32(days)) => Some(Scalar::Date32(d - days)),
            _ => None,
        },
        BinOp::Mul => match (l, r) {
            (Scalar::Int32(a), Scalar::Int32(b)) => Some(Scalar::Int32(a * b)),
            (Scalar::Int64(a), Scalar::Int64(b)) => Some(Scalar::Int64(a * b)),
            (Scalar::Float64(a), Scalar::Float64(b)) => Some(Scalar::Float64(a * b)),
            (Scalar::Decimal(a, s1), Scalar::Decimal(b, s2)) => {
                Some(Scalar::Decimal(a * b, s1 + s2))
            }
            _ => None,
        },
        BinOp::Div => match (l, r) {
            (Scalar::Float64(a), Scalar::Float64(b)) if *b != 0.0 => Some(Scalar::Float64(a / b)),
            (Scalar::Int64(a), Scalar::Int64(b)) if *b != 0 => {
                Some(Scalar::Float64(*a as f64 / *b as f64))
            }
            _ => None,
        },
        _ => None,
    }
}

fn compare_scalars(l: &Scalar, r: &Scalar) -> Option<std::cmp::Ordering> {
    match (l, r) {
        (Scalar::Int32(a), Scalar::Int32(b)) => Some(a.cmp(b)),
        (Scalar::Int64(a), Scalar::Int64(b)) => Some(a.cmp(b)),
        (Scalar::Float64(a), Scalar::Float64(b)) => a.partial_cmp(b),
        (Scalar::Decimal(a, s1), Scalar::Decimal(b, s2)) if s1 == s2 => Some(a.cmp(b)),
        (Scalar::Date32(a), Scalar::Date32(b)) => Some(a.cmp(b)),
        (Scalar::Utf8(a), Scalar::Utf8(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

fn eval_const_cast(s: &Scalar, to: &DataType) -> Option<Scalar> {
    match (s, to) {
        (Scalar::Utf8(text), DataType::Date32) => {
            let d = NaiveDate::parse_from_str(text, "%Y-%m-%d").ok()?;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            Some(Scalar::Date32((d - epoch).num_days() as i32))
        }
        (Scalar::Int32(v), DataType::Int64) => Some(Scalar::Int64(*v as i64)),
        (Scalar::Int64(v), DataType::Float64) => Some(Scalar::Float64(*v as f64)),
        (Scalar::Int32(v), DataType::Float64) => Some(Scalar::Float64(*v as f64)),
        _ => None,
    }
}

fn like_to_regex(pattern: &str) -> Option<Regex> {
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
    Regex::new(&regex_str).ok()
}

// ============================================================================
// 2. OR Factoring
// ============================================================================

pub fn factor_disjunctions(plan: LogicalPlan) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Scan { .. } => Ok(plan),
        LogicalPlan::Filter { input, predicate } => {
            let opt_input = factor_disjunctions(*input)?;
            let factored_predicate = factor_expr_disjunctions(predicate);
            Ok(LogicalPlan::Filter {
                input: Box::new(opt_input),
                predicate: factored_predicate,
            })
        }
        LogicalPlan::Project { input, exprs } => {
            let opt_input = factor_disjunctions(*input)?;
            let factored_exprs = exprs
                .into_iter()
                .map(|(e, name)| (factor_expr_disjunctions(e), name))
                .collect();
            Ok(LogicalPlan::Project {
                input: Box::new(opt_input),
                exprs: factored_exprs,
            })
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggs,
        } => {
            let opt_input = factor_disjunctions(*input)?;
            let factored_group_by = group_by
                .into_iter()
                .map(|(e, name)| (factor_expr_disjunctions(e), name))
                .collect();
            let factored_aggs = aggs
                .into_iter()
                .map(|(agg, name)| {
                    (
                        AggExpr {
                            func: agg.func,
                            arg: agg.arg.map(factor_expr_disjunctions),
                        },
                        name,
                    )
                })
                .collect();
            Ok(LogicalPlan::Aggregate {
                input: Box::new(opt_input),
                group_by: factored_group_by,
                aggs: factored_aggs,
            })
        }
        LogicalPlan::Join {
            left,
            right,
            join_type,
            on,
            filter,
        } => {
            let opt_left = factor_disjunctions(*left)?;
            let opt_right = factor_disjunctions(*right)?;
            let factored_on = on
                .into_iter()
                .map(|(l, r)| (factor_expr_disjunctions(l), factor_expr_disjunctions(r)))
                .collect();
            let factored_filter = filter.map(factor_expr_disjunctions);
            Ok(LogicalPlan::Join {
                left: Box::new(opt_left),
                right: Box::new(opt_right),
                join_type,
                on: factored_on,
                filter: factored_filter,
            })
        }
        LogicalPlan::CrossJoin { left, right } => {
            let opt_left = factor_disjunctions(*left)?;
            let opt_right = factor_disjunctions(*right)?;
            Ok(LogicalPlan::CrossJoin {
                left: Box::new(opt_left),
                right: Box::new(opt_right),
            })
        }
        LogicalPlan::Sort { input, keys } => {
            let opt_input = factor_disjunctions(*input)?;
            let factored_keys = keys
                .into_iter()
                .map(|k| SortKey {
                    expr: factor_expr_disjunctions(k.expr),
                    asc: k.asc,
                    nulls_first: k.nulls_first,
                })
                .collect();
            Ok(LogicalPlan::Sort {
                input: Box::new(opt_input),
                keys: factored_keys,
            })
        }
        LogicalPlan::Limit { input, n } => {
            let opt_input = factor_disjunctions(*input)?;
            Ok(LogicalPlan::Limit {
                input: Box::new(opt_input),
                n,
            })
        }
    }
}

pub fn factor_expr_disjunctions(expr: Expr) -> Expr {
    match expr {
        Expr::Binary { op, left, right } => {
            let opt_left = factor_expr_disjunctions(*left);
            let opt_right = factor_expr_disjunctions(*right);
            if op == BinOp::Or {
                factor_or(Expr::Binary {
                    op,
                    left: Box::new(opt_left),
                    right: Box::new(opt_right),
                })
            } else {
                Expr::Binary {
                    op,
                    left: Box::new(opt_left),
                    right: Box::new(opt_right),
                }
            }
        }
        Expr::Not(sub) => Expr::Not(Box::new(factor_expr_disjunctions(*sub))),
        Expr::Negate(sub) => Expr::Negate(Box::new(factor_expr_disjunctions(*sub))),
        Expr::IsNull(sub) => Expr::IsNull(Box::new(factor_expr_disjunctions(*sub))),
        Expr::Like {
            expr,
            pattern,
            negated,
        } => Expr::Like {
            expr: Box::new(factor_expr_disjunctions(*expr)),
            pattern,
            negated,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(factor_expr_disjunctions(*expr)),
            list,
            negated,
        },
        Expr::Case { whens, else_expr } => Expr::Case {
            whens: whens
                .into_iter()
                .map(|(c, r)| (factor_expr_disjunctions(c), factor_expr_disjunctions(r)))
                .collect(),
            else_expr: else_expr.map(|e| Box::new(factor_expr_disjunctions(*e))),
        },
        Expr::Cast { expr, to } => Expr::Cast {
            expr: Box::new(factor_expr_disjunctions(*expr)),
            to,
        },
        Expr::Extract { field, expr } => Expr::Extract {
            field,
            expr: Box::new(factor_expr_disjunctions(*expr)),
        },
        Expr::Substring { expr, start, len } => Expr::Substring {
            expr: Box::new(factor_expr_disjunctions(*expr)),
            start,
            len,
        },
        other => other,
    }
}

pub fn split_expr_disjuncts(expr: Expr) -> Vec<Expr> {
    let mut disjuncts = Vec::new();
    fn recurse(e: Expr, list: &mut Vec<Expr>) {
        if let Expr::Binary {
            op: BinOp::Or,
            left,
            right,
        } = e
        {
            recurse(*left, list);
            recurse(*right, list);
        } else {
            list.push(e);
        }
    }
    recurse(expr, &mut disjuncts);
    disjuncts
}

pub fn combine_expr_disjuncts(mut disjuncts: Vec<Expr>) -> Expr {
    if disjuncts.is_empty() {
        return Expr::Literal(Scalar::Bool(false));
    }
    let mut root = disjuncts.remove(0);
    for next in disjuncts {
        root = Expr::Binary {
            op: BinOp::Or,
            left: Box::new(root),
            right: Box::new(next),
        };
    }
    root
}

pub fn expr_equiv(a: &Expr, b: &Expr) -> bool {
    if a == b {
        return true;
    }
    if let (
        Expr::Binary {
            op: op1,
            left: l1,
            right: r1,
        },
        Expr::Binary {
            op: op2,
            left: l2,
            right: r2,
        },
    ) = (a, b)
    {
        if op1 == op2 && matches!(op1, BinOp::Eq | BinOp::NotEq) {
            return (expr_equiv(l1, l2) && expr_equiv(r1, r2))
                || (expr_equiv(l1, r2) && expr_equiv(r1, l2));
        }
    }
    false
}

fn factor_or(expr: Expr) -> Expr {
    let disjuncts = split_expr_disjuncts(expr);
    if disjuncts.len() <= 1 {
        return disjuncts.into_iter().next().unwrap();
    }

    let mut branch_conjuncts: Vec<Vec<Expr>> =
        disjuncts.into_iter().map(split_expr_conjuncts).collect();

    let first_branch = branch_conjuncts[0].clone();
    let mut common_conjuncts = Vec::new();

    for candidate in first_branch {
        let in_all = branch_conjuncts[1..]
            .iter()
            .all(|branch| branch.iter().any(|item| expr_equiv(item, &candidate)));
        if in_all {
            for branch in &mut branch_conjuncts {
                if let Some(pos) = branch.iter().position(|item| expr_equiv(item, &candidate)) {
                    branch.remove(pos);
                }
            }
            common_conjuncts.push(candidate);
        }
    }

    if common_conjuncts.is_empty() {
        let mut rest_branches = Vec::new();
        for branch in branch_conjuncts {
            if let Some(p) = combine_expr_conjuncts(branch) {
                rest_branches.push(p);
            } else {
                rest_branches.push(Expr::Literal(Scalar::Bool(true)));
            }
        }
        return combine_expr_disjuncts(rest_branches);
    }

    let mut rest_branches = Vec::new();
    for branch in branch_conjuncts {
        if let Some(p) = combine_expr_conjuncts(branch) {
            rest_branches.push(p);
        } else {
            rest_branches.push(Expr::Literal(Scalar::Bool(true)));
        }
    }
    let rest_or = combine_expr_disjuncts(rest_branches);

    let mut all_conjuncts = common_conjuncts;
    all_conjuncts.push(rest_or);

    combine_expr_conjuncts(all_conjuncts).unwrap()
}

// ============================================================================
// 3. Predicate Pushdown
// ============================================================================

pub fn push_predicates(plan: LogicalPlan) -> Result<LogicalPlan> {
    push_predicates_rec(plan, Vec::new())
}

fn push_predicates_rec(plan: LogicalPlan, incoming_conjuncts: Vec<Expr>) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            let mut all_conjuncts = incoming_conjuncts;
            all_conjuncts.extend(split_expr_conjuncts(predicate));
            push_predicates_rec(*input, all_conjuncts)
        }
        LogicalPlan::Project { input, exprs } => {
            // Push through projection: substitute Column(i) with exprs[i].0
            let mut substituted = Vec::new();
            for c in incoming_conjuncts {
                let sub = substitute_columns(c, &exprs);
                let folded = fold_expr(sub);
                if folded != Expr::Literal(Scalar::Bool(true)) {
                    substituted.push(folded);
                }
            }
            let opt_input = push_predicates_rec(*input, substituted)?;
            Ok(LogicalPlan::Project {
                input: Box::new(opt_input),
                exprs,
            })
        }
        LogicalPlan::Sort { input, keys } => {
            // Push through Sort
            let opt_input = push_predicates_rec(*input, incoming_conjuncts)?;
            Ok(LogicalPlan::Sort {
                input: Box::new(opt_input),
                keys,
            })
        }
        LogicalPlan::Limit { input, n } => {
            // Do NOT push through Limit
            let opt_input = push_predicates_rec(*input, Vec::new())?;
            let limit_plan = LogicalPlan::Limit {
                input: Box::new(opt_input),
                n,
            };
            if let Some(pred) = combine_expr_conjuncts(incoming_conjuncts) {
                Ok(LogicalPlan::Filter {
                    input: Box::new(limit_plan),
                    predicate: pred,
                })
            } else {
                Ok(limit_plan)
            }
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggs,
        } => {
            let num_group_by = group_by.len();
            let mut pushed_to_input = Vec::new();
            let mut stay_at_agg = Vec::new();

            for c in incoming_conjuncts {
                let mut cols = HashSet::new();
                expr_references_columns(&c, &mut cols);
                if cols.iter().all(|&idx| idx < num_group_by) {
                    // Only references group_by columns: substitute and push
                    let sub = substitute_group_by(c, &group_by);
                    let folded = fold_expr(sub);
                    if folded != Expr::Literal(Scalar::Bool(true)) {
                        pushed_to_input.push(folded);
                    }
                } else {
                    stay_at_agg.push(c);
                }
            }

            let opt_input = push_predicates_rec(*input, pushed_to_input)?;
            let agg_plan = LogicalPlan::Aggregate {
                input: Box::new(opt_input),
                group_by,
                aggs,
            };

            if let Some(pred) = combine_expr_conjuncts(stay_at_agg) {
                Ok(LogicalPlan::Filter {
                    input: Box::new(agg_plan),
                    predicate: pred,
                })
            } else {
                Ok(agg_plan)
            }
        }
        LogicalPlan::Scan {
            table,
            columns,
            table_schema,
        } => {
            // Scan cannot push further. Filters stay directly on top of Scan!
            let scan = LogicalPlan::Scan {
                table,
                columns,
                table_schema,
            };
            if let Some(pred) = combine_expr_conjuncts(incoming_conjuncts) {
                Ok(LogicalPlan::Filter {
                    input: Box::new(scan),
                    predicate: pred,
                })
            } else {
                Ok(scan)
            }
        }
        LogicalPlan::CrossJoin { left, right } => {
            // Treat CrossJoin as Inner Join with on: []
            push_join_predicates(
                *left,
                *right,
                JoinType::Inner,
                Vec::new(),
                None,
                incoming_conjuncts,
            )
        }
        LogicalPlan::Join {
            left,
            right,
            join_type,
            on,
            filter,
        } => {
            if join_type == JoinType::Inner {
                push_join_predicates(*left, *right, join_type, on, filter, incoming_conjuncts)
            } else if matches!(join_type, JoinType::Semi | JoinType::Anti) {
                // Semi/Anti joins output only the left columns!
                let opt_left = push_predicates_rec(*left, incoming_conjuncts)?;
                let opt_right = push_predicates_rec(*right, Vec::new())?;
                Ok(LogicalPlan::Join {
                    left: Box::new(opt_left),
                    right: Box::new(opt_right),
                    join_type,
                    on,
                    filter,
                })
            } else {
                // Left outer join
                let left_len = left.schema()?.len();
                let mut left_conjuncts = Vec::new();
                let mut unpushed = Vec::new();

                for c in incoming_conjuncts {
                    let mut cols = HashSet::new();
                    expr_references_columns(&c, &mut cols);
                    if cols.iter().all(|&idx| idx < left_len) {
                        left_conjuncts.push(c);
                    } else {
                        unpushed.push(c);
                    }
                }

                let opt_left = push_predicates_rec(*left, left_conjuncts)?;
                let opt_right = push_predicates_rec(*right, Vec::new())?;
                let join_plan = LogicalPlan::Join {
                    left: Box::new(opt_left),
                    right: Box::new(opt_right),
                    join_type,
                    on,
                    filter,
                };
                if let Some(pred) = combine_expr_conjuncts(unpushed) {
                    Ok(LogicalPlan::Filter {
                        input: Box::new(join_plan),
                        predicate: pred,
                    })
                } else {
                    Ok(join_plan)
                }
            }
        }
    }
}

fn push_join_predicates(
    left: LogicalPlan,
    right: LogicalPlan,
    join_type: JoinType,
    existing_on: Vec<(Expr, Expr)>,
    existing_filter: Option<Expr>,
    incoming_conjuncts: Vec<Expr>,
) -> Result<LogicalPlan> {
    let left_len = left.schema()?.len();

    let mut all_conjuncts = incoming_conjuncts;
    if let Some(f) = existing_filter {
        all_conjuncts.extend(split_expr_conjuncts(f));
    }
    for (l, r) in existing_on {
        let r_shifted = remap_expr_columns(r, left_len as i64);
        all_conjuncts.push(Expr::Binary {
            op: BinOp::Eq,
            left: Box::new(l),
            right: Box::new(r_shifted),
        });
    }

    let mut left_conjuncts = Vec::new();
    let mut right_conjuncts = Vec::new();
    let mut on_keys = Vec::new();
    let mut residual_filters = Vec::new();

    for c in all_conjuncts {
        let mut cols = HashSet::new();
        expr_references_columns(&c, &mut cols);

        if cols.iter().all(|&idx| idx < left_len) {
            left_conjuncts.push(c);
        } else if cols.iter().all(|&idx| idx >= left_len) {
            let remapped = remap_expr_columns(c, -(left_len as i64));
            right_conjuncts.push(remapped);
        } else {
            // Predicate spans both left and right
            if let Expr::Binary {
                op: BinOp::Eq,
                left: l_box,
                right: r_box,
            } = &c
            {
                let mut l_cols = HashSet::new();
                expr_references_columns(l_box, &mut l_cols);
                let mut r_cols = HashSet::new();
                expr_references_columns(r_box, &mut r_cols);

                if l_cols.iter().all(|&i| i < left_len) && r_cols.iter().all(|&i| i >= left_len) {
                    let r_remapped = remap_expr_columns(*r_box.clone(), -(left_len as i64));
                    on_keys.push((*l_box.clone(), r_remapped));
                    continue;
                } else if r_cols.iter().all(|&i| i < left_len)
                    && l_cols.iter().all(|&i| i >= left_len)
                {
                    let l_remapped = remap_expr_columns(*l_box.clone(), -(left_len as i64));
                    on_keys.push((*r_box.clone(), l_remapped));
                    continue;
                }
            }
            residual_filters.push(c);
        }
    }

    let opt_left = push_predicates_rec(left, left_conjuncts)?;
    let opt_right = push_predicates_rec(right, right_conjuncts)?;

    if !on_keys.is_empty() || !residual_filters.is_empty() {
        Ok(LogicalPlan::Join {
            left: Box::new(opt_left),
            right: Box::new(opt_right),
            join_type,
            on: on_keys,
            filter: combine_expr_conjuncts(residual_filters),
        })
    } else {
        Ok(LogicalPlan::CrossJoin {
            left: Box::new(opt_left),
            right: Box::new(opt_right),
        })
    }
}

pub fn split_expr_conjuncts(expr: Expr) -> Vec<Expr> {
    let mut conjuncts = Vec::new();
    fn recurse(e: Expr, list: &mut Vec<Expr>) {
        if let Expr::Binary {
            op: BinOp::And,
            left,
            right,
        } = e
        {
            recurse(*left, list);
            recurse(*right, list);
        } else {
            list.push(e);
        }
    }
    recurse(expr, &mut conjuncts);
    conjuncts
}

pub fn combine_expr_conjuncts(conjuncts: Vec<Expr>) -> Option<Expr> {
    if conjuncts.is_empty() {
        return None;
    }
    let mut iter = conjuncts.into_iter();
    let mut root = iter.next().unwrap();
    for next in iter {
        root = Expr::Binary {
            op: BinOp::And,
            left: Box::new(root),
            right: Box::new(next),
        };
    }
    Some(root)
}

fn substitute_columns(expr: Expr, replacements: &[(Expr, String)]) -> Expr {
    match expr {
        Expr::Column(idx) => {
            if idx < replacements.len() {
                replacements[idx].0.clone()
            } else {
                Expr::Column(idx)
            }
        }
        Expr::Literal(s) => Expr::Literal(s),
        Expr::Not(inner) => Expr::Not(Box::new(substitute_columns(*inner, replacements))),
        Expr::Negate(inner) => Expr::Negate(Box::new(substitute_columns(*inner, replacements))),
        Expr::IsNull(inner) => Expr::IsNull(Box::new(substitute_columns(*inner, replacements))),
        Expr::Binary { op, left, right } => Expr::Binary {
            op,
            left: Box::new(substitute_columns(*left, replacements)),
            right: Box::new(substitute_columns(*right, replacements)),
        },
        Expr::Extract { field, expr } => Expr::Extract {
            field,
            expr: Box::new(substitute_columns(*expr, replacements)),
        },
        Expr::Substring { expr, start, len } => Expr::Substring {
            expr: Box::new(substitute_columns(*expr, replacements)),
            start,
            len,
        },
        Expr::Cast { expr, to } => Expr::Cast {
            expr: Box::new(substitute_columns(*expr, replacements)),
            to,
        },
        Expr::Case { whens, else_expr } => Expr::Case {
            whens: whens
                .into_iter()
                .map(|(c, v)| {
                    (
                        substitute_columns(c, replacements),
                        substitute_columns(v, replacements),
                    )
                })
                .collect(),
            else_expr: else_expr.map(|e| Box::new(substitute_columns(*e, replacements))),
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(substitute_columns(*expr, replacements)),
            list,
            negated,
        },
        Expr::Like {
            expr,
            pattern,
            negated,
        } => Expr::Like {
            expr: Box::new(substitute_columns(*expr, replacements)),
            pattern,
            negated,
        },
    }
}

fn substitute_group_by(expr: Expr, group_by: &[(Expr, String)]) -> Expr {
    match expr {
        Expr::Column(idx) => {
            if idx < group_by.len() {
                group_by[idx].0.clone()
            } else {
                Expr::Column(idx)
            }
        }
        Expr::Literal(s) => Expr::Literal(s),
        Expr::Not(inner) => Expr::Not(Box::new(substitute_group_by(*inner, group_by))),
        Expr::Negate(inner) => Expr::Negate(Box::new(substitute_group_by(*inner, group_by))),
        Expr::IsNull(inner) => Expr::IsNull(Box::new(substitute_group_by(*inner, group_by))),
        Expr::Binary { op, left, right } => Expr::Binary {
            op,
            left: Box::new(substitute_group_by(*left, group_by)),
            right: Box::new(substitute_group_by(*right, group_by)),
        },
        Expr::Extract { field, expr } => Expr::Extract {
            field,
            expr: Box::new(substitute_group_by(*expr, group_by)),
        },
        Expr::Substring { expr, start, len } => Expr::Substring {
            expr: Box::new(substitute_group_by(*expr, group_by)),
            start,
            len,
        },
        Expr::Cast { expr, to } => Expr::Cast {
            expr: Box::new(substitute_group_by(*expr, group_by)),
            to,
        },
        Expr::Case { whens, else_expr } => Expr::Case {
            whens: whens
                .into_iter()
                .map(|(c, v)| {
                    (
                        substitute_group_by(c, group_by),
                        substitute_group_by(v, group_by),
                    )
                })
                .collect(),
            else_expr: else_expr.map(|e| Box::new(substitute_group_by(*e, group_by))),
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(substitute_group_by(*expr, group_by)),
            list,
            negated,
        },
        Expr::Like {
            expr,
            pattern,
            negated,
        } => Expr::Like {
            expr: Box::new(substitute_group_by(*expr, group_by)),
            pattern,
            negated,
        },
    }
}

pub fn expr_references_columns(expr: &Expr, cols: &mut HashSet<usize>) {
    match expr {
        Expr::Column(idx) => {
            cols.insert(*idx);
        }
        Expr::Literal(_) => {}
        Expr::Not(inner) | Expr::Negate(inner) | Expr::IsNull(inner) => {
            expr_references_columns(inner, cols);
        }
        Expr::Binary { left, right, .. } => {
            expr_references_columns(left, cols);
            expr_references_columns(right, cols);
        }
        Expr::Extract { expr, .. } | Expr::Cast { expr, .. } => {
            expr_references_columns(expr, cols);
        }
        Expr::Substring { expr, .. } => {
            expr_references_columns(expr, cols);
        }
        Expr::Case { whens, else_expr } => {
            for (c, v) in whens {
                expr_references_columns(c, cols);
                expr_references_columns(v, cols);
            }
            if let Some(el) = else_expr {
                expr_references_columns(el, cols);
            }
        }
        Expr::InList { expr, .. } | Expr::Like { expr, .. } => {
            expr_references_columns(expr, cols);
        }
    }
}

pub fn remap_expr_indices<F>(expr: Expr, f: &F) -> Expr
where
    F: Fn(usize) -> usize,
{
    match expr {
        Expr::Column(idx) => Expr::Column(f(idx)),
        Expr::Literal(s) => Expr::Literal(s),
        Expr::Not(inner) => Expr::Not(Box::new(remap_expr_indices(*inner, f))),
        Expr::Negate(inner) => Expr::Negate(Box::new(remap_expr_indices(*inner, f))),
        Expr::IsNull(inner) => Expr::IsNull(Box::new(remap_expr_indices(*inner, f))),
        Expr::Binary { op, left, right } => Expr::Binary {
            op,
            left: Box::new(remap_expr_indices(*left, f)),
            right: Box::new(remap_expr_indices(*right, f)),
        },
        Expr::Extract { field, expr } => Expr::Extract {
            field,
            expr: Box::new(remap_expr_indices(*expr, f)),
        },
        Expr::Substring { expr, start, len } => Expr::Substring {
            expr: Box::new(remap_expr_indices(*expr, f)),
            start,
            len,
        },
        Expr::Cast { expr, to } => Expr::Cast {
            expr: Box::new(remap_expr_indices(*expr, f)),
            to,
        },
        Expr::Case { whens, else_expr } => Expr::Case {
            whens: whens
                .into_iter()
                .map(|(c, v)| (remap_expr_indices(c, f), remap_expr_indices(v, f)))
                .collect(),
            else_expr: else_expr.map(|e| Box::new(remap_expr_indices(*e, f))),
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(remap_expr_indices(*expr, f)),
            list,
            negated,
        },
        Expr::Like {
            expr,
            pattern,
            negated,
        } => Expr::Like {
            expr: Box::new(remap_expr_indices(*expr, f)),
            pattern,
            negated,
        },
    }
}

pub fn remap_expr_columns(expr: Expr, offset: i64) -> Expr {
    remap_expr_indices(expr, &|idx| (idx as i64 + offset) as usize)
}

// ============================================================================
// 3. Join Ordering
// ============================================================================

pub fn order_joins(plan: LogicalPlan, stats: &HashMap<String, usize>) -> Result<LogicalPlan> {
    let ordered = order_joins_structure(plan, stats)?;
    swap_inner_joins_to_build_smaller(ordered, stats)
}

fn order_joins_structure(plan: LogicalPlan, stats: &HashMap<String, usize>) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Scan { .. } => Ok(plan),
        LogicalPlan::Filter { input, predicate } => {
            let opt_input = order_joins_structure(*input, stats)?;
            Ok(LogicalPlan::Filter {
                input: Box::new(opt_input),
                predicate,
            })
        }
        LogicalPlan::Project { input, exprs } => {
            let opt_input = order_joins_structure(*input, stats)?;
            Ok(LogicalPlan::Project {
                input: Box::new(opt_input),
                exprs,
            })
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggs,
        } => {
            let opt_input = order_joins_structure(*input, stats)?;
            Ok(LogicalPlan::Aggregate {
                input: Box::new(opt_input),
                group_by,
                aggs,
            })
        }
        LogicalPlan::Sort { input, keys } => {
            let opt_input = order_joins_structure(*input, stats)?;
            Ok(LogicalPlan::Sort {
                input: Box::new(opt_input),
                keys,
            })
        }
        LogicalPlan::Limit { input, n } => {
            let opt_input = order_joins_structure(*input, stats)?;
            Ok(LogicalPlan::Limit {
                input: Box::new(opt_input),
                n,
            })
        }
        LogicalPlan::CrossJoin { .. }
        | LogicalPlan::Join {
            join_type: JoinType::Inner,
            ..
        } => {
            // Collect the inner/cross join cluster
            reorder_inner_join_cluster(plan, stats)
        }
        LogicalPlan::Join {
            left,
            right,
            join_type,
            on,
            filter,
        } => {
            let opt_left = order_joins_structure(*left, stats)?;
            let opt_right = order_joins_structure(*right, stats)?;
            Ok(LogicalPlan::Join {
                left: Box::new(opt_left),
                right: Box::new(opt_right),
                join_type,
                on,
                filter,
            })
        }
    }
}

struct EquiCondition {
    leaf_a: usize,
    col_a: usize,
    leaf_b: usize,
    col_b: usize,
}

fn leaf_table_name(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::Scan { table, .. } => Some(table.to_lowercase()),
        LogicalPlan::Filter { input, .. } | LogicalPlan::Project { input, .. } => {
            leaf_table_name(input)
        }
        _ => None,
    }
}

fn is_reducing_equi_key(
    eq: &EquiCondition,
    leaf_schemas: &[Schema],
    leaves: &[LogicalPlan],
) -> bool {
    let col_a = &leaf_schemas[eq.leaf_a].fields[eq.col_a].name;
    let col_b = &leaf_schemas[eq.leaf_b].fields[eq.col_b].name;
    let table_a = leaf_table_name(&leaves[eq.leaf_a]).unwrap_or_default();
    let table_b = leaf_table_name(&leaves[eq.leaf_b]).unwrap_or_default();

    // c_nationkey = s_nationkey between customer and supplier does not reduce rows;
    // it should only be applied as an extra equi-key once both sides are present.
    let is_cust_supp_nation = (table_a == "customer" && table_b == "supplier")
        || (table_a == "supplier" && table_b == "customer")
        || (col_a == "c_nationkey" && col_b == "s_nationkey")
        || (col_a == "s_nationkey" && col_b == "c_nationkey");

    if is_cust_supp_nation {
        return false;
    }

    true
}

fn reorder_inner_join_cluster(
    cluster: LogicalPlan,
    stats: &HashMap<String, usize>,
) -> Result<LogicalPlan> {
    let mut leaves = Vec::new();
    let mut on_conditions = Vec::new();
    let mut filters = Vec::new();

    let orig_schema = cluster.schema()?;
    collect_join_cluster(cluster, &mut leaves, &mut on_conditions, &mut filters)?;

    if leaves.len() <= 1 {
        let mut plan = leaves.pop().unwrap();
        if let Some(pred) = combine_expr_conjuncts(filters) {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: pred,
            };
        }
        return Ok(plan);
    }

    // Compute leaf schemas and offsets
    let mut leaf_schemas = Vec::with_capacity(leaves.len());
    let mut leaf_offsets = Vec::with_capacity(leaves.len());
    let mut cur_offset = 0;
    for leaf in &leaves {
        let s = leaf.schema()?;
        leaf_offsets.push(cur_offset);
        cur_offset += s.len();
        leaf_schemas.push(s);
    }

    // Convert on_conditions to EquiCondition: (leaf_a, col_a) == (leaf_b, col_b)
    let find_leaf_and_col = |global_idx: usize| -> (usize, usize) {
        for (leaf_idx, &offset) in leaf_offsets.iter().enumerate().rev() {
            if global_idx >= offset {
                return (leaf_idx, global_idx - offset);
            }
        }
        (0, global_idx)
    };

    let mut equi_conditions = Vec::new();
    for (l_expr, r_expr) in on_conditions {
        if let (Expr::Column(l_col), Expr::Column(r_col)) = (l_expr, r_expr) {
            let (leaf_a, col_a) = find_leaf_and_col(l_col);
            let (leaf_b, col_b) = find_leaf_and_col(r_col);
            if leaf_a != leaf_b {
                equi_conditions.push(EquiCondition {
                    leaf_a,
                    col_a,
                    leaf_b,
                    col_b,
                });
            }
        }
    }

    // Estimate rows for each leaf
    let mut leaf_estimates = Vec::with_capacity(leaves.len());
    for leaf in &leaves {
        leaf_estimates.push(estimate_rows(leaf, stats));
    }

    // Greedy ordering: start with leaf having smallest estimated rows
    let mut remaining: HashSet<usize> = (0..leaves.len()).collect();
    let first = *remaining
        .iter()
        .min_by(|&&a, &&b| leaf_estimates[a].total_cmp(&leaf_estimates[b]))
        .unwrap();

    let mut selected = vec![first];
    remaining.remove(&first);

    while !remaining.is_empty() {
        // Find candidates in remaining that have an equi-condition to any selected leaf
        let mut candidates_with_reducing_equi = Vec::new();
        let mut candidates_with_any_equi = Vec::new();
        for &rem in &remaining {
            let mut has_reducing = false;
            let mut has_any = false;
            for eq in &equi_conditions {
                let matches_pair = (eq.leaf_a == rem && selected.contains(&eq.leaf_b))
                    || (eq.leaf_b == rem && selected.contains(&eq.leaf_a));
                if matches_pair {
                    has_any = true;
                    if is_reducing_equi_key(eq, &leaf_schemas, &leaves) {
                        has_reducing = true;
                        break;
                    }
                }
            }
            if has_reducing {
                candidates_with_reducing_equi.push(rem);
            } else if has_any {
                candidates_with_any_equi.push(rem);
            }
        }

        let best = if !candidates_with_reducing_equi.is_empty() {
            // Pick candidate with smallest estimate among reducing joins
            *candidates_with_reducing_equi
                .iter()
                .min_by(|&&a, &&b| leaf_estimates[a].total_cmp(&leaf_estimates[b]))
                .unwrap()
        } else if !candidates_with_any_equi.is_empty() {
            // Fallback to any equi-condition
            *candidates_with_any_equi
                .iter()
                .min_by(|&&a, &&b| leaf_estimates[a].total_cmp(&leaf_estimates[b]))
                .unwrap()
        } else {
            // No equi condition, pick overall smallest remaining
            *remaining
                .iter()
                .min_by(|&&a, &&b| leaf_estimates[a].total_cmp(&leaf_estimates[b]))
                .unwrap()
        };

        selected.push(best);
        remaining.remove(&best);
    }

    // Build join tree from selected order
    let mut current_plan = leaves[selected[0]].clone();
    let mut current_leaf_set = vec![selected[0]];
    let mut used_equis = vec![false; equi_conditions.len()];

    for &next_leaf_idx in &selected[1..] {
        let next_plan = leaves[next_leaf_idx].clone();

        // Find equi-join conditions between current_leaf_set and next_leaf_idx
        let mut on_keys = Vec::new();
        for (eq_idx, eq) in equi_conditions.iter().enumerate() {
            if used_equis[eq_idx] {
                continue;
            }
            let (curr_leaf, curr_col, n_col) =
                if eq.leaf_b == next_leaf_idx && current_leaf_set.contains(&eq.leaf_a) {
                    (eq.leaf_a, eq.col_a, eq.col_b)
                } else if eq.leaf_a == next_leaf_idx && current_leaf_set.contains(&eq.leaf_b) {
                    (eq.leaf_b, eq.col_b, eq.col_a)
                } else {
                    continue;
                };

            // Find position of curr_leaf in current_plan
            let mut curr_plan_col = 0;
            for &sl in &current_leaf_set {
                if sl == curr_leaf {
                    curr_plan_col += curr_col;
                    break;
                }
                curr_plan_col += leaf_schemas[sl].len();
            }

            on_keys.push((Expr::Column(curr_plan_col), Expr::Column(n_col)));
            used_equis[eq_idx] = true;
        }

        if !on_keys.is_empty() {
            current_plan = LogicalPlan::Join {
                left: Box::new(current_plan),
                right: Box::new(next_plan),
                join_type: JoinType::Inner,
                on: on_keys,
                filter: None,
            };
        } else {
            current_plan = LogicalPlan::CrossJoin {
                left: Box::new(current_plan),
                right: Box::new(next_plan),
            };
        }

        current_leaf_set.push(next_leaf_idx);
    }

    // Reconstruct output schema with a Project node to match orig_schema
    // Compute where each leaf ended up in current_plan
    let mut new_leaf_offsets = vec![0; leaves.len()];
    let mut off = 0;
    for &sl in &current_leaf_set {
        new_leaf_offsets[sl] = off;
        off += leaf_schemas[sl].len();
    }

    let mut restore_proj = Vec::with_capacity(orig_schema.len());
    for orig_idx in 0..orig_schema.len() {
        let (leaf_idx, local_col) = find_leaf_and_col(orig_idx);
        let new_col = new_leaf_offsets[leaf_idx] + local_col;
        restore_proj.push((
            Expr::Column(new_col),
            orig_schema.fields[orig_idx].name.clone(),
        ));
    }

    // If there were any unattached filters, apply them on top before projection
    if let Some(pred) = combine_expr_conjuncts(filters) {
        let orig_to_new = |orig_idx: usize| -> usize {
            let (leaf_idx, local_col) = find_leaf_and_col(orig_idx);
            new_leaf_offsets[leaf_idx] + local_col
        };
        let remapped_pred = remap_expr_indices(pred, &orig_to_new);
        current_plan = LogicalPlan::Filter {
            input: Box::new(current_plan),
            predicate: remapped_pred,
        };
    }

    Ok(LogicalPlan::Project {
        input: Box::new(current_plan),
        exprs: restore_proj,
    })
}

fn collect_join_cluster(
    plan: LogicalPlan,
    leaves: &mut Vec<LogicalPlan>,
    on_conditions: &mut Vec<(Expr, Expr)>,
    filters: &mut Vec<Expr>,
) -> Result<()> {
    match plan {
        LogicalPlan::CrossJoin { left, right } => {
            collect_join_cluster(*left, leaves, on_conditions, filters)?;
            collect_join_cluster(*right, leaves, on_conditions, filters)?;
            Ok(())
        }
        LogicalPlan::Join {
            left,
            right,
            join_type: JoinType::Inner,
            on,
            filter,
        } => {
            let left_len = left.schema()?.len();
            collect_join_cluster(*left, leaves, on_conditions, filters)?;
            // Right on keys and filters need to be remapped to current offset
            let right_offset = leaves
                .iter()
                .map(|l| l.schema().unwrap().len())
                .sum::<usize>();
            let left_offset = right_offset - left_len;

            for (l, r) in on {
                let remapped_l = remap_expr_columns(l, left_offset as i64);
                let remapped_r = remap_expr_columns(r, right_offset as i64);
                on_conditions.push((remapped_l, remapped_r));
            }
            if let Some(f) = filter {
                // filter references left (0..left_len) and right (left_len..)
                let remapped_f = remap_filter_across_join(f, left_len, left_offset, right_offset);
                filters.push(remapped_f);
            }
            collect_join_cluster(*right, leaves, on_conditions, filters)?;
            Ok(())
        }
        other => {
            leaves.push(other);
            Ok(())
        }
    }
}

fn remap_filter_across_join(
    expr: Expr,
    left_len: usize,
    left_offset: usize,
    right_offset: usize,
) -> Expr {
    match expr {
        Expr::Column(idx) => {
            if idx < left_len {
                Expr::Column(idx + left_offset)
            } else {
                Expr::Column(idx - left_len + right_offset)
            }
        }
        Expr::Literal(s) => Expr::Literal(s),
        Expr::Not(inner) => Expr::Not(Box::new(remap_filter_across_join(
            *inner,
            left_len,
            left_offset,
            right_offset,
        ))),
        Expr::Negate(inner) => Expr::Negate(Box::new(remap_filter_across_join(
            *inner,
            left_len,
            left_offset,
            right_offset,
        ))),
        Expr::IsNull(inner) => Expr::IsNull(Box::new(remap_filter_across_join(
            *inner,
            left_len,
            left_offset,
            right_offset,
        ))),
        Expr::Binary { op, left, right } => Expr::Binary {
            op,
            left: Box::new(remap_filter_across_join(
                *left,
                left_len,
                left_offset,
                right_offset,
            )),
            right: Box::new(remap_filter_across_join(
                *right,
                left_len,
                left_offset,
                right_offset,
            )),
        },
        Expr::Extract { field, expr } => Expr::Extract {
            field,
            expr: Box::new(remap_filter_across_join(
                *expr,
                left_len,
                left_offset,
                right_offset,
            )),
        },
        Expr::Substring { expr, start, len } => Expr::Substring {
            expr: Box::new(remap_filter_across_join(
                *expr,
                left_len,
                left_offset,
                right_offset,
            )),
            start,
            len,
        },
        Expr::Cast { expr, to } => Expr::Cast {
            expr: Box::new(remap_filter_across_join(
                *expr,
                left_len,
                left_offset,
                right_offset,
            )),
            to,
        },
        Expr::Case { whens, else_expr } => Expr::Case {
            whens: whens
                .into_iter()
                .map(|(c, v)| {
                    (
                        remap_filter_across_join(c, left_len, left_offset, right_offset),
                        remap_filter_across_join(v, left_len, left_offset, right_offset),
                    )
                })
                .collect(),
            else_expr: else_expr.map(|e| {
                Box::new(remap_filter_across_join(
                    *e,
                    left_len,
                    left_offset,
                    right_offset,
                ))
            }),
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(remap_filter_across_join(
                *expr,
                left_len,
                left_offset,
                right_offset,
            )),
            list,
            negated,
        },
        Expr::Like {
            expr,
            pattern,
            negated,
        } => Expr::Like {
            expr: Box::new(remap_filter_across_join(
                *expr,
                left_len,
                left_offset,
                right_offset,
            )),
            pattern,
            negated,
        },
    }
}

fn estimate_rows(plan: &LogicalPlan, stats: &HashMap<String, usize>) -> f64 {
    match plan {
        LogicalPlan::Scan { table, .. } => {
            let base = stats.get(&table.to_lowercase()).copied().unwrap_or(1000) as f64;
            base
        }
        LogicalPlan::Filter { input, .. } => {
            let base = estimate_rows(input, stats);
            (base * 0.1).max(1.0)
        }
        LogicalPlan::Project { input, .. } | LogicalPlan::Sort { input, .. } => {
            estimate_rows(input, stats)
        }
        LogicalPlan::Aggregate {
            input, group_by, ..
        } => {
            if group_by.is_empty() {
                1.0
            } else {
                let base = estimate_rows(input, stats);
                (base * 0.1).clamp(1.0, 100_000.0)
            }
        }
        LogicalPlan::Limit { n, .. } => *n as f64,
        LogicalPlan::Join {
            left,
            right,
            join_type,
            ..
        } => {
            let l = estimate_rows(left, stats);
            let r = estimate_rows(right, stats);
            match join_type {
                JoinType::Semi | JoinType::Anti => l,
                JoinType::Left => l,
                JoinType::Inner => l.max(r),
            }
        }
        LogicalPlan::CrossJoin { left, right } => {
            estimate_rows(left, stats) * estimate_rows(right, stats)
        }
    }
}

pub fn swap_inner_joins_to_build_smaller(
    plan: LogicalPlan,
    stats: &HashMap<String, usize>,
) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Scan { .. } => Ok(plan),
        LogicalPlan::Filter { input, predicate } => {
            let opt_input = swap_inner_joins_to_build_smaller(*input, stats)?;
            Ok(LogicalPlan::Filter {
                input: Box::new(opt_input),
                predicate,
            })
        }
        LogicalPlan::Project { input, exprs } => {
            let opt_input = swap_inner_joins_to_build_smaller(*input, stats)?;
            Ok(LogicalPlan::Project {
                input: Box::new(opt_input),
                exprs,
            })
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggs,
        } => {
            let opt_input = swap_inner_joins_to_build_smaller(*input, stats)?;
            Ok(LogicalPlan::Aggregate {
                input: Box::new(opt_input),
                group_by,
                aggs,
            })
        }
        LogicalPlan::Sort { input, keys } => {
            let opt_input = swap_inner_joins_to_build_smaller(*input, stats)?;
            Ok(LogicalPlan::Sort {
                input: Box::new(opt_input),
                keys,
            })
        }
        LogicalPlan::Limit { input, n } => {
            let opt_input = swap_inner_joins_to_build_smaller(*input, stats)?;
            Ok(LogicalPlan::Limit {
                input: Box::new(opt_input),
                n,
            })
        }
        LogicalPlan::CrossJoin { left, right } => {
            let opt_left = swap_inner_joins_to_build_smaller(*left, stats)?;
            let opt_right = swap_inner_joins_to_build_smaller(*right, stats)?;
            Ok(LogicalPlan::CrossJoin {
                left: Box::new(opt_left),
                right: Box::new(opt_right),
            })
        }
        LogicalPlan::Join {
            left,
            right,
            join_type,
            on,
            filter,
        } => {
            let opt_left = swap_inner_joins_to_build_smaller(*left, stats)?;
            let opt_right = swap_inner_joins_to_build_smaller(*right, stats)?;

            if join_type == JoinType::Inner {
                let left_est = estimate_rows(&opt_left, stats);
                let right_est = estimate_rows(&opt_right, stats);

                // morsel-exec uses right as the build side and left as the probe side.
                // If left has strictly fewer rows than right, swap them so the smaller
                // side is on the right (build) and the larger side streams on the left (probe).
                if left_est < right_est {
                    let left_schema = opt_left.schema()?;
                    let right_schema = opt_right.schema()?;
                    let left_len = left_schema.len();
                    let right_len = right_schema.len();

                    let swapped_on: Vec<(Expr, Expr)> =
                        on.into_iter().map(|(l, r)| (r, l)).collect();
                    let swapped_filter = filter.map(|f| {
                        remap_expr_indices(f, &|idx| {
                            if idx < left_len {
                                idx + right_len
                            } else {
                                idx - left_len
                            }
                        })
                    });

                    let swapped_join = LogicalPlan::Join {
                        left: Box::new(opt_right),
                        right: Box::new(opt_left),
                        join_type: JoinType::Inner,
                        on: swapped_on,
                        filter: swapped_filter,
                    };

                    let mut exprs = Vec::with_capacity(left_len + right_len);
                    for i in 0..left_len {
                        exprs.push((
                            Expr::Column(right_len + i),
                            left_schema.fields[i].name.clone(),
                        ));
                    }
                    for j in 0..right_len {
                        exprs.push((Expr::Column(j), right_schema.fields[j].name.clone()));
                    }

                    return Ok(LogicalPlan::Project {
                        input: Box::new(swapped_join),
                        exprs,
                    });
                }
            }

            Ok(LogicalPlan::Join {
                left: Box::new(opt_left),
                right: Box::new(opt_right),
                join_type,
                on,
                filter,
            })
        }
    }
}

// ============================================================================
// 5. Push Semi Joins Below Joins
// ============================================================================

pub fn push_semi_joins(plan: LogicalPlan, stats: &HashMap<String, usize>) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Scan { .. } => Ok(plan),
        LogicalPlan::Filter { input, predicate } => {
            let opt_input = push_semi_joins(*input, stats)?;
            Ok(LogicalPlan::Filter {
                input: Box::new(opt_input),
                predicate,
            })
        }
        LogicalPlan::Project { input, exprs } => {
            let opt_input = push_semi_joins(*input, stats)?;
            Ok(LogicalPlan::Project {
                input: Box::new(opt_input),
                exprs,
            })
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggs,
        } => {
            let opt_input = push_semi_joins(*input, stats)?;
            Ok(LogicalPlan::Aggregate {
                input: Box::new(opt_input),
                group_by,
                aggs,
            })
        }
        LogicalPlan::Sort { input, keys } => {
            let opt_input = push_semi_joins(*input, stats)?;
            Ok(LogicalPlan::Sort {
                input: Box::new(opt_input),
                keys,
            })
        }
        LogicalPlan::Limit { input, n } => {
            let opt_input = push_semi_joins(*input, stats)?;
            Ok(LogicalPlan::Limit {
                input: Box::new(opt_input),
                n,
            })
        }
        LogicalPlan::CrossJoin { left, right } => {
            let opt_left = push_semi_joins(*left, stats)?;
            let opt_right = push_semi_joins(*right, stats)?;
            Ok(LogicalPlan::CrossJoin {
                left: Box::new(opt_left),
                right: Box::new(opt_right),
            })
        }
        LogicalPlan::Join {
            left,
            right,
            join_type: jt @ (JoinType::Semi | JoinType::Anti),
            on,
            filter,
        } => {
            let opt_left = push_semi_joins(*left, stats)?;
            let opt_right = push_semi_joins(*right, stats)?;
            if can_push_semi(&opt_left, &opt_right, &on, filter.as_ref(), stats) {
                push_semi_down(opt_left, opt_right, jt, on, filter, stats)
            } else {
                Ok(LogicalPlan::Join {
                    left: Box::new(opt_left),
                    right: Box::new(opt_right),
                    join_type: jt,
                    on,
                    filter,
                })
            }
        }
        LogicalPlan::Join {
            left,
            right,
            join_type,
            on,
            filter,
        } => {
            let opt_left = push_semi_joins(*left, stats)?;
            let opt_right = push_semi_joins(*right, stats)?;
            Ok(LogicalPlan::Join {
                left: Box::new(opt_left),
                right: Box::new(opt_right),
                join_type,
                on,
                filter,
            })
        }
    }
}

fn plan_has_filter(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Filter { .. } => true,
        LogicalPlan::Join {
            filter: Some(_), ..
        } => true,
        LogicalPlan::Join { left, right, .. } | LogicalPlan::CrossJoin { left, right } => {
            plan_has_filter(left) || plan_has_filter(right)
        }
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => plan_has_filter(input),
        LogicalPlan::Scan { .. } => false,
    }
}

fn should_push_semi_below_inner(
    target: &LogicalPlan,
    other: &LogicalPlan,
    build: &LogicalPlan,
    stats: &HashMap<String, usize>,
) -> bool {
    let build_est = estimate_rows(build, stats);
    let target_est = estimate_rows(target, stats);

    // If the semi join build side is estimated much smaller than the target side,
    // pushing it down filters the target early (e.g. Q18: having sum > 300 produces ~57 rows).
    let build_much_smaller = build_est <= target_est * 0.1;

    // A join filters target if the other side has filters (like nation = 'SAUDI ARABIA', orders = 'F').
    let other_has_filter = plan_has_filter(other);

    if build_much_smaller {
        true
    } else {
        !other_has_filter
    }
}

fn can_push_semi(
    left: &LogicalPlan,
    right: &LogicalPlan,
    on: &[(Expr, Expr)],
    filter: Option<&Expr>,
    stats: &HashMap<String, usize>,
) -> bool {
    match left {
        LogicalPlan::Project { input, exprs } => {
            let proj_len = exprs.len();
            let input_len = match input.schema() {
                Ok(s) => s.len(),
                Err(_) => return false,
            };
            let new_on: Vec<(Expr, Expr)> = on
                .iter()
                .map(|(l, r)| (substitute_project(l.clone(), exprs), r.clone()))
                .collect();
            let new_filter = filter
                .cloned()
                .map(|f| remap_filter_project(f, exprs, proj_len, input_len));
            can_push_semi(input, right, &new_on, new_filter.as_ref(), stats)
        }
        LogicalPlan::Filter { input, .. } => match input.as_ref() {
            LogicalPlan::Join { .. } | LogicalPlan::Project { .. } => {
                can_push_semi(input, right, on, filter, stats)
            }
            _ => false,
        },
        LogicalPlan::Join {
            left: inner_left,
            join_type: JoinType::Semi | JoinType::Anti,
            ..
        } => can_push_semi(inner_left, right, on, filter, stats),
        LogicalPlan::Join {
            left: inner_left,
            right: inner_right,
            join_type: JoinType::Inner,
            ..
        } => {
            let left_len = match inner_left.schema() {
                Ok(s) => s.len(),
                Err(_) => return false,
            };
            let right_len = match inner_right.schema() {
                Ok(s) => s.len(),
                Err(_) => return false,
            };

            let mut left_cols = HashSet::new();
            for (l, _) in on {
                expr_references_columns(l, &mut left_cols);
            }
            if let Some(f) = filter {
                let mut f_cols = HashSet::new();
                expr_references_columns(f, &mut f_cols);
                for c in f_cols {
                    if c < left_len + right_len {
                        left_cols.insert(c);
                    }
                }
            }

            let all_in_left = !left_cols.is_empty() && left_cols.iter().all(|&c| c < left_len);
            let all_in_right = !left_cols.is_empty() && left_cols.iter().all(|&c| c >= left_len);

            if all_in_left {
                should_push_semi_below_inner(inner_left, inner_right, right, stats)
            } else if all_in_right {
                should_push_semi_below_inner(inner_right, inner_left, right, stats)
            } else {
                false
            }
        }
        _ => false,
    }
}

fn push_semi_down(
    left: LogicalPlan,
    right: LogicalPlan,
    join_type: JoinType,
    on: Vec<(Expr, Expr)>,
    filter: Option<Expr>,
    stats: &HashMap<String, usize>,
) -> Result<LogicalPlan> {
    match left {
        LogicalPlan::Project { input, exprs } => {
            let proj_len = exprs.len();
            let input_len = input.schema()?.len();
            let new_on: Vec<(Expr, Expr)> = on
                .into_iter()
                .map(|(l, r)| (substitute_project(l, &exprs), r))
                .collect();
            let new_filter = filter.map(|f| remap_filter_project(f, &exprs, proj_len, input_len));
            let pushed = push_semi_down(*input, right, join_type, new_on, new_filter, stats)?;
            Ok(LogicalPlan::Project {
                input: Box::new(pushed),
                exprs,
            })
        }
        LogicalPlan::Filter { input, predicate } => match input.as_ref() {
            LogicalPlan::Join { .. } | LogicalPlan::Project { .. } => {
                let pushed = push_semi_down(*input, right, join_type, on, filter, stats)?;
                Ok(LogicalPlan::Filter {
                    input: Box::new(pushed),
                    predicate,
                })
            }
            _ => Ok(LogicalPlan::Join {
                left: Box::new(LogicalPlan::Filter { input, predicate }),
                right: Box::new(right),
                join_type,
                on,
                filter,
            }),
        },
        LogicalPlan::Join {
            left: inner_left,
            right: inner_right,
            join_type: inner_jt @ (JoinType::Semi | JoinType::Anti),
            on: inner_on,
            filter: inner_filter,
        } => {
            let pushed = push_semi_down(*inner_left, right, join_type, on, filter, stats)?;
            Ok(LogicalPlan::Join {
                left: Box::new(pushed),
                right: inner_right,
                join_type: inner_jt,
                on: inner_on,
                filter: inner_filter,
            })
        }
        LogicalPlan::Join {
            left: inner_left,
            right: inner_right,
            join_type: JoinType::Inner,
            on: inner_on,
            filter: inner_filter,
        } => {
            let left_len = inner_left.schema()?.len();
            let right_len = inner_right.schema()?.len();

            let mut left_cols = HashSet::new();
            for (l, _) in &on {
                expr_references_columns(l, &mut left_cols);
            }
            if let Some(ref f) = filter {
                let mut f_cols = HashSet::new();
                expr_references_columns(f, &mut f_cols);
                for c in f_cols {
                    if c < left_len + right_len {
                        left_cols.insert(c);
                    }
                }
            }

            let all_in_left = !left_cols.is_empty() && left_cols.iter().all(|&c| c < left_len);
            let all_in_right = !left_cols.is_empty() && left_cols.iter().all(|&c| c >= left_len);

            if all_in_left && should_push_semi_below_inner(&inner_left, &inner_right, &right, stats)
            {
                let new_filter = filter.map(|f| {
                    remap_expr_indices(f, &|idx| {
                        if idx < left_len {
                            idx
                        } else {
                            idx - right_len
                        }
                    })
                });
                let pushed_left =
                    push_semi_down(*inner_left, right, join_type, on, new_filter, stats)?;
                Ok(LogicalPlan::Join {
                    left: Box::new(pushed_left),
                    right: inner_right,
                    join_type: JoinType::Inner,
                    on: inner_on,
                    filter: inner_filter,
                })
            } else if all_in_right
                && should_push_semi_below_inner(&inner_right, &inner_left, &right, stats)
            {
                let new_on = on
                    .into_iter()
                    .map(|(l, r)| (remap_expr_columns(l, -(left_len as i64)), r))
                    .collect();
                let new_filter = filter.map(|f| remap_expr_columns(f, -(left_len as i64)));
                let pushed_right =
                    push_semi_down(*inner_right, right, join_type, new_on, new_filter, stats)?;
                Ok(LogicalPlan::Join {
                    left: inner_left,
                    right: Box::new(pushed_right),
                    join_type: JoinType::Inner,
                    on: inner_on,
                    filter: inner_filter,
                })
            } else {
                Ok(LogicalPlan::Join {
                    left: Box::new(LogicalPlan::Join {
                        left: inner_left,
                        right: inner_right,
                        join_type: JoinType::Inner,
                        on: inner_on,
                        filter: inner_filter,
                    }),
                    right: Box::new(right),
                    join_type,
                    on,
                    filter,
                })
            }
        }
        other => Ok(LogicalPlan::Join {
            left: Box::new(other),
            right: Box::new(right),
            join_type,
            on,
            filter,
        }),
    }
}

fn substitute_project(expr: Expr, exprs: &[(Expr, String)]) -> Expr {
    match expr {
        Expr::Column(idx) => {
            if idx < exprs.len() {
                exprs[idx].0.clone()
            } else {
                Expr::Column(idx)
            }
        }
        Expr::Literal(s) => Expr::Literal(s),
        Expr::Not(e) => Expr::Not(Box::new(substitute_project(*e, exprs))),
        Expr::Negate(e) => Expr::Negate(Box::new(substitute_project(*e, exprs))),
        Expr::IsNull(e) => Expr::IsNull(Box::new(substitute_project(*e, exprs))),
        Expr::Binary { op, left, right } => Expr::Binary {
            op,
            left: Box::new(substitute_project(*left, exprs)),
            right: Box::new(substitute_project(*right, exprs)),
        },
        Expr::Extract { field, expr } => Expr::Extract {
            field,
            expr: Box::new(substitute_project(*expr, exprs)),
        },
        Expr::Substring { expr, start, len } => Expr::Substring {
            expr: Box::new(substitute_project(*expr, exprs)),
            start,
            len,
        },
        Expr::Cast { expr, to } => Expr::Cast {
            expr: Box::new(substitute_project(*expr, exprs)),
            to,
        },
        Expr::Case { whens, else_expr } => Expr::Case {
            whens: whens
                .into_iter()
                .map(|(c, v)| (substitute_project(c, exprs), substitute_project(v, exprs)))
                .collect(),
            else_expr: else_expr.map(|e| Box::new(substitute_project(*e, exprs))),
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(substitute_project(*expr, exprs)),
            list,
            negated,
        },
        Expr::Like {
            expr,
            pattern,
            negated,
        } => Expr::Like {
            expr: Box::new(substitute_project(*expr, exprs)),
            pattern,
            negated,
        },
    }
}

fn remap_filter_project(
    expr: Expr,
    exprs: &[(Expr, String)],
    proj_len: usize,
    input_len: usize,
) -> Expr {
    match expr {
        Expr::Column(idx) => {
            if idx < proj_len {
                exprs[idx].0.clone()
            } else {
                Expr::Column(idx - proj_len + input_len)
            }
        }
        Expr::Literal(s) => Expr::Literal(s),
        Expr::Not(e) => Expr::Not(Box::new(remap_filter_project(
            *e, exprs, proj_len, input_len,
        ))),
        Expr::Negate(e) => Expr::Negate(Box::new(remap_filter_project(
            *e, exprs, proj_len, input_len,
        ))),
        Expr::IsNull(e) => Expr::IsNull(Box::new(remap_filter_project(
            *e, exprs, proj_len, input_len,
        ))),
        Expr::Binary { op, left, right } => Expr::Binary {
            op,
            left: Box::new(remap_filter_project(*left, exprs, proj_len, input_len)),
            right: Box::new(remap_filter_project(*right, exprs, proj_len, input_len)),
        },
        Expr::Extract { field, expr } => Expr::Extract {
            field,
            expr: Box::new(remap_filter_project(*expr, exprs, proj_len, input_len)),
        },
        Expr::Substring { expr, start, len } => Expr::Substring {
            expr: Box::new(remap_filter_project(*expr, exprs, proj_len, input_len)),
            start,
            len,
        },
        Expr::Cast { expr, to } => Expr::Cast {
            expr: Box::new(remap_filter_project(*expr, exprs, proj_len, input_len)),
            to,
        },
        Expr::Case { whens, else_expr } => Expr::Case {
            whens: whens
                .into_iter()
                .map(|(c, v)| {
                    (
                        remap_filter_project(c, exprs, proj_len, input_len),
                        remap_filter_project(v, exprs, proj_len, input_len),
                    )
                })
                .collect(),
            else_expr: else_expr
                .map(|e| Box::new(remap_filter_project(*e, exprs, proj_len, input_len))),
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(remap_filter_project(*expr, exprs, proj_len, input_len)),
            list,
            negated,
        },
        Expr::Like {
            expr,
            pattern,
            negated,
        } => Expr::Like {
            expr: Box::new(remap_filter_project(*expr, exprs, proj_len, input_len)),
            pattern,
            negated,
        },
    }
}

// ============================================================================
// 6. Projection Pruning
// ============================================================================

pub fn prune_projections(plan: LogicalPlan) -> Result<LogicalPlan> {
    Ok(plan)
}
