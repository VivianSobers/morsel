use std::collections::HashMap;

use anyhow::{anyhow, bail, Result};
use morsel_core::Schema;
use morsel_plan::{BinOp, Expr, JoinType, LogicalPlan, SchemaProvider};
use sqlparser::ast as sql;

use crate::bind::{bind_query, coerce_for_cmp, map_bin_op, ExprBinderContext, QueryScope};

pub fn split_conjuncts(expr: &sql::Expr) -> Vec<sql::Expr> {
    let mut conjuncts = Vec::new();
    fn recurse(e: &sql::Expr, list: &mut Vec<sql::Expr>) {
        if let sql::Expr::BinaryOp {
            left,
            op: sql::BinaryOperator::And,
            right,
        } = e
        {
            recurse(left, list);
            recurse(right, list);
        } else if let sql::Expr::Nested(inner) = e {
            recurse(inner, list);
        } else {
            list.push(e.clone());
        }
    }
    recurse(expr, &mut conjuncts);
    conjuncts
}

pub fn combine_conjuncts(conjuncts: Vec<sql::Expr>) -> Option<sql::Expr> {
    if conjuncts.is_empty() {
        return None;
    }
    let mut iter = conjuncts.into_iter();
    let mut root = iter.next().unwrap();
    for next in iter {
        root = sql::Expr::BinaryOp {
            left: Box::new(root),
            op: sql::BinaryOperator::And,
            right: Box::new(next),
        };
    }
    Some(root)
}

pub fn contains_subquery(expr: &sql::Expr) -> bool {
    match expr {
        sql::Expr::Exists { .. } | sql::Expr::InSubquery { .. } | sql::Expr::Subquery(_) => true,
        sql::Expr::BinaryOp { left, right, .. } => {
            contains_subquery(left) || contains_subquery(right)
        }
        sql::Expr::UnaryOp { expr, .. } => contains_subquery(expr),
        sql::Expr::Nested(inner) => contains_subquery(inner),
        _ => false,
    }
}

#[derive(Clone, Debug)]
pub(crate) struct InnerTableInfo {
    pub alias: String,
    pub real_table: Option<String>,
}

pub(crate) fn get_subquery_tables(query: &sql::Query) -> Vec<InnerTableInfo> {
    let mut tables = Vec::new();
    if let sql::SetExpr::Select(select) = &*query.body {
        for twj in &select.from {
            add_factor_tables(&twj.relation, &mut tables);
            for join in &twj.joins {
                add_factor_tables(&join.relation, &mut tables);
            }
        }
    }
    tables
}

fn add_factor_tables(factor: &sql::TableFactor, tables: &mut Vec<InnerTableInfo>) {
    match factor {
        sql::TableFactor::Table { name, alias, .. } => {
            let real = name.to_string();
            let alias_str = alias
                .as_ref()
                .map(|a| a.name.value.clone())
                .unwrap_or_else(|| real.clone());
            tables.push(InnerTableInfo {
                alias: alias_str.to_lowercase(),
                real_table: Some(real),
            });
        }
        sql::TableFactor::Derived { alias: Some(a), .. } => {
            tables.push(InnerTableInfo {
                alias: a.name.value.to_lowercase(),
                real_table: None,
            });
        }
        sql::TableFactor::Derived { alias: None, .. } => {}
        sql::TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            add_factor_tables(&table_with_joins.relation, tables);
            for join in &table_with_joins.joins {
                add_factor_tables(&join.relation, tables);
            }
        }
        _ => {}
    }
}

pub(crate) fn contains_outer_ref(
    expr: &sql::Expr,
    inner_tables: &[InnerTableInfo],
    outer_scope: &QueryScope,
    schemas: &dyn SchemaProvider,
) -> bool {
    match expr {
        sql::Expr::CompoundIdentifier(parts) => {
            if parts.len() == 2 {
                let tbl = parts[0].value.to_lowercase();
                if inner_tables.iter().any(|t| t.alias == tbl) {
                    return false;
                }
                for t in &outer_scope.tables {
                    if t.alias.eq_ignore_ascii_case(&tbl)
                        || t.table_name
                            .as_deref()
                            .is_some_and(|tn| tn.eq_ignore_ascii_case(&tbl))
                    {
                        return true;
                    }
                }
            }
            false
        }
        sql::Expr::Identifier(ident) => {
            let col = &ident.value;
            let found_in_inner = inner_tables.iter().any(|t| {
                if let Some(real) = &t.real_table {
                    if let Some(schema) = schemas.table_schema(real) {
                        return schema.index_of(col).is_some();
                    }
                }
                false
            });
            if found_in_inner {
                return false;
            }
            let found_in_outer = outer_scope
                .tables
                .iter()
                .any(|t| t.full_schema.index_of(col).is_some());
            found_in_outer
        }
        sql::Expr::BinaryOp { left, right, .. } => {
            contains_outer_ref(left, inner_tables, outer_scope, schemas)
                || contains_outer_ref(right, inner_tables, outer_scope, schemas)
        }
        sql::Expr::UnaryOp { expr, .. } => {
            contains_outer_ref(expr, inner_tables, outer_scope, schemas)
        }
        sql::Expr::Nested(inner) => contains_outer_ref(inner, inner_tables, outer_scope, schemas),
        _ => false,
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn decorrelate_where(
    selection: &sql::Expr,
    mut current_plan: LogicalPlan,
    outer_scope: &QueryScope,
    table_offsets: &[usize],
    col_maps: &[HashMap<usize, usize>],
    from_schema: &Schema,
    schemas: &dyn SchemaProvider,
    ctes: &HashMap<String, sql::Query>,
) -> Result<(LogicalPlan, Option<Expr>)> {
    let conjuncts = split_conjuncts(selection);
    let mut regular_conjuncts = Vec::new();

    let binder_ctx = ExprBinderContext {
        scope: outer_scope,
        table_offsets,
        col_maps,
        from_schema,
    };

    for conjunct in conjuncts {
        if !contains_subquery(&conjunct) {
            let bound = binder_ctx.bind_expr(&conjunct)?;
            regular_conjuncts.push(bound);
            continue;
        }

        match conjunct {
            sql::Expr::Exists { subquery, negated } => {
                let inner_tables = get_subquery_tables(&subquery);
                let sub_select = match &*subquery.body {
                    sql::SetExpr::Select(s) => s,
                    _ => bail!("unsupported subquery in EXISTS"),
                };

                let sub_conjuncts = if let Some(sub_sel) = &sub_select.selection {
                    split_conjuncts(sub_sel)
                } else {
                    Vec::new()
                };

                let mut equi_on = Vec::new();
                let mut inner_filter_conjuncts = Vec::new();
                let mut residual_filters = Vec::new();

                for sc in sub_conjuncts {
                    if let sql::Expr::BinaryOp { left, op, right } = &sc {
                        let left_outer =
                            contains_outer_ref(left, &inner_tables, outer_scope, schemas);
                        let right_outer =
                            contains_outer_ref(right, &inner_tables, outer_scope, schemas);

                        if left_outer && !right_outer {
                            if *op == sql::BinaryOperator::Eq {
                                equi_on.push((*left.clone(), *right.clone()));
                            } else {
                                residual_filters.push(sc);
                            }
                            continue;
                        } else if !left_outer && right_outer {
                            if *op == sql::BinaryOperator::Eq {
                                equi_on.push((*right.clone(), *left.clone()));
                            } else {
                                residual_filters.push(sc);
                            }
                            continue;
                        }
                    }

                    if contains_outer_ref(&sc, &inner_tables, outer_scope, schemas) {
                        residual_filters.push(sc);
                    } else {
                        inner_filter_conjuncts.push(sc);
                    }
                }

                let mut modified_subquery = (*subquery).clone();
                if let sql::SetExpr::Select(ref mut s) = *modified_subquery.body {
                    s.selection = combine_conjuncts(inner_filter_conjuncts);
                }

                let sub_plan = bind_query(&modified_subquery, schemas, ctes)?;
                let sub_schema = sub_plan.schema()?;
                let outer_schema = current_plan.schema()?;

                let mut bound_on = Vec::new();
                for (outer_expr, inner_expr) in equi_on {
                    let l_bound = binder_ctx.bind_expr(&outer_expr)?;
                    let r_idx = match &inner_expr {
                        sql::Expr::Identifier(ident) => {
                            sub_schema.index_of(&ident.value).ok_or_else(|| {
                                anyhow!("column not found in subquery: {}", ident.value)
                            })?
                        }
                        sql::Expr::CompoundIdentifier(parts) => {
                            sub_schema.index_of(&parts[1].value).ok_or_else(|| {
                                anyhow!("column not found in subquery: {}", parts[1].value)
                            })?
                        }
                        _ => bail!("unsupported inner expression in join: {:?}", inner_expr),
                    };
                    bound_on.push((l_bound, Expr::Column(r_idx)));
                }

                let bound_residual = if residual_filters.is_empty() {
                    None
                } else {
                    let mut res_exprs = Vec::new();
                    for rf in residual_filters {
                        if let sql::Expr::BinaryOp { left, op, right } = rf {
                            let left_is_outer =
                                contains_outer_ref(&left, &inner_tables, outer_scope, schemas);
                            let (out_e, in_e) = if left_is_outer {
                                (*left, *right)
                            } else {
                                (*right, *left)
                            };

                            let l_bound = binder_ctx.bind_expr(&out_e)?;
                            let r_idx = match in_e {
                                sql::Expr::Identifier(ident) => {
                                    sub_schema.index_of(&ident.value).ok_or_else(|| {
                                        anyhow!("column not found in subquery: {}", ident.value)
                                    })?
                                }
                                sql::Expr::CompoundIdentifier(parts) => {
                                    sub_schema.index_of(&parts[1].value).ok_or_else(|| {
                                        anyhow!("column not found in subquery: {}", parts[1].value)
                                    })?
                                }
                                _ => bail!("unsupported inner residual: {:?}", in_e),
                            };
                            let r_col_in_comb = outer_schema.len() + r_idx;
                            let bin_op = map_bin_op(&op)?;
                            let (c_left, c_right) = if left_is_outer {
                                (l_bound, Expr::Column(r_col_in_comb))
                            } else {
                                (Expr::Column(r_col_in_comb), l_bound)
                            };
                            res_exprs.push(Expr::Binary {
                                op: bin_op,
                                left: Box::new(c_left),
                                right: Box::new(c_right),
                            });
                        }
                    }
                    if res_exprs.is_empty() {
                        None
                    } else {
                        let mut r = res_exprs.remove(0);
                        for next in res_exprs {
                            r = Expr::Binary {
                                op: BinOp::And,
                                left: Box::new(r),
                                right: Box::new(next),
                            };
                        }
                        Some(r)
                    }
                };

                let join_type = if negated {
                    JoinType::Anti
                } else {
                    JoinType::Semi
                };
                current_plan = LogicalPlan::Join {
                    left: Box::new(current_plan),
                    right: Box::new(sub_plan),
                    join_type,
                    on: bound_on,
                    filter: bound_residual,
                };
            }

            sql::Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let sub_plan = bind_query(&subquery, schemas, ctes)?;
                let bound_left = binder_ctx.bind_expr(&expr)?;
                let join_type = if negated {
                    JoinType::Anti
                } else {
                    JoinType::Semi
                };

                current_plan = LogicalPlan::Join {
                    left: Box::new(current_plan),
                    right: Box::new(sub_plan),
                    join_type,
                    on: vec![(bound_left, Expr::Column(0))],
                    filter: None,
                };
            }

            sql::Expr::BinaryOp {
                ref left,
                ref op,
                ref right,
            } => {
                let right_sub = match &**right {
                    sql::Expr::Subquery(sub) => Some((true, sub)),
                    _ => match &**left {
                        sql::Expr::Subquery(sub) => Some((false, sub)),
                        _ => None,
                    },
                };

                if let Some((sub_is_right, subquery)) = right_sub {
                    let other_expr: &sql::Expr = if sub_is_right { left } else { right };
                    let inner_tables = get_subquery_tables(subquery);
                    let sub_select = match &*subquery.body {
                        sql::SetExpr::Select(s) => s,
                        _ => bail!("unsupported subquery in scalar comparison"),
                    };

                    let sub_conjuncts = if let Some(sub_sel) = &sub_select.selection {
                        split_conjuncts(sub_sel)
                    } else {
                        Vec::new()
                    };

                    let mut equi_on = Vec::new();
                    let mut inner_filter_conjuncts = Vec::new();

                    for sc in sub_conjuncts {
                        if let sql::Expr::BinaryOp {
                            left,
                            op: sql::BinaryOperator::Eq,
                            right,
                        } = &sc
                        {
                            let left_outer =
                                contains_outer_ref(left, &inner_tables, outer_scope, schemas);
                            let right_outer =
                                contains_outer_ref(right, &inner_tables, outer_scope, schemas);
                            if left_outer && !right_outer {
                                equi_on.push((*left.clone(), *right.clone()));
                                continue;
                            } else if !left_outer && right_outer {
                                equi_on.push((*right.clone(), *left.clone()));
                                continue;
                            }
                        }
                        if !contains_outer_ref(&sc, &inner_tables, outer_scope, schemas) {
                            inner_filter_conjuncts.push(sc);
                        }
                    }

                    if equi_on.is_empty() {
                        let sub_plan = bind_query(subquery, schemas, ctes)?;
                        let left_len = current_plan.schema()?.len();
                        current_plan = LogicalPlan::CrossJoin {
                            left: Box::new(current_plan),
                            right: Box::new(sub_plan),
                        };
                        let b_other = binder_ctx.bind_expr(other_expr)?;
                        let sub_col = Expr::Column(left_len);
                        let bin_op = map_bin_op(op)?;
                        let (final_l, final_r) = if sub_is_right {
                            (b_other, sub_col)
                        } else {
                            (sub_col, b_other)
                        };
                        let cur_schema = current_plan.schema()?;
                        let (final_l, final_r) = coerce_for_cmp(final_l, final_r, &cur_schema)?;
                        regular_conjuncts.push(Expr::Binary {
                            op: bin_op,
                            left: Box::new(final_l),
                            right: Box::new(final_r),
                        });
                    } else {
                        let mut modified_subquery = (*subquery).clone();

                        let inner_group_by_exprs: Vec<sql::Expr> =
                            equi_on.iter().map(|(_, inner)| inner.clone()).collect();
                        let mut proj_items: Vec<sql::SelectItem> = inner_group_by_exprs
                            .iter()
                            .map(|e| sql::SelectItem::UnnamedExpr(e.clone()))
                            .collect();

                        if let sql::SetExpr::Select(ref mut s) = *modified_subquery.body {
                            s.selection = combine_conjuncts(inner_filter_conjuncts);
                            s.group_by =
                                sql::GroupByExpr::Expressions(inner_group_by_exprs, vec![]);
                            let orig_scalar_expr = match &s.projection[0] {
                                sql::SelectItem::UnnamedExpr(e)
                                | sql::SelectItem::ExprWithAlias { expr: e, .. } => e.clone(),
                                other => bail!("unexpected subquery projection: {:?}", other),
                            };
                            proj_items.push(sql::SelectItem::ExprWithAlias {
                                expr: orig_scalar_expr,
                                alias: sql::Ident::new("agg_val"),
                            });
                            s.projection = proj_items;
                        }

                        let sub_plan = bind_query(&modified_subquery, schemas, ctes)?;
                        let sub_schema = sub_plan.schema()?;

                        let mut bound_on = Vec::with_capacity(equi_on.len());
                        for (i, (outer_key, inner_key)) in equi_on.iter().enumerate() {
                            let l_bound = binder_ctx.bind_expr(outer_key)?;
                            let r_idx = match inner_key {
                                sql::Expr::Identifier(ident) => {
                                    sub_schema.index_of(&ident.value).unwrap_or(i)
                                }
                                sql::Expr::CompoundIdentifier(parts) => parts
                                    .last()
                                    .and_then(|p| sub_schema.index_of(&p.value))
                                    .unwrap_or(i),
                                _ => i,
                            };
                            bound_on.push((l_bound, Expr::Column(r_idx)));
                        }

                        let left_len = current_plan.schema()?.len();
                        current_plan = LogicalPlan::Join {
                            left: Box::new(current_plan),
                            right: Box::new(sub_plan),
                            join_type: JoinType::Inner,
                            on: bound_on,
                            filter: None,
                        };

                        let b_other = binder_ctx.bind_expr(other_expr)?;
                        let agg_val_col = Expr::Column(left_len + equi_on.len());
                        let bin_op = map_bin_op(op)?;
                        let (final_l, final_r) = if sub_is_right {
                            (b_other, agg_val_col)
                        } else {
                            (agg_val_col, b_other)
                        };
                        let cur_schema = current_plan.schema()?;
                        let (final_l, final_r) = coerce_for_cmp(final_l, final_r, &cur_schema)?;
                        regular_conjuncts.push(Expr::Binary {
                            op: bin_op,
                            left: Box::new(final_l),
                            right: Box::new(final_r),
                        });
                    }
                } else {
                    let bound = binder_ctx.bind_expr(&conjunct)?;
                    regular_conjuncts.push(bound);
                }
            }

            _ => {
                let bound = binder_ctx.bind_expr(&conjunct)?;
                regular_conjuncts.push(bound);
            }
        }
    }

    let final_predicate = if regular_conjuncts.is_empty() {
        None
    } else {
        let mut p = regular_conjuncts.remove(0);
        for next in regular_conjuncts {
            p = Expr::Binary {
                op: BinOp::And,
                left: Box::new(p),
                right: Box::new(next),
            };
        }
        Some(p)
    };

    Ok((current_plan, final_predicate))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn decorrelate_having(
    having: &sql::Expr,
    mut cur_plan: LogicalPlan,
    group_exprs: &[(Expr, String)],
    aggs: &[(morsel_plan::AggFunc, Option<sql::Expr>, String)],
    binder_ctx: &crate::bind::ExprBinderContext,
    schemas: &dyn SchemaProvider,
    ctes: &HashMap<String, sql::Query>,
) -> Result<LogicalPlan> {
    let conjuncts = split_conjuncts(having);
    let mut bound_conjuncts = Vec::new();

    for conjunct in conjuncts {
        if let sql::Expr::BinaryOp { left, op, right } = &conjunct {
            let left_has_sub = contains_subquery(left);
            let right_has_sub = contains_subquery(right);

            if left_has_sub || right_has_sub {
                let (subquery_expr, other_expr, sub_is_right) = if right_has_sub {
                    (right.as_ref(), left.as_ref(), true)
                } else {
                    (left.as_ref(), right.as_ref(), false)
                };

                let subquery = match subquery_expr {
                    sql::Expr::Subquery(q) => q,
                    sql::Expr::Nested(inner) => match &**inner {
                        sql::Expr::Subquery(q) => q,
                        _ => bail!("unsupported subquery in HAVING: {:?}", subquery_expr),
                    },
                    _ => bail!("unsupported subquery in HAVING: {:?}", subquery_expr),
                };

                let sub_plan = bind_query(subquery, schemas, ctes)?;
                let left_len = cur_plan.schema()?.len();
                cur_plan = LogicalPlan::CrossJoin {
                    left: Box::new(cur_plan),
                    right: Box::new(sub_plan),
                };

                let cur_schema = cur_plan.schema()?;
                let b_other = crate::bind::rewrite_post_agg_expr(
                    other_expr,
                    group_exprs,
                    aggs,
                    binder_ctx,
                    &cur_schema,
                )?;
                let sub_col = Expr::Column(left_len);
                let bin_op = crate::bind::map_bin_op(op)?;
                let (final_l, final_r) = if sub_is_right {
                    (b_other, sub_col)
                } else {
                    (sub_col, b_other)
                };
                let (final_l, final_r) =
                    crate::bind::coerce_for_cmp(final_l, final_r, &cur_schema)?;
                bound_conjuncts.push(Expr::Binary {
                    op: bin_op,
                    left: Box::new(final_l),
                    right: Box::new(final_r),
                });
                continue;
            }
        }

        let cur_schema = cur_plan.schema()?;
        let bound = crate::bind::rewrite_post_agg_expr(
            &conjunct,
            group_exprs,
            aggs,
            binder_ctx,
            &cur_schema,
        )?;
        bound_conjuncts.push(bound);
    }

    if !bound_conjuncts.is_empty() {
        let mut iter = bound_conjuncts.into_iter();
        let mut root = iter.next().unwrap();
        for next in iter {
            root = Expr::Binary {
                op: BinOp::And,
                left: Box::new(root),
                right: Box::new(next),
            };
        }
        cur_plan = LogicalPlan::Filter {
            input: Box::new(cur_plan),
            predicate: root,
        };
    }

    Ok(cur_plan)
}
