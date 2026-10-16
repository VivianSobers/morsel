use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, bail, Result};
use chrono::{Duration, Months, NaiveDate};
use morsel_core::{DataType, Scalar, Schema};
use morsel_plan::{
    AggExpr, AggFunc, BinOp, DateField, Expr, JoinType, LogicalPlan, SchemaProvider, SortKey,
};
use sqlparser::ast as sql;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub fn plan_sql(sql_str: &str, schemas: &dyn SchemaProvider) -> Result<LogicalPlan> {
    let dialect = GenericDialect {};
    let statements =
        Parser::parse_sql(&dialect, sql_str).map_err(|e| anyhow!("sql parser error: {}", e))?;

    if statements.is_empty() {
        bail!("empty sql query");
    }
    if statements.len() > 1 {
        bail!("multiple sql statements not supported");
    }

    match &statements[0] {
        sql::Statement::Query(query) => {
            let mut ctes = HashMap::new();
            if let Some(with) = &query.with {
                for cte in &with.cte_tables {
                    ctes.insert(cte.alias.name.value.to_lowercase(), (*cte.query).clone());
                }
            }
            bind_query(query, schemas, &ctes)
        }
        other => bail!("unsupported SQL statement: {:?}", other),
    }
}

pub(crate) fn bind_query(
    query: &sql::Query,
    schemas: &dyn SchemaProvider,
    ctes: &HashMap<String, sql::Query>,
) -> Result<LogicalPlan> {
    let body = match &*query.body {
        sql::SetExpr::Select(select) => select,
        sql::SetExpr::Query(inner_query) => return bind_query(inner_query, schemas, ctes),
        other => bail!("unsupported query body: {:?}", other),
    };

    bind_select(body, query, schemas, ctes)
}

pub(crate) struct TableScope {
    pub(crate) alias: String,
    pub(crate) table_name: Option<String>,
    pub(crate) full_schema: Schema,
    pub(crate) used_columns: HashSet<usize>,
    pub(crate) derived_plan: Option<LogicalPlan>,
}

pub(crate) struct QueryScope {
    pub(crate) tables: Vec<TableScope>,
}

impl QueryScope {
    fn resolve_column(&self, qualifier: Option<&str>, col_name: &str) -> Result<(usize, usize)> {
        if let Some(q) = qualifier {
            for (t_idx, t) in self.tables.iter().enumerate() {
                if t.alias.eq_ignore_ascii_case(q)
                    || t.table_name
                        .as_deref()
                        .is_some_and(|tn| tn.eq_ignore_ascii_case(q))
                {
                    if let Some(col_idx) = t.full_schema.index_of(col_name) {
                        return Ok((t_idx, col_idx));
                    }
                    bail!("column '{}' not found in table '{}'", col_name, q);
                }
            }
            bail!("table '{}' not found", q);
        } else {
            let mut matches = Vec::new();
            for (t_idx, t) in self.tables.iter().enumerate() {
                if let Some(col_idx) = t.full_schema.index_of(col_name) {
                    matches.push((t_idx, col_idx));
                }
            }
            match matches.len() {
                0 => bail!("column '{}' not found", col_name),
                1 => Ok(matches[0]),
                _ => bail!("column '{}' is ambiguous", col_name),
            }
        }
    }

    fn mark_column_used(&mut self, qualifier: Option<&str>, col_name: &str) -> Result<()> {
        let (t_idx, col_idx) = self.resolve_column(qualifier, col_name)?;
        self.tables[t_idx].used_columns.insert(col_idx);
        Ok(())
    }

    fn mark_all_columns_used(&mut self, qualifier: Option<&str>) -> Result<()> {
        if let Some(q) = qualifier {
            let mut found = false;
            for t in &mut self.tables {
                if t.alias.eq_ignore_ascii_case(q)
                    || t.table_name
                        .as_deref()
                        .is_some_and(|tn| tn.eq_ignore_ascii_case(q))
                {
                    for i in 0..t.full_schema.len() {
                        t.used_columns.insert(i);
                    }
                    found = true;
                    break;
                }
            }
            if !found {
                bail!("table '{}' not found", q);
            }
        } else {
            for t in &mut self.tables {
                for i in 0..t.full_schema.len() {
                    t.used_columns.insert(i);
                }
            }
        }
        Ok(())
    }
}

fn collect_used_columns_from_expr(
    expr: &sql::Expr,
    scope: &mut QueryScope,
    select_aliases: &HashMap<String, &sql::Expr>,
    schemas: &dyn SchemaProvider,
) -> Result<()> {
    match expr {
        sql::Expr::Identifier(ident) => {
            if let Some(alias_expr) = select_aliases.get(&ident.value.to_lowercase()) {
                collect_used_columns_from_expr(alias_expr, scope, select_aliases, schemas)?;
            } else {
                scope.mark_column_used(None, &ident.value)?;
            }
        }
        sql::Expr::CompoundIdentifier(parts) => {
            if parts.len() == 2 {
                scope.mark_column_used(Some(&parts[0].value), &parts[1].value)?;
            }
        }
        sql::Expr::BinaryOp { left, right, .. } => {
            collect_used_columns_from_expr(left, scope, select_aliases, schemas)?;
            collect_used_columns_from_expr(right, scope, select_aliases, schemas)?;
        }
        sql::Expr::UnaryOp { expr, .. } => {
            collect_used_columns_from_expr(expr, scope, select_aliases, schemas)?;
        }
        sql::Expr::Between {
            expr, low, high, ..
        } => {
            collect_used_columns_from_expr(expr, scope, select_aliases, schemas)?;
            collect_used_columns_from_expr(low, scope, select_aliases, schemas)?;
            collect_used_columns_from_expr(high, scope, select_aliases, schemas)?;
        }
        sql::Expr::IsNull(expr) | sql::Expr::IsNotNull(expr) => {
            collect_used_columns_from_expr(expr, scope, select_aliases, schemas)?;
        }
        sql::Expr::Like { expr, pattern, .. } => {
            collect_used_columns_from_expr(expr, scope, select_aliases, schemas)?;
            collect_used_columns_from_expr(pattern, scope, select_aliases, schemas)?;
        }
        sql::Expr::InList { expr, list, .. } => {
            collect_used_columns_from_expr(expr, scope, select_aliases, schemas)?;
            for item in list {
                collect_used_columns_from_expr(item, scope, select_aliases, schemas)?;
            }
        }
        sql::Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(op) = operand {
                collect_used_columns_from_expr(op, scope, select_aliases, schemas)?;
            }
            for cond in conditions {
                collect_used_columns_from_expr(cond, scope, select_aliases, schemas)?;
            }
            for res in results {
                collect_used_columns_from_expr(res, scope, select_aliases, schemas)?;
            }
            if let Some(el) = else_result {
                collect_used_columns_from_expr(el, scope, select_aliases, schemas)?;
            }
        }
        sql::Expr::Cast { expr, .. } => {
            collect_used_columns_from_expr(expr, scope, select_aliases, schemas)?;
        }
        sql::Expr::Extract { expr, .. } => {
            collect_used_columns_from_expr(expr, scope, select_aliases, schemas)?;
        }
        sql::Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            collect_used_columns_from_expr(expr, scope, select_aliases, schemas)?;
            if let Some(f) = substring_from {
                collect_used_columns_from_expr(f, scope, select_aliases, schemas)?;
            }
            if let Some(f) = substring_for {
                collect_used_columns_from_expr(f, scope, select_aliases, schemas)?;
            }
        }
        sql::Expr::Function(func) => {
            let (args, _) = get_function_args(func);
            for arg in args {
                if let sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(e)) = arg {
                    collect_used_columns_from_expr(e, scope, select_aliases, schemas)?;
                }
            }
        }
        sql::Expr::Nested(inner) => {
            collect_used_columns_from_expr(inner, scope, select_aliases, schemas)?;
        }
        sql::Expr::Exists { subquery, .. } => {
            collect_outer_columns_from_query(subquery, scope, schemas)?;
        }
        sql::Expr::InSubquery { expr, subquery, .. } => {
            collect_used_columns_from_expr(expr, scope, select_aliases, schemas)?;
            collect_outer_columns_from_query(subquery, scope, schemas)?;
        }
        sql::Expr::Subquery(subquery) => {
            collect_outer_columns_from_query(subquery, scope, schemas)?;
        }
        _ => {}
    }
    Ok(())
}

fn collect_outer_columns_from_query(
    query: &sql::Query,
    scope: &mut QueryScope,
    schemas: &dyn SchemaProvider,
) -> Result<()> {
    let inner_tables = crate::subquery::get_subquery_tables(query);
    if let sql::SetExpr::Select(select) = &*query.body {
        if let Some(selection) = &select.selection {
            collect_outer_from_expr(selection, &inner_tables, scope, schemas)?;
        }
    }
    Ok(())
}

fn collect_outer_from_expr(
    expr: &sql::Expr,
    inner_tables: &[crate::subquery::InnerTableInfo],
    scope: &mut QueryScope,
    schemas: &dyn SchemaProvider,
) -> Result<()> {
    match expr {
        sql::Expr::CompoundIdentifier(parts) => {
            if parts.len() == 2 {
                let tbl = parts[0].value.to_lowercase();
                if !inner_tables.iter().any(|t| t.alias == tbl) {
                    scope
                        .mark_column_used(Some(&parts[0].value), &parts[1].value)
                        .ok();
                }
            }
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
            if !found_in_inner {
                let found_in_outer = scope
                    .tables
                    .iter()
                    .any(|t| t.full_schema.index_of(col).is_some());
                if found_in_outer {
                    scope.mark_column_used(None, col).ok();
                }
            }
        }
        sql::Expr::BinaryOp { left, right, .. } => {
            collect_outer_from_expr(left, inner_tables, scope, schemas)?;
            collect_outer_from_expr(right, inner_tables, scope, schemas)?;
        }
        sql::Expr::UnaryOp { expr, .. } => {
            collect_outer_from_expr(expr, inner_tables, scope, schemas)?;
        }
        sql::Expr::Nested(inner) => {
            collect_outer_from_expr(inner, inner_tables, scope, schemas)?;
        }
        _ => {}
    }
    Ok(())
}

fn bind_select(
    select: &sql::Select,
    query: &sql::Query,
    schemas: &dyn SchemaProvider,
    ctes: &HashMap<String, sql::Query>,
) -> Result<LogicalPlan> {
    let mut scope = QueryScope { tables: Vec::new() };

    for table_with_joins in &select.from {
        bind_table_factor(&table_with_joins.relation, &mut scope, schemas, ctes)?;
        for join in &table_with_joins.joins {
            bind_table_factor(&join.relation, &mut scope, schemas, ctes)?;
        }
    }

    let mut select_aliases = HashMap::new();
    for item in &select.projection {
        if let sql::SelectItem::ExprWithAlias { expr, alias } = item {
            select_aliases.insert(alias.value.to_lowercase(), expr);
        }
    }

    for item in &select.projection {
        match item {
            sql::SelectItem::UnnamedExpr(expr) | sql::SelectItem::ExprWithAlias { expr, .. } => {
                collect_used_columns_from_expr(expr, &mut scope, &select_aliases, schemas)?;
            }
            sql::SelectItem::Wildcard(_) => {
                scope.mark_all_columns_used(None)?;
            }
            sql::SelectItem::QualifiedWildcard(obj_name, _) => {
                let name = obj_name.to_string();
                scope.mark_all_columns_used(Some(&name))?;
            }
        }
    }

    if let Some(selection) = &select.selection {
        collect_used_columns_from_expr(selection, &mut scope, &select_aliases, schemas)?;
    }

    match &select.group_by {
        sql::GroupByExpr::Expressions(exprs, _) => {
            for expr in exprs {
                collect_used_columns_from_expr(expr, &mut scope, &select_aliases, schemas)?;
            }
        }
        sql::GroupByExpr::All(_) => {}
    }

    if let Some(having) = &select.having {
        collect_used_columns_from_expr(having, &mut scope, &select_aliases, schemas)?;
    }

    if let Some(order_by) = &query.order_by {
        for order_expr in &order_by.exprs {
            collect_used_columns_from_expr(&order_expr.expr, &mut scope, &select_aliases, schemas)?;
        }
    }

    for table_with_joins in &select.from {
        for join in &table_with_joins.joins {
            match &join.join_operator {
                sql::JoinOperator::Inner(sql::JoinConstraint::On(expr))
                | sql::JoinOperator::LeftOuter(sql::JoinConstraint::On(expr)) => {
                    collect_used_columns_from_expr(expr, &mut scope, &select_aliases, schemas)?;
                }
                _ => {}
            }
        }
    }

    let mut table_plans = Vec::new();
    let mut table_offsets = Vec::new();
    let mut col_maps = Vec::new();
    let mut current_offset = 0;
    let mut from_schema_fields = Vec::new();

    for table in &mut scope.tables {
        table_offsets.push(current_offset);
        let mut col_map = HashMap::new();

        if let Some(derived) = table.derived_plan.take() {
            let schema = derived.schema()?;
            for i in 0..schema.len() {
                col_map.insert(i, i);
            }
            current_offset += schema.len();
            from_schema_fields.extend(schema.fields);
            table_plans.push(derived);
        } else {
            let mut used: Vec<usize> = table.used_columns.iter().copied().collect();
            used.sort_unstable();
            if used.is_empty() {
                used.push(0);
            }
            for (new_idx, &orig_idx) in used.iter().enumerate() {
                col_map.insert(orig_idx, new_idx);
            }

            let scan = LogicalPlan::Scan {
                table: table.table_name.clone().unwrap(),
                columns: used.clone(),
                table_schema: table.full_schema.clone(),
            };
            let scan_schema = scan.schema()?;
            current_offset += scan_schema.len();
            from_schema_fields.extend(scan_schema.fields);
            table_plans.push(scan);
        }
        col_maps.push(col_map);
    }

    let from_schema = Schema {
        fields: from_schema_fields,
    };

    let binder_ctx = ExprBinderContext {
        scope: &scope,
        table_offsets: &table_offsets,
        col_maps: &col_maps,
        from_schema: &from_schema,
    };

    let mut plan = if table_plans.is_empty() {
        LogicalPlan::Scan {
            table: String::new(),
            columns: vec![],
            table_schema: Schema::empty(),
        }
    } else {
        let mut t_idx = 0;
        let mut current_plan = table_plans[0].clone();
        t_idx += 1;

        for table_with_joins in &select.from {
            for join in &table_with_joins.joins {
                let right_plan = table_plans[t_idx].clone();
                let right_offset = table_offsets[t_idx];
                t_idx += 1;

                match &join.join_operator {
                    sql::JoinOperator::Inner(sql::JoinConstraint::On(on_expr)) => {
                        let bound_on = binder_ctx.bind_expr(on_expr)?;
                        let (on_keys, filter) = split_join_predicate(&bound_on, right_offset)?;
                        current_plan = LogicalPlan::Join {
                            left: Box::new(current_plan),
                            right: Box::new(right_plan),
                            join_type: JoinType::Inner,
                            on: on_keys,
                            filter,
                        };
                    }
                    sql::JoinOperator::LeftOuter(sql::JoinConstraint::On(on_expr)) => {
                        let bound_on = binder_ctx.bind_expr(on_expr)?;
                        let (on_keys, filter) = split_join_predicate(&bound_on, right_offset)?;
                        current_plan = LogicalPlan::Join {
                            left: Box::new(current_plan),
                            right: Box::new(right_plan),
                            join_type: JoinType::Left,
                            on: on_keys,
                            filter,
                        };
                    }
                    sql::JoinOperator::CrossJoin => {
                        current_plan = LogicalPlan::CrossJoin {
                            left: Box::new(current_plan),
                            right: Box::new(right_plan),
                        };
                    }
                    other => bail!("unsupported join operator: {:?}", other),
                }
            }
        }
        while t_idx < table_plans.len() {
            current_plan = LogicalPlan::CrossJoin {
                left: Box::new(current_plan),
                right: Box::new(table_plans[t_idx].clone()),
            };
            t_idx += 1;
        }
        current_plan
    };

    if let Some(selection) = &select.selection {
        let (new_plan, final_pred) = crate::subquery::decorrelate_where(
            selection,
            plan,
            &scope,
            &table_offsets,
            &col_maps,
            &from_schema,
            schemas,
            ctes,
        )?;
        plan = new_plan;
        if let Some(predicate) = final_pred {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate,
            };
        }
    }

    let mut group_exprs = Vec::new();
    match &select.group_by {
        sql::GroupByExpr::Expressions(exprs, _) => {
            for (i, expr) in exprs.iter().enumerate() {
                if let sql::Expr::Identifier(ident) = expr {
                    if let Some(alias_expr) = select_aliases.get(&ident.value.to_lowercase()) {
                        let b_expr = binder_ctx.bind_expr(alias_expr)?;
                        group_exprs.push((b_expr, ident.value.clone()));
                        continue;
                    }
                }
                let b_expr = binder_ctx.bind_expr(expr)?;
                let name = expr_default_name(expr, &format!("col_{}", i));
                group_exprs.push((b_expr, name));
            }
        }
        sql::GroupByExpr::All(_) => bail!("GROUP BY ALL not supported"),
    }

    let mut agg_collector = AggCollector::default();
    for item in &select.projection {
        match item {
            sql::SelectItem::UnnamedExpr(e) | sql::SelectItem::ExprWithAlias { expr: e, .. } => {
                agg_collector.find_aggs(e)?;
            }
            _ => {}
        }
    }
    if let Some(having) = &select.having {
        agg_collector.find_aggs(having)?;
    }

    let is_aggregate = !group_exprs.is_empty() || !agg_collector.aggs.is_empty();

    if is_aggregate {
        let mut bound_aggs = Vec::new();
        for (func, arg_opt, alias) in &agg_collector.aggs {
            let bound_arg = match arg_opt {
                Some(arg) => Some(binder_ctx.bind_expr(arg)?),
                None => None,
            };
            bound_aggs.push((
                AggExpr {
                    func: *func,
                    arg: bound_arg,
                },
                alias.clone(),
            ));
        }

        let is_q6_like_direct_agg = group_exprs.is_empty()
            && select.projection.len() == 1
            && select.having.is_none()
            && match &select.projection[0] {
                sql::SelectItem::UnnamedExpr(e)
                | sql::SelectItem::ExprWithAlias { expr: e, .. } => {
                    matches!(e, sql::Expr::Function(_))
                }
                _ => false,
            };

        if is_q6_like_direct_agg {
            let alias = match &select.projection[0] {
                sql::SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
                sql::SelectItem::UnnamedExpr(e) => expr_default_name(e, "agg_0"),
                _ => unreachable!(),
            };
            bound_aggs[0].1 = alias;
            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_by: vec![],
                aggs: bound_aggs,
            };
        } else {
            let agg_plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_by: group_exprs.clone(),
                aggs: bound_aggs,
            };
            let agg_schema = agg_plan.schema()?;

            let mut final_proj = Vec::new();
            for item in &select.projection {
                match item {
                    sql::SelectItem::UnnamedExpr(e) => {
                        let expr = rewrite_post_agg_expr(
                            e,
                            &group_exprs,
                            &agg_collector.aggs,
                            &binder_ctx,
                            &agg_schema,
                        )?;
                        let name = expr_default_name(e, "col");
                        final_proj.push((expr, name));
                    }
                    sql::SelectItem::ExprWithAlias { expr: e, alias } => {
                        let expr = rewrite_post_agg_expr(
                            e,
                            &group_exprs,
                            &agg_collector.aggs,
                            &binder_ctx,
                            &agg_schema,
                        )?;
                        final_proj.push((expr, alias.value.clone()));
                    }
                    sql::SelectItem::Wildcard(_) => {
                        for (i, f) in agg_schema.fields.iter().enumerate() {
                            final_proj.push((Expr::Column(i), f.name.clone()));
                        }
                    }
                    sql::SelectItem::QualifiedWildcard(obj_name, _) => {
                        bail!(
                            "qualified wildcard over aggregate not supported: {}",
                            obj_name
                        );
                    }
                }
            }

            let mut cur_plan = agg_plan;

            if let Some(having) = &select.having {
                cur_plan = crate::subquery::decorrelate_having(
                    having,
                    cur_plan,
                    &group_exprs,
                    &agg_collector.aggs,
                    &binder_ctx,
                    schemas,
                    ctes,
                )?;
            }

            let is_identity = final_proj.len() == agg_schema.len()
                && final_proj.iter().enumerate().all(|(i, (expr, name))| {
                    matches!(expr, Expr::Column(c) if *c == i) && *name == agg_schema.fields[i].name
                });

            if !is_identity {
                cur_plan = LogicalPlan::Project {
                    input: Box::new(cur_plan),
                    exprs: final_proj,
                };
            }
            plan = cur_plan;
        }
    } else {
        let mut proj_exprs = Vec::new();
        for item in &select.projection {
            match item {
                sql::SelectItem::UnnamedExpr(e) => {
                    let b_expr = binder_ctx.bind_expr(e)?;
                    let name = expr_default_name(e, "col");
                    proj_exprs.push((b_expr, name));
                }
                sql::SelectItem::ExprWithAlias { expr: e, alias } => {
                    let b_expr = binder_ctx.bind_expr(e)?;
                    proj_exprs.push((b_expr, alias.value.clone()));
                }
                sql::SelectItem::Wildcard(_) => {
                    let cur_schema = plan.schema()?;
                    for (i, f) in cur_schema.fields.iter().enumerate() {
                        proj_exprs.push((Expr::Column(i), f.name.clone()));
                    }
                }
                sql::SelectItem::QualifiedWildcard(tbl, _) => {
                    let q = tbl.to_string();
                    let cur_schema = plan.schema()?;
                    let (t_idx, _) = scope
                        .resolve_column(Some(&q), &cur_schema.fields[0].name)
                        .or_else(|_| {
                            for t in &scope.tables {
                                if t.alias.eq_ignore_ascii_case(&q)
                                    || t.table_name
                                        .as_deref()
                                        .is_some_and(|tn| tn.eq_ignore_ascii_case(&q))
                                {
                                    return Ok((0, 0));
                                }
                            }
                            bail!("table '{}' not found", q);
                        })?;
                    let t_start = table_offsets[t_idx];
                    let t_len = col_maps[t_idx].len();
                    for i in 0..t_len {
                        let col_idx = t_start + i;
                        proj_exprs.push((
                            Expr::Column(col_idx),
                            cur_schema.fields[col_idx].name.clone(),
                        ));
                    }
                }
            }
        }

        let cur_schema = plan.schema()?;
        let is_identity = proj_exprs.len() == cur_schema.len()
            && proj_exprs.iter().enumerate().all(|(i, (expr, name))| {
                matches!(expr, Expr::Column(c) if *c == i) && *name == cur_schema.fields[i].name
            });

        if !is_identity {
            plan = LogicalPlan::Project {
                input: Box::new(plan),
                exprs: proj_exprs,
            };
        }
    }

    if let Some(order_by) = &query.order_by {
        let cur_schema = plan.schema()?;
        let mut keys = Vec::new();
        for sort_expr in &order_by.exprs {
            let asc = sort_expr.asc.unwrap_or(true);
            let nulls_first = sort_expr.nulls_first.unwrap_or(!asc);

            let bound_sort_expr = match &sort_expr.expr {
                sql::Expr::Value(sql::Value::Number(n, _)) => {
                    let idx: usize = n
                        .parse()
                        .map_err(|_| anyhow!("invalid order by index: {}", n))?;
                    if idx == 0 || idx > cur_schema.len() {
                        bail!(
                            "order by index {} out of range (1..{})",
                            idx,
                            cur_schema.len()
                        );
                    }
                    Expr::Column(idx - 1)
                }
                sql::Expr::Identifier(ident) => {
                    if let Some(pos) = cur_schema.index_of(&ident.value) {
                        Expr::Column(pos)
                    } else {
                        binder_ctx.bind_expr(&sort_expr.expr)?
                    }
                }
                _ => binder_ctx.bind_expr(&sort_expr.expr)?,
            };

            keys.push(SortKey {
                expr: bound_sort_expr,
                asc,
                nulls_first,
            });
        }
        plan = LogicalPlan::Sort {
            input: Box::new(plan),
            keys,
        };
    }

    if let Some(limit_expr) = &query.limit {
        match limit_expr {
            sql::Expr::Value(sql::Value::Number(n, _)) => {
                let limit: usize = n
                    .parse()
                    .map_err(|_| anyhow!("invalid limit value: {}", n))?;
                plan = LogicalPlan::Limit {
                    input: Box::new(plan),
                    n: limit,
                };
            }
            other => bail!("unsupported limit expression: {:?}", other),
        }
    }

    Ok(plan)
}

type JoinPredicates = (Vec<(Expr, Expr)>, Option<Expr>);

fn split_join_predicate(expr: &Expr, right_offset: usize) -> Result<JoinPredicates> {
    let mut on_keys = Vec::new();
    let mut filter_parts = Vec::new();

    fn collect_preds(
        e: &Expr,
        right_offset: usize,
        on_keys: &mut Vec<(Expr, Expr)>,
        filters: &mut Vec<Expr>,
    ) {
        match e {
            Expr::Binary {
                op: BinOp::And,
                left,
                right,
            } => {
                collect_preds(left, right_offset, on_keys, filters);
                collect_preds(right, right_offset, on_keys, filters);
            }
            Expr::Binary {
                op: BinOp::Eq,
                left,
                right,
            } => {
                let left_cols = get_referenced_columns(left);
                let right_cols = get_referenced_columns(right);

                let left_is_left =
                    !left_cols.is_empty() && left_cols.iter().all(|&c| c < right_offset);
                let left_is_right =
                    !left_cols.is_empty() && left_cols.iter().all(|&c| c >= right_offset);
                let right_is_left =
                    !right_cols.is_empty() && right_cols.iter().all(|&c| c < right_offset);
                let right_is_right =
                    !right_cols.is_empty() && right_cols.iter().all(|&c| c >= right_offset);

                if left_is_left && right_is_right {
                    let r_shifted = shift_columns(right, -(right_offset as i64));
                    on_keys.push(((**left).clone(), r_shifted));
                } else if left_is_right && right_is_left {
                    let l_shifted = shift_columns(left, -(right_offset as i64));
                    on_keys.push(((**right).clone(), l_shifted));
                } else {
                    filters.push(e.clone());
                }
            }
            _ => filters.push(e.clone()),
        }
    }

    collect_preds(expr, right_offset, &mut on_keys, &mut filter_parts);

    let filter = if filter_parts.is_empty() {
        None
    } else {
        let mut f = filter_parts.remove(0);
        for next in filter_parts {
            f = Expr::Binary {
                op: BinOp::And,
                left: Box::new(f),
                right: Box::new(next),
            };
        }
        Some(f)
    };

    Ok((on_keys, filter))
}

fn get_referenced_columns(expr: &Expr) -> Vec<usize> {
    let mut cols = Vec::new();
    fn visit(e: &Expr, cols: &mut Vec<usize>) {
        match e {
            Expr::Column(idx) => cols.push(*idx),
            Expr::Binary { left, right, .. } => {
                visit(left, cols);
                visit(right, cols);
            }
            Expr::Not(sub) | Expr::Negate(sub) | Expr::IsNull(sub) => visit(sub, cols),
            Expr::Like { expr, .. } => visit(expr, cols),
            Expr::InList { expr, .. } => visit(expr, cols),
            Expr::Case { whens, else_expr } => {
                for (c, r) in whens {
                    visit(c, cols);
                    visit(r, cols);
                }
                if let Some(el) = else_expr {
                    visit(el, cols);
                }
            }
            Expr::Cast { expr, .. } => visit(expr, cols),
            Expr::Extract { expr, .. } => visit(expr, cols),
            Expr::Substring { expr, .. } => visit(expr, cols),
            Expr::Literal(_) => {}
        }
    }
    visit(expr, &mut cols);
    cols
}

fn shift_columns(expr: &Expr, shift: i64) -> Expr {
    match expr {
        Expr::Column(idx) => Expr::Column((*idx as i64 + shift) as usize),
        Expr::Literal(s) => Expr::Literal(s.clone()),
        Expr::Binary { op, left, right } => Expr::Binary {
            op: *op,
            left: Box::new(shift_columns(left, shift)),
            right: Box::new(shift_columns(right, shift)),
        },
        Expr::Not(sub) => Expr::Not(Box::new(shift_columns(sub, shift))),
        Expr::Negate(sub) => Expr::Negate(Box::new(shift_columns(sub, shift))),
        Expr::IsNull(sub) => Expr::IsNull(Box::new(shift_columns(sub, shift))),
        Expr::Like {
            expr,
            pattern,
            negated,
        } => Expr::Like {
            expr: Box::new(shift_columns(expr, shift)),
            pattern: pattern.clone(),
            negated: *negated,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(shift_columns(expr, shift)),
            list: list.clone(),
            negated: *negated,
        },
        Expr::Case { whens, else_expr } => Expr::Case {
            whens: whens
                .iter()
                .map(|(c, r)| (shift_columns(c, shift), shift_columns(r, shift)))
                .collect(),
            else_expr: else_expr
                .as_ref()
                .map(|e| Box::new(shift_columns(e, shift))),
        },
        Expr::Cast { expr, to } => Expr::Cast {
            expr: Box::new(shift_columns(expr, shift)),
            to: *to,
        },
        Expr::Extract { field, expr } => Expr::Extract {
            field: *field,
            expr: Box::new(shift_columns(expr, shift)),
        },
        Expr::Substring { expr, start, len } => Expr::Substring {
            expr: Box::new(shift_columns(expr, shift)),
            start: *start,
            len: *len,
        },
    }
}

fn bind_table_factor(
    factor: &sql::TableFactor,
    scope: &mut QueryScope,
    schemas: &dyn SchemaProvider,
    ctes: &HashMap<String, sql::Query>,
) -> Result<()> {
    match factor {
        sql::TableFactor::Table { name, alias, .. } => {
            let tbl_name = name.to_string();
            let effective_alias = alias
                .as_ref()
                .map(|a| a.name.value.clone())
                .unwrap_or_else(|| tbl_name.clone());

            if let Some(cte_query) = ctes.get(&tbl_name.to_lowercase()) {
                let plan = bind_query(cte_query, schemas, ctes)?;
                let schema = plan.schema()?;
                scope.tables.push(TableScope {
                    alias: effective_alias,
                    table_name: Some(tbl_name),
                    full_schema: schema,
                    used_columns: HashSet::new(),
                    derived_plan: Some(plan),
                });
            } else if let Some(schema) = schemas.table_schema(&tbl_name) {
                scope.tables.push(TableScope {
                    alias: effective_alias,
                    table_name: Some(tbl_name),
                    full_schema: schema,
                    used_columns: HashSet::new(),
                    derived_plan: None,
                });
            } else {
                bail!("table '{}' not found", tbl_name);
            }
        }
        sql::TableFactor::Derived {
            subquery, alias, ..
        } => {
            let derived_plan = bind_query(subquery, schemas, ctes)?;
            let mut schema = derived_plan.schema()?;

            let effective_alias = if let Some(a) = alias {
                if !a.columns.is_empty() {
                    for (i, col_def) in a.columns.iter().enumerate() {
                        if i < schema.fields.len() {
                            schema.fields[i].name = col_def.name.value.clone();
                        }
                    }
                }
                a.name.value.clone()
            } else {
                "derived".to_string()
            };

            scope.tables.push(TableScope {
                alias: effective_alias,
                table_name: None,
                full_schema: schema,
                used_columns: HashSet::new(),
                derived_plan: Some(derived_plan),
            });
        }
        sql::TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            bind_table_factor(&table_with_joins.relation, scope, schemas, ctes)?;
            for join in &table_with_joins.joins {
                bind_table_factor(&join.relation, scope, schemas, ctes)?;
            }
        }
        other => bail!("unsupported table factor: {:?}", other),
    }
    Ok(())
}

pub(crate) struct ExprBinderContext<'a> {
    pub(crate) scope: &'a QueryScope,
    pub(crate) table_offsets: &'a [usize],
    pub(crate) col_maps: &'a [HashMap<usize, usize>],
    pub(crate) from_schema: &'a Schema,
}

impl<'a> ExprBinderContext<'a> {
    pub(crate) fn bind_expr(&self, expr: &sql::Expr) -> Result<Expr> {
        if let Some(folded_date) = try_fold_date_expr(expr)? {
            return Ok(Expr::Literal(folded_date));
        }

        match expr {
            sql::Expr::Identifier(ident) => {
                let (t_idx, orig_col) = self.scope.resolve_column(None, &ident.value)?;
                let col_idx = self.table_offsets[t_idx] + self.col_maps[t_idx][&orig_col];
                Ok(Expr::Column(col_idx))
            }
            sql::Expr::CompoundIdentifier(parts) => {
                if parts.len() == 2 {
                    let (t_idx, orig_col) = self
                        .scope
                        .resolve_column(Some(&parts[0].value), &parts[1].value)?;
                    let col_idx = self.table_offsets[t_idx] + self.col_maps[t_idx][&orig_col];
                    Ok(Expr::Column(col_idx))
                } else {
                    bail!("unsupported compound identifier: {:?}", parts);
                }
            }
            sql::Expr::Value(val) => {
                let scalar = parse_sql_value(val)?;
                Ok(Expr::Literal(scalar))
            }
            sql::Expr::UnaryOp {
                op: sql::UnaryOperator::Minus,
                expr,
            } => {
                if let sql::Expr::Value(val) = &**expr {
                    let scalar = parse_sql_value(val)?;
                    let neg_scalar = match scalar {
                        Scalar::Int32(x) => Scalar::Int32(-x),
                        Scalar::Int64(x) => Scalar::Int64(-x),
                        Scalar::Float64(f) => Scalar::Float64(-f),
                        Scalar::Decimal(v, s) => Scalar::Decimal(-v, s),
                        other => bail!("cannot negate {:?}", other),
                    };
                    Ok(Expr::Literal(neg_scalar))
                } else {
                    let b = self.bind_expr(expr)?;
                    Ok(Expr::Negate(Box::new(b)))
                }
            }
            sql::Expr::UnaryOp {
                op: sql::UnaryOperator::Not,
                expr,
            } => {
                let b = self.bind_expr(expr)?;
                Ok(Expr::Not(Box::new(b)))
            }
            sql::Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let b_expr = self.bind_expr(expr)?;
                let b_low = self.bind_expr(low)?;
                let b_high = self.bind_expr(high)?;

                let (b_expr_low, b_low) =
                    coerce_binary_operands(b_expr.clone(), b_low, BinOp::GtEq, self.from_schema)?;
                let (b_expr_high, b_high) =
                    coerce_binary_operands(b_expr, b_high, BinOp::LtEq, self.from_schema)?;

                let gte = Expr::Binary {
                    op: BinOp::GtEq,
                    left: Box::new(b_expr_low),
                    right: Box::new(b_low),
                };
                let lte = Expr::Binary {
                    op: BinOp::LtEq,
                    left: Box::new(b_expr_high),
                    right: Box::new(b_high),
                };
                let and = Expr::Binary {
                    op: BinOp::And,
                    left: Box::new(gte),
                    right: Box::new(lte),
                };
                if *negated {
                    Ok(Expr::Not(Box::new(and)))
                } else {
                    Ok(and)
                }
            }
            sql::Expr::BinaryOp { left, op, right } => {
                let b_left = self.bind_expr(left)?;
                let b_right = self.bind_expr(right)?;
                let bin_op = map_bin_op(op)?;

                let (final_l, final_r) =
                    coerce_binary_operands(b_left, b_right, bin_op, self.from_schema)?;

                Ok(Expr::Binary {
                    op: bin_op,
                    left: Box::new(final_l),
                    right: Box::new(final_r),
                })
            }
            sql::Expr::IsNull(expr) => {
                let b = self.bind_expr(expr)?;
                Ok(Expr::IsNull(Box::new(b)))
            }
            sql::Expr::IsNotNull(expr) => {
                let b = self.bind_expr(expr)?;
                Ok(Expr::Not(Box::new(Expr::IsNull(Box::new(b)))))
            }
            sql::Expr::Like {
                expr,
                pattern,
                negated,
                ..
            } => {
                let b = self.bind_expr(expr)?;
                let pat_str = match &**pattern {
                    sql::Expr::Value(sql::Value::SingleQuotedString(s)) => s.clone(),
                    other => bail!("unsupported LIKE pattern: {:?}", other),
                };
                Ok(Expr::Like {
                    expr: Box::new(b),
                    pattern: pat_str,
                    negated: *negated,
                })
            }
            sql::Expr::InList {
                expr,
                list,
                negated,
            } => {
                let mut b = self.bind_expr(expr)?;
                let mut scalars = Vec::new();
                for item in list {
                    match item {
                        sql::Expr::Value(v) => scalars.push(parse_sql_value(v)?),
                        sql::Expr::UnaryOp {
                            op: sql::UnaryOperator::Minus,
                            expr,
                        } => {
                            if let sql::Expr::Value(v) = &**expr {
                                let s = parse_sql_value(v)?;
                                let neg = match s {
                                    Scalar::Int32(x) => Scalar::Int32(-x),
                                    Scalar::Int64(x) => Scalar::Int64(-x),
                                    Scalar::Decimal(x, sc) => Scalar::Decimal(-x, sc),
                                    Scalar::Float64(f) => Scalar::Float64(-f),
                                    other => bail!("cannot negate {:?}", other),
                                };
                                scalars.push(neg);
                            } else {
                                bail!("unsupported negative in list: {:?}", item);
                            }
                        }
                        other => bail!("unsupported IN list item: {:?}", other),
                    }
                }

                if let Ok(expr_type) = b.data_type(self.from_schema) {
                    if is_numeric(&expr_type) {
                        let mut types = vec![expr_type];
                        for s in &scalars {
                            if let Some(st) = scalar_dtype(s) {
                                types.push(st);
                            }
                        }
                        if let Some(target) = unify_data_types(&types) {
                            if expr_type != target {
                                b = Expr::Cast {
                                    expr: Box::new(b),
                                    to: target,
                                };
                            }
                            let mut new_scalars = Vec::with_capacity(scalars.len());
                            for s in &scalars {
                                if let Some(cs) = cast_scalar(s, &target) {
                                    new_scalars.push(cs);
                                } else {
                                    new_scalars.push(s.clone());
                                }
                            }
                            scalars = new_scalars;
                        }
                    }
                }

                Ok(Expr::InList {
                    expr: Box::new(b),
                    list: scalars,
                    negated: *negated,
                })
            }
            sql::Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                let mut whens = Vec::new();
                for (cond, res) in conditions.iter().zip(results) {
                    let cond_expr = if let Some(op) = operand {
                        let b_op = self.bind_expr(op)?;
                        let b_cond = self.bind_expr(cond)?;
                        let (final_op, final_cond) =
                            coerce_binary_operands(b_op, b_cond, BinOp::Eq, self.from_schema)?;
                        Expr::Binary {
                            op: BinOp::Eq,
                            left: Box::new(final_op),
                            right: Box::new(final_cond),
                        }
                    } else {
                        self.bind_expr(cond)?
                    };
                    let res_expr = self.bind_expr(res)?;
                    whens.push((cond_expr, res_expr));
                }
                let mut else_expr = if let Some(el) = else_result {
                    Some(Box::new(self.bind_expr(el)?))
                } else {
                    None
                };
                unify_case_branches(&mut whens, &mut else_expr, self.from_schema)?;
                Ok(Expr::Case { whens, else_expr })
            }
            sql::Expr::Function(func) if func.name.to_string().eq_ignore_ascii_case("coalesce") => {
                let (args, _) = get_function_args(func);
                if args.is_empty() {
                    bail!("COALESCE requires at least one argument");
                }
                let expr_args: Vec<sql::Expr> = args
                    .into_iter()
                    .filter_map(|arg| {
                        if let sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(e)) = arg {
                            Some(e.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                if expr_args.len() == 1 {
                    self.bind_expr(&expr_args[0])
                } else {
                    let mut whens = Vec::new();
                    for arg in &expr_args[..expr_args.len() - 1] {
                        let cond = sql::Expr::IsNotNull(Box::new(arg.clone()));
                        whens.push((cond, arg.clone()));
                    }
                    let else_res = expr_args.last().cloned();
                    let case_expr = sql::Expr::Case {
                        operand: None,
                        conditions: whens.iter().map(|(c, _)| c.clone()).collect(),
                        results: whens.into_iter().map(|(_, r)| r).collect(),
                        else_result: else_res.map(Box::new),
                    };
                    self.bind_expr(&case_expr)
                }
            }
            sql::Expr::Cast {
                expr, data_type, ..
            } => {
                let b = self.bind_expr(expr)?;
                let to_dtype = map_sql_datatype(data_type)?;
                Ok(Expr::Cast {
                    expr: Box::new(b),
                    to: to_dtype,
                })
            }
            sql::Expr::Extract { field, expr, .. } => {
                let b = self.bind_expr(expr)?;
                let date_field = match field {
                    sql::DateTimeField::Year => DateField::Year,
                    other => bail!("unsupported extract field: {:?}", other),
                };
                Ok(Expr::Extract {
                    field: date_field,
                    expr: Box::new(b),
                })
            }
            sql::Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let b = self.bind_expr(expr)?;
                let start = if let Some(from_expr) = substring_from {
                    match &**from_expr {
                        sql::Expr::Value(sql::Value::Number(n, _)) => n.parse::<i64>()?,
                        _ => bail!("unsupported substring from expr"),
                    }
                } else {
                    1
                };
                let len = if let Some(for_expr) = substring_for {
                    match &**for_expr {
                        sql::Expr::Value(sql::Value::Number(n, _)) => n.parse::<i64>()?,
                        _ => bail!("unsupported substring for expr"),
                    }
                } else {
                    i64::MAX
                };
                Ok(Expr::Substring {
                    expr: Box::new(b),
                    start,
                    len,
                })
            }
            sql::Expr::Nested(inner) => self.bind_expr(inner),
            other => bail!("unsupported SQL expression: {:?}", other),
        }
    }
}

pub(crate) fn pow10(n: u8) -> i128 {
    10_i128.pow(n as u32)
}

pub(crate) fn is_numeric(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int32 | DataType::Int64 | DataType::Float64 | DataType::Decimal { .. }
    )
}

fn scalar_dtype(s: &Scalar) -> Option<DataType> {
    match s {
        Scalar::Int32(_) => Some(DataType::Int32),
        Scalar::Int64(_) => Some(DataType::Int64),
        Scalar::Float64(_) => Some(DataType::Float64),
        Scalar::Decimal(_, scale) => Some(DataType::Decimal {
            precision: 38,
            scale: *scale,
        }),
        _ => None,
    }
}

pub(crate) fn cast_scalar(s: &Scalar, to: &DataType) -> Option<Scalar> {
    match (s, to) {
        (Scalar::Int32(x), DataType::Int32) => Some(Scalar::Int32(*x)),
        (Scalar::Int32(x), DataType::Int64) => Some(Scalar::Int64(*x as i64)),
        (Scalar::Int32(x), DataType::Float64) => Some(Scalar::Float64(*x as f64)),
        (Scalar::Int32(x), DataType::Decimal { scale, .. }) => {
            Some(Scalar::Decimal(*x as i128 * pow10(*scale), *scale))
        }

        (Scalar::Int64(x), DataType::Int64) => Some(Scalar::Int64(*x)),
        (Scalar::Int64(x), DataType::Int32) => Some(Scalar::Int32(*x as i32)),
        (Scalar::Int64(x), DataType::Float64) => Some(Scalar::Float64(*x as f64)),
        (Scalar::Int64(x), DataType::Decimal { scale, .. }) => {
            Some(Scalar::Decimal(*x as i128 * pow10(*scale), *scale))
        }

        (Scalar::Float64(x), DataType::Float64) => Some(Scalar::Float64(*x)),
        (Scalar::Float64(x), DataType::Decimal { scale, .. }) => Some(Scalar::Decimal(
            (*x * pow10(*scale) as f64).round() as i128,
            *scale,
        )),

        (
            Scalar::Decimal(x, s),
            DataType::Decimal {
                scale: target_s, ..
            },
        ) => {
            if *target_s >= *s {
                Some(Scalar::Decimal(*x * pow10(*target_s - *s), *target_s))
            } else {
                Some(Scalar::Decimal(*x / pow10(*s - *target_s), *target_s))
            }
        }
        (Scalar::Decimal(x, s), DataType::Float64) => {
            Some(Scalar::Float64(*x as f64 / pow10(*s) as f64))
        }

        (Scalar::Utf8(s), DataType::Utf8) => Some(Scalar::Utf8(s.clone())),
        (Scalar::Date32(d), DataType::Date32) => Some(Scalar::Date32(*d)),
        (Scalar::Bool(b), DataType::Bool) => Some(Scalar::Bool(*b)),
        _ => None,
    }
}

pub(crate) fn needs_cast(current: &DataType, target: &DataType) -> bool {
    if current == target {
        return false;
    }
    if let (DataType::Decimal { scale: s1, .. }, DataType::Decimal { scale: s2, .. }) =
        (current, target)
    {
        return s1 != s2;
    }
    true
}

pub(crate) fn unify_two_types(t1: &DataType, t2: &DataType) -> Option<DataType> {
    if t1 == t2 {
        return Some(*t1);
    }
    if !is_numeric(t1) || !is_numeric(t2) {
        return None;
    }
    if *t1 == DataType::Float64 || *t2 == DataType::Float64 {
        return Some(DataType::Float64);
    }
    if let (DataType::Decimal { scale: s1, .. }, DataType::Decimal { scale: s2, .. }) = (t1, t2) {
        return Some(DataType::Decimal {
            precision: 38,
            scale: (*s1).max(*s2),
        });
    }
    if let (DataType::Decimal { scale, .. }, DataType::Int32 | DataType::Int64)
    | (DataType::Int32 | DataType::Int64, DataType::Decimal { scale, .. }) = (t1, t2)
    {
        return Some(DataType::Decimal {
            precision: 38,
            scale: *scale,
        });
    }
    if matches!(
        (t1, t2),
        (DataType::Int32, DataType::Int64) | (DataType::Int64, DataType::Int32)
    ) {
        return Some(DataType::Int64);
    }
    None
}

pub(crate) fn unify_data_types(types: &[DataType]) -> Option<DataType> {
    if types.is_empty() {
        return None;
    }
    let mut current = types[0];
    for t in &types[1..] {
        current = unify_two_types(&current, t)?;
    }
    Some(current)
}

pub(crate) fn unify_case_branches(
    whens: &mut [(Expr, Expr)],
    else_expr: &mut Option<Box<Expr>>,
    schema: &Schema,
) -> Result<()> {
    let mut types = Vec::new();
    for (_, res) in whens.iter() {
        if let Ok(t) = res.data_type(schema) {
            types.push(t);
        }
    }
    if let Some(el) = else_expr {
        if let Ok(t) = el.data_type(schema) {
            types.push(t);
        }
    }
    if let Some(target) = unify_data_types(&types) {
        for (_, res) in whens.iter_mut() {
            if let Ok(cur_t) = res.data_type(schema) {
                if needs_cast(&cur_t, &target) {
                    *res = Expr::Cast {
                        expr: Box::new(res.clone()),
                        to: target,
                    };
                }
            }
        }
        if let Some(el) = else_expr {
            if let Ok(cur_t) = el.data_type(schema) {
                if needs_cast(&cur_t, &target) {
                    let inner = el.clone();
                    **el = Expr::Cast {
                        expr: inner,
                        to: target,
                    };
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn coerce_binary_operands(
    left: Expr,
    right: Expr,
    op: BinOp,
    schema: &Schema,
) -> Result<(Expr, Expr)> {
    if matches!(op, BinOp::And | BinOp::Or) {
        return Ok((left, right));
    }

    let lt = match left.data_type(schema) {
        Ok(t) => t,
        Err(_) => return Ok((left, right)),
    };
    let rt = match right.data_type(schema) {
        Ok(t) => t,
        Err(_) => return Ok((left, right)),
    };

    if !is_numeric(&lt) || !is_numeric(&rt) {
        return Ok((left, right));
    }

    if lt == rt {
        return Ok((left, right));
    }

    // Follow docs/plan-m1.md typing rules:
    // anything with Float64 becomes Float64
    if lt == DataType::Float64 {
        return Ok((
            left,
            Expr::Cast {
                expr: Box::new(right),
                to: DataType::Float64,
            },
        ));
    }
    if rt == DataType::Float64 {
        return Ok((
            Expr::Cast {
                expr: Box::new(left),
                to: DataType::Float64,
            },
            right,
        ));
    }

    // integer with Decimal becomes Decimal with the integer at scale 0
    if matches!(lt, DataType::Decimal { .. }) && matches!(rt, DataType::Int32 | DataType::Int64) {
        return Ok((
            left,
            Expr::Cast {
                expr: Box::new(right),
                to: DataType::Decimal {
                    precision: 38,
                    scale: 0,
                },
            },
        ));
    }
    if matches!(rt, DataType::Decimal { .. }) && matches!(lt, DataType::Int32 | DataType::Int64) {
        return Ok((
            Expr::Cast {
                expr: Box::new(left),
                to: DataType::Decimal {
                    precision: 38,
                    scale: 0,
                },
            },
            right,
        ));
    }

    // Int32 op Int64 gives Int64
    if lt == DataType::Int32 && rt == DataType::Int64 {
        return Ok((
            Expr::Cast {
                expr: Box::new(left),
                to: DataType::Int64,
            },
            right,
        ));
    }
    if lt == DataType::Int64 && rt == DataType::Int32 {
        return Ok((
            left,
            Expr::Cast {
                expr: Box::new(right),
                to: DataType::Int64,
            },
        ));
    }

    Ok((left, right))
}

pub(crate) fn coerce_for_cmp(left: Expr, right: Expr, schema: &Schema) -> Result<(Expr, Expr)> {
    coerce_binary_operands(left, right, BinOp::Eq, schema)
}

pub(crate) fn map_bin_op(op: &sql::BinaryOperator) -> Result<BinOp> {
    match op {
        sql::BinaryOperator::Plus => Ok(BinOp::Add),
        sql::BinaryOperator::Minus => Ok(BinOp::Sub),
        sql::BinaryOperator::Multiply => Ok(BinOp::Mul),
        sql::BinaryOperator::Divide => Ok(BinOp::Div),
        sql::BinaryOperator::Eq => Ok(BinOp::Eq),
        sql::BinaryOperator::NotEq => Ok(BinOp::NotEq),
        sql::BinaryOperator::Lt => Ok(BinOp::Lt),
        sql::BinaryOperator::LtEq => Ok(BinOp::LtEq),
        sql::BinaryOperator::Gt => Ok(BinOp::Gt),
        sql::BinaryOperator::GtEq => Ok(BinOp::GtEq),
        sql::BinaryOperator::And => Ok(BinOp::And),
        sql::BinaryOperator::Or => Ok(BinOp::Or),
        other => bail!("unsupported binary operator: {:?}", other),
    }
}

fn map_sql_datatype(dt: &sql::DataType) -> Result<DataType> {
    match dt {
        sql::DataType::Date => Ok(DataType::Date32),
        sql::DataType::Int(_) | sql::DataType::Integer(_) => Ok(DataType::Int32),
        sql::DataType::BigInt(_) => Ok(DataType::Int64),
        sql::DataType::Float(_) | sql::DataType::Double(_) => Ok(DataType::Float64),
        sql::DataType::Decimal(info) => match info {
            sql::ExactNumberInfo::PrecisionAndScale(p, s) => Ok(DataType::Decimal {
                precision: *p as u8,
                scale: *s as u8,
            }),
            _ => Ok(DataType::Decimal {
                precision: 38,
                scale: 2,
            }),
        },
        sql::DataType::Varchar(_) | sql::DataType::Text | sql::DataType::Char(_) => {
            Ok(DataType::Utf8)
        }
        sql::DataType::Boolean => Ok(DataType::Bool),
        other => bail!("unsupported sql data type: {:?}", other),
    }
}

fn parse_sql_value(val: &sql::Value) -> Result<Scalar> {
    match val {
        sql::Value::Number(s, _) => {
            if s.contains('.') {
                let parts: Vec<&str> = s.split('.').collect();
                if parts.len() == 2 {
                    let scale = parts[1].len() as u8;
                    let whole_str = format!("{}{}", parts[0], parts[1]);
                    if let Ok(v) = whole_str.parse::<i128>() {
                        return Ok(Scalar::Decimal(v, scale));
                    }
                }
                if let Ok(f) = s.parse::<f64>() {
                    Ok(Scalar::Float64(f))
                } else {
                    bail!("invalid decimal/float literal: {}", s)
                }
            } else if let Ok(i) = s.parse::<i64>() {
                if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                    Ok(Scalar::Int32(i as i32))
                } else {
                    Ok(Scalar::Int64(i))
                }
            } else {
                bail!("invalid integer literal: {}", s)
            }
        }
        sql::Value::SingleQuotedString(s) => Ok(Scalar::Utf8(s.clone())),
        sql::Value::Boolean(b) => Ok(Scalar::Bool(*b)),
        sql::Value::Null => Ok(Scalar::Null),
        other => bail!("unsupported sql value literal: {:?}", other),
    }
}

fn parse_date_str(s: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|e| anyhow!("invalid date '{}': {}", s, e))
}

fn date_to_days(d: NaiveDate) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    d.signed_duration_since(epoch).num_days() as i32
}

fn extract_interval_parts(
    val_expr: &sql::Expr,
    leading_field: Option<&sql::DateTimeField>,
) -> Result<(i64, sql::DateTimeField)> {
    let amount_str = match val_expr {
        sql::Expr::Value(sql::Value::SingleQuotedString(s)) => s.clone(),
        sql::Expr::Value(sql::Value::Number(s, _)) => s.clone(),
        other => bail!("unsupported interval value expr: {:?}", other),
    };
    let amount: i64 = amount_str
        .parse()
        .map_err(|_| anyhow!("invalid interval amount: {}", amount_str))?;

    let field = match leading_field {
        Some(f) => f.clone(),
        None => sql::DateTimeField::Day,
    };
    Ok((amount, field))
}

fn try_fold_date_expr(expr: &sql::Expr) -> Result<Option<Scalar>> {
    match expr {
        sql::Expr::TypedString { data_type, value } => {
            if matches!(data_type, sql::DataType::Date)
                || data_type.to_string().eq_ignore_ascii_case("date")
            {
                let d = parse_date_str(value)?;
                return Ok(Some(Scalar::Date32(date_to_days(d))));
            }
        }
        sql::Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            if matches!(data_type, sql::DataType::Date) {
                if let sql::Expr::Value(sql::Value::SingleQuotedString(s)) = &**inner {
                    let d = parse_date_str(s)?;
                    return Ok(Some(Scalar::Date32(date_to_days(d))));
                }
            }
        }
        sql::Expr::BinaryOp { left, op, right } => {
            let left_folded = try_fold_date_expr(left)?;
            let right_folded = try_fold_date_expr(right)?;

            if let Some(Scalar::Date32(days)) = left_folded {
                if let sql::Expr::Interval(interval) = &**right {
                    let (amt, unit) =
                        extract_interval_parts(&interval.value, interval.leading_field.as_ref())?;
                    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let date = epoch + Duration::days(days as i64);

                    let res_date = match op {
                        sql::BinaryOperator::Plus => match unit {
                            sql::DateTimeField::Day => date + Duration::days(amt),
                            sql::DateTimeField::Month => date
                                .checked_add_months(Months::new(amt as u32))
                                .ok_or_else(|| anyhow!("date overflow"))?,
                            sql::DateTimeField::Year => date
                                .checked_add_months(Months::new((amt * 12) as u32))
                                .ok_or_else(|| anyhow!("date overflow"))?,
                            other => bail!("unsupported interval unit: {:?}", other),
                        },
                        sql::BinaryOperator::Minus => match unit {
                            sql::DateTimeField::Day => date - Duration::days(amt),
                            sql::DateTimeField::Month => date
                                .checked_sub_months(Months::new(amt as u32))
                                .ok_or_else(|| anyhow!("date overflow"))?,
                            sql::DateTimeField::Year => date
                                .checked_sub_months(Months::new((amt * 12) as u32))
                                .ok_or_else(|| anyhow!("date overflow"))?,
                            other => bail!("unsupported interval unit: {:?}", other),
                        },
                        other => bail!("unsupported date operator: {:?}", other),
                    };
                    return Ok(Some(Scalar::Date32(date_to_days(res_date))));
                }
            } else if let Some(Scalar::Date32(days)) = right_folded {
                if let sql::Expr::Interval(interval) = &**left {
                    if *op == sql::BinaryOperator::Plus {
                        let (amt, unit) = extract_interval_parts(
                            &interval.value,
                            interval.leading_field.as_ref(),
                        )?;
                        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        let date = epoch + Duration::days(days as i64);
                        let res_date = match unit {
                            sql::DateTimeField::Day => date + Duration::days(amt),
                            sql::DateTimeField::Month => date
                                .checked_add_months(Months::new(amt as u32))
                                .ok_or_else(|| anyhow!("date overflow"))?,
                            sql::DateTimeField::Year => date
                                .checked_add_months(Months::new((amt * 12) as u32))
                                .ok_or_else(|| anyhow!("date overflow"))?,
                            other => bail!("unsupported interval unit: {:?}", other),
                        };
                        return Ok(Some(Scalar::Date32(date_to_days(res_date))));
                    }
                }
            }
        }
        _ => {}
    }
    Ok(None)
}

fn get_function_args(func: &sql::Function) -> (Vec<&sql::FunctionArg>, bool) {
    match &func.args {
        sql::FunctionArguments::List(list) => {
            let is_distinct = matches!(
                list.duplicate_treatment,
                Some(sql::DuplicateTreatment::Distinct)
            );
            (list.args.iter().collect(), is_distinct)
        }
        _ => (Vec::new(), false),
    }
}

#[derive(Default)]
struct AggCollector {
    aggs: Vec<(AggFunc, Option<sql::Expr>, String)>,
}

impl AggCollector {
    fn find_aggs(&mut self, expr: &sql::Expr) -> Result<()> {
        match expr {
            sql::Expr::Function(func) => {
                let name = func.name.to_string().to_lowercase();
                let (args, is_distinct) = get_function_args(func);
                let agg_func = match name.as_str() {
                    "sum" => Some(AggFunc::Sum),
                    "avg" => Some(AggFunc::Avg),
                    "min" => Some(AggFunc::Min),
                    "max" => Some(AggFunc::Max),
                    "count" => {
                        let is_star = args.iter().any(|arg| {
                            matches!(
                                arg,
                                sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Wildcard)
                            )
                        });
                        if is_star {
                            Some(AggFunc::CountStar)
                        } else if is_distinct {
                            Some(AggFunc::CountDistinct)
                        } else {
                            Some(AggFunc::Count)
                        }
                    }
                    _ => None,
                };

                if let Some(f) = agg_func {
                    let arg = if f == AggFunc::CountStar {
                        None
                    } else if let Some(sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(e))) =
                        args.first()
                    {
                        Some((*e).clone())
                    } else {
                        None
                    };

                    let exists = self.aggs.iter().any(|(existing_f, existing_arg, _)| {
                        *existing_f == f && existing_arg == &arg
                    });

                    if !exists {
                        let alias = format!("agg_{}", self.aggs.len());
                        self.aggs.push((f, arg, alias));
                    }
                    return Ok(());
                } else {
                    for arg in args {
                        if let sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(e)) = arg {
                            self.find_aggs(e)?;
                        }
                    }
                }
            }
            sql::Expr::BinaryOp { left, right, .. } => {
                self.find_aggs(left)?;
                self.find_aggs(right)?;
            }
            sql::Expr::UnaryOp { expr, .. } => {
                self.find_aggs(expr)?;
            }
            sql::Expr::Case {
                conditions,
                results,
                else_result,
                ..
            } => {
                for cond in conditions {
                    self.find_aggs(cond)?;
                }
                for res in results {
                    self.find_aggs(res)?;
                }
                if let Some(el) = else_result {
                    self.find_aggs(el)?;
                }
            }
            sql::Expr::Cast { expr, .. } => self.find_aggs(expr)?,
            sql::Expr::Nested(inner) => self.find_aggs(inner)?,
            _ => {}
        }
        Ok(())
    }
}

pub(crate) fn rewrite_post_agg_expr(
    expr: &sql::Expr,
    group_by: &[(Expr, String)],
    aggs: &[(AggFunc, Option<sql::Expr>, String)],
    binder_ctx: &ExprBinderContext,
    schema: &Schema,
) -> Result<Expr> {
    if let Ok(b_expr) = binder_ctx.bind_expr(expr) {
        if let Some(pos) = group_by.iter().position(|(g, _)| *g == b_expr) {
            return Ok(Expr::Column(pos));
        }
    }

    match expr {
        sql::Expr::Function(func) => {
            let name = func.name.to_string().to_lowercase();
            let (args, is_distinct) = get_function_args(func);
            let agg_func = match name.as_str() {
                "sum" => Some(AggFunc::Sum),
                "avg" => Some(AggFunc::Avg),
                "min" => Some(AggFunc::Min),
                "max" => Some(AggFunc::Max),
                "count" => {
                    let is_star = args.iter().any(|arg| {
                        matches!(
                            arg,
                            sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Wildcard)
                        )
                    });
                    if is_star {
                        Some(AggFunc::CountStar)
                    } else if is_distinct {
                        Some(AggFunc::CountDistinct)
                    } else {
                        Some(AggFunc::Count)
                    }
                }
                _ => None,
            };

            if let Some(f) = agg_func {
                let arg = if f == AggFunc::CountStar {
                    None
                } else if let Some(sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(e))) =
                    args.first()
                {
                    Some((*e).clone())
                } else {
                    None
                };

                let pos = aggs
                    .iter()
                    .position(|(existing_f, existing_arg, _)| {
                        *existing_f == f && existing_arg == &arg
                    })
                    .ok_or_else(|| anyhow!("aggregate not found in collector: {:?}", expr))?;

                return Ok(Expr::Column(group_by.len() + pos));
            }
            if name == "coalesce" {
                if args.is_empty() {
                    bail!("COALESCE requires at least one argument");
                }
                let expr_args: Vec<sql::Expr> = args
                    .into_iter()
                    .filter_map(|arg| {
                        if let sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(e)) = arg {
                            Some(e.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                if expr_args.len() == 1 {
                    return rewrite_post_agg_expr(
                        &expr_args[0],
                        group_by,
                        aggs,
                        binder_ctx,
                        schema,
                    );
                } else {
                    let mut whens = Vec::new();
                    for arg in &expr_args[..expr_args.len() - 1] {
                        let cond = sql::Expr::IsNotNull(Box::new(arg.clone()));
                        whens.push((cond, arg.clone()));
                    }
                    let else_res = expr_args.last().cloned();
                    let case_expr = sql::Expr::Case {
                        operand: None,
                        conditions: whens.iter().map(|(c, _)| c.clone()).collect(),
                        results: whens.into_iter().map(|(_, r)| r).collect(),
                        else_result: else_res.map(Box::new),
                    };
                    return rewrite_post_agg_expr(&case_expr, group_by, aggs, binder_ctx, schema);
                }
            }
            bail!("unsupported function in post-agg: {:?}", expr);
        }
        sql::Expr::BinaryOp { left, op, right } => {
            let l = rewrite_post_agg_expr(left, group_by, aggs, binder_ctx, schema)?;
            let r = rewrite_post_agg_expr(right, group_by, aggs, binder_ctx, schema)?;
            let bin_op = map_bin_op(op)?;
            let (final_l, final_r) = coerce_binary_operands(l, r, bin_op, schema)?;
            Ok(Expr::Binary {
                op: bin_op,
                left: Box::new(final_l),
                right: Box::new(final_r),
            })
        }
        sql::Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let b_expr = rewrite_post_agg_expr(expr, group_by, aggs, binder_ctx, schema)?;
            let b_low = rewrite_post_agg_expr(low, group_by, aggs, binder_ctx, schema)?;
            let b_high = rewrite_post_agg_expr(high, group_by, aggs, binder_ctx, schema)?;

            let (b_expr_low, b_low) =
                coerce_binary_operands(b_expr.clone(), b_low, BinOp::GtEq, schema)?;
            let (b_expr_high, b_high) =
                coerce_binary_operands(b_expr, b_high, BinOp::LtEq, schema)?;

            let gte = Expr::Binary {
                op: BinOp::GtEq,
                left: Box::new(b_expr_low),
                right: Box::new(b_low),
            };
            let lte = Expr::Binary {
                op: BinOp::LtEq,
                left: Box::new(b_expr_high),
                right: Box::new(b_high),
            };
            let and = Expr::Binary {
                op: BinOp::And,
                left: Box::new(gte),
                right: Box::new(lte),
            };
            if *negated {
                Ok(Expr::Not(Box::new(and)))
            } else {
                Ok(and)
            }
        }
        sql::Expr::UnaryOp {
            op: sql::UnaryOperator::Minus,
            expr,
        } => {
            if let sql::Expr::Value(val) = &**expr {
                let s = parse_sql_value(val)?;
                let neg = match s {
                    Scalar::Int32(x) => Scalar::Int32(-x),
                    Scalar::Int64(x) => Scalar::Int64(-x),
                    Scalar::Float64(f) => Scalar::Float64(-f),
                    Scalar::Decimal(v, sc) => Scalar::Decimal(-v, sc),
                    other => bail!("cannot negate {:?}", other),
                };
                Ok(Expr::Literal(neg))
            } else {
                let sub = rewrite_post_agg_expr(expr, group_by, aggs, binder_ctx, schema)?;
                Ok(Expr::Negate(Box::new(sub)))
            }
        }
        sql::Expr::UnaryOp {
            op: sql::UnaryOperator::Not,
            expr,
        } => {
            let sub = rewrite_post_agg_expr(expr, group_by, aggs, binder_ctx, schema)?;
            Ok(Expr::Not(Box::new(sub)))
        }
        sql::Expr::Value(v) => Ok(Expr::Literal(parse_sql_value(v)?)),
        sql::Expr::Case {
            conditions,
            results,
            else_result,
            ..
        } => {
            let mut whens = Vec::new();
            for (c, r) in conditions.iter().zip(results) {
                let b_c = rewrite_post_agg_expr(c, group_by, aggs, binder_ctx, schema)?;
                let b_r = rewrite_post_agg_expr(r, group_by, aggs, binder_ctx, schema)?;
                whens.push((b_c, b_r));
            }
            let mut el = if let Some(e) = else_result {
                Some(Box::new(rewrite_post_agg_expr(
                    e, group_by, aggs, binder_ctx, schema,
                )?))
            } else {
                None
            };
            unify_case_branches(&mut whens, &mut el, schema)?;
            Ok(Expr::Case {
                whens,
                else_expr: el,
            })
        }
        sql::Expr::Nested(inner) => {
            rewrite_post_agg_expr(inner, group_by, aggs, binder_ctx, schema)
        }
        other => binder_ctx.bind_expr(other),
    }
}

fn expr_default_name(expr: &sql::Expr, fallback: &str) -> String {
    match expr {
        sql::Expr::Identifier(ident) => ident.value.clone(),
        sql::Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|p| p.value.clone())
            .unwrap_or_else(|| fallback.to_string()),
        sql::Expr::Function(func) => func.name.to_string(),
        _ => fallback.to_string(),
    }
}
