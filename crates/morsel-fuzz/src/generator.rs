use anyhow::Result;
use morsel_core::{DataType, Field, Scalar};
use morsel_exec::catalog::Catalog;
use morsel_plan::{AggExpr, AggFunc, BinOp, DateField, Expr, JoinType, LogicalPlan, SortKey};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::drop::DropMask;
use crate::sampler::ValueSampler;
use crate::sql::plan_to_sql;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

struct KeyPair {
    table1: &'static str,
    col1: &'static str,
    table2: &'static str,
    col2: &'static str,
}

const KEY_PAIRS: &[KeyPair] = &[
    KeyPair {
        table1: "lineitem",
        col1: "l_orderkey",
        table2: "orders",
        col2: "o_orderkey",
    },
    KeyPair {
        table1: "orders",
        col1: "o_custkey",
        table2: "customer",
        col2: "c_custkey",
    },
    KeyPair {
        table1: "customer",
        col1: "c_nationkey",
        table2: "nation",
        col2: "n_nationkey",
    },
    KeyPair {
        table1: "nation",
        col1: "n_regionkey",
        table2: "region",
        col2: "r_regionkey",
    },
    KeyPair {
        table1: "supplier",
        col1: "s_nationkey",
        table2: "nation",
        col2: "n_nationkey",
    },
    KeyPair {
        table1: "partsupp",
        col1: "ps_partkey",
        table2: "part",
        col2: "p_partkey",
    },
    KeyPair {
        table1: "partsupp",
        col1: "ps_suppkey",
        table2: "supplier",
        col2: "s_suppkey",
    },
    KeyPair {
        table1: "lineitem",
        col1: "l_partkey",
        table2: "part",
        col2: "p_partkey",
    },
    KeyPair {
        table1: "lineitem",
        col1: "l_suppkey",
        table2: "supplier",
        col2: "s_suppkey",
    },
];

#[derive(Clone, Debug)]
pub struct FuzzPlan {
    pub seed: u64,
    pub plan: LogicalPlan,
    pub sql: String,
    pub shape: String,
    pub droppable_pieces: Vec<String>,
}

fn pick_random_columns(
    total: usize,
    required: Option<usize>,
    min_k: usize,
    max_k: usize,
    rng: &mut StdRng,
) -> Vec<usize> {
    let k = rng.gen_range(min_k..=max_k).min(total).max(1);
    let mut indices: Vec<usize> = (0..total).collect();
    for i in 0..k {
        let j = rng.gen_range(i..total);
        indices.swap(i, j);
    }
    let mut selected: Vec<usize> = indices[0..k].to_vec();
    if let Some(req) = required {
        if !selected.contains(&req) {
            if selected.len() >= max_k {
                selected.pop();
            }
            selected.push(req);
        }
    }
    selected.sort_unstable();
    selected
}

pub fn generate_plan(
    seed: u64,
    catalog: &Catalog,
    sampler: &ValueSampler,
    drop_mask: &DropMask,
) -> Result<FuzzPlan> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut droppable_pieces = Vec::new();
    let mut shape_parts = Vec::new();

    // 1. Pick primary table T1
    let t1_idx = rng.gen_range(0..TPCH_TABLES.len());
    let t1_name = TPCH_TABLES[t1_idx];
    let t1_table = catalog
        .get_table(t1_name)
        .ok_or_else(|| anyhow::anyhow!("table {} not found", t1_name))?;
    let t1_schema = (*t1_table.schema).clone();

    // 2. Decide if Join
    let wants_join = rng.gen_bool(0.5);
    let mut join_info = None;

    if wants_join {
        // Find matching key pairs
        let matching_pairs: Vec<&KeyPair> = KEY_PAIRS
            .iter()
            .filter(|kp| kp.table1 == t1_name || kp.table2 == t1_name)
            .collect();
        if !matching_pairs.is_empty() {
            let pair = matching_pairs[rng.gen_range(0..matching_pairs.len())];
            let (t2_name, k1_name, k2_name) = if pair.table1 == t1_name {
                (pair.table2, pair.col1, pair.col2)
            } else {
                (pair.table1, pair.col2, pair.col1)
            };

            let join_type = match rng.gen_range(0..4) {
                0 => JoinType::Inner,
                1 => JoinType::Left,
                2 => JoinType::Semi,
                _ => JoinType::Anti,
            };
            let has_residual = rng.gen_bool(0.3);

            join_info = Some((t2_name, k1_name, k2_name, join_type, has_residual));
            droppable_pieces.push("join".to_string());
        }
    }

    let is_join_active = join_info.is_some() && !drop_mask.drop_join;

    // 3. Select columns for T1
    let req_k1 = if is_join_active {
        let (_, k1_name, _, _, _) = join_info.unwrap();
        t1_schema.index_of(k1_name)
    } else {
        None
    };

    let t1_cols = pick_random_columns(t1_schema.fields.len(), req_k1, 2, 6, &mut rng);
    let mut plan = LogicalPlan::Scan {
        table: t1_name.to_string(),
        columns: t1_cols.clone(),
        table_schema: t1_schema.clone(),
    };
    shape_parts.push(format!("Scan({})", t1_name));

    // 4. Optional Filter on T1
    let wants_filter = rng.gen_bool(0.7);
    if wants_filter {
        let num_conjuncts = rng.gen_range(1..=3);
        let mut conjuncts = Vec::new();

        for c_idx in 0..num_conjuncts {
            droppable_pieces.push(format!("conjunct:{}", c_idx));
            if drop_mask.drop_filter || drop_mask.drop_conjuncts.contains(&c_idx) {
                continue;
            }

            // Pick a column from t1_cols
            let chosen_col_pos = rng.gen_range(0..t1_cols.len());
            let actual_col_idx = t1_cols[chosen_col_pos];
            let field = &t1_schema.fields[actual_col_idx];

            let conjunct = generate_conjunct(
                chosen_col_pos,
                field,
                t1_name,
                actual_col_idx,
                sampler,
                &mut rng,
            );
            conjuncts.push(conjunct);
        }

        if !conjuncts.is_empty() {
            let mut pred = conjuncts.remove(0);
            for next_c in conjuncts {
                pred = Expr::Binary {
                    op: BinOp::And,
                    left: Box::new(pred),
                    right: Box::new(next_c),
                };
            }
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: pred,
            };
            shape_parts.push(format!("Filter({})", num_conjuncts));
        }
    }

    // 5. Optional Join
    if is_join_active {
        let (t2_name, _k1_name, k2_name, join_type, has_residual) = join_info.unwrap();
        let t2_table = catalog
            .get_table(t2_name)
            .ok_or_else(|| anyhow::anyhow!("table {} not found", t2_name))?;
        let t2_schema = (*t2_table.schema).clone();
        let req_k2 = t2_schema.index_of(k2_name);

        let t2_cols = pick_random_columns(t2_schema.fields.len(), req_k2, 2, 6, &mut rng);
        let r_scan = LogicalPlan::Scan {
            table: t2_name.to_string(),
            columns: t2_cols.clone(),
            table_schema: t2_schema.clone(),
        };

        // Find position of k1 in left output columns and k2 in right output columns
        let l_k_pos = t1_cols
            .iter()
            .position(|&c| Some(c) == req_k1)
            .expect("k1 in t1_cols");
        let r_k_pos = t2_cols
            .iter()
            .position(|&c| Some(c) == req_k2)
            .expect("k2 in t2_cols");

        let on = vec![(Expr::Column(l_k_pos), Expr::Column(r_k_pos))];

        let mut residual = None;
        if has_residual {
            // Find compatible columns between left and right for residual filter
            let l_schema = plan.schema()?;
            let r_schema = r_scan.schema()?;
            let mut compatible_pairs = Vec::new();

            for (l_idx, lf) in l_schema.fields.iter().enumerate() {
                for (r_idx, rf) in r_schema.fields.iter().enumerate() {
                    if lf.dtype == rf.dtype && !matches!(lf.dtype, DataType::Bool) {
                        compatible_pairs.push((l_idx, r_idx, lf.dtype));
                    }
                }
            }

            if !compatible_pairs.is_empty() {
                let (l_idx, r_idx, dtype) =
                    compatible_pairs[rng.gen_range(0..compatible_pairs.len())];
                let op = if matches!(dtype, DataType::Utf8) {
                    BinOp::NotEq
                } else {
                    match rng.gen_range(0..4) {
                        0 => BinOp::Lt,
                        1 => BinOp::LtEq,
                        2 => BinOp::Gt,
                        _ => BinOp::GtEq,
                    }
                };
                residual = Some(Expr::Binary {
                    op,
                    left: Box::new(Expr::Column(l_idx)),
                    right: Box::new(Expr::Column(l_schema.fields.len() + r_idx)),
                });
            }
        }

        plan = LogicalPlan::Join {
            left: Box::new(plan),
            right: Box::new(r_scan),
            join_type,
            on,
            filter: residual,
        };
        shape_parts.push(format!("Join({}, {:?})", t2_name, join_type));
    }

    // 6. Optional Project
    let wants_project = rng.gen_bool(0.5);
    if wants_project {
        droppable_pieces.push("project".to_string());
    }

    if wants_project && !drop_mask.drop_project {
        let cur_schema = plan.schema()?;
        let num_in_cols = cur_schema.fields.len();
        let target_out_cols = rng.gen_range(2..=num_in_cols.clamp(2, 6));

        let mut exprs = Vec::new();
        // Pass through some columns
        for i in 0..target_out_cols.min(num_in_cols) {
            exprs.push((Expr::Column(i), cur_schema.fields[i].name.clone()));
        }

        // Add 1 or 2 computed expressions if possible
        let mut computed_added = 0;

        // Try arithmetic on numeric/decimal columns
        let num_dec_cols: Vec<(usize, &Field)> = cur_schema
            .fields
            .iter()
            .enumerate()
            .filter(|(_, f)| {
                matches!(
                    f.dtype,
                    DataType::Int32 | DataType::Int64 | DataType::Decimal { .. }
                )
            })
            .collect();

        if num_dec_cols.len() >= 2 && rng.gen_bool(0.7) {
            let idx1 = num_dec_cols[rng.gen_range(0..num_dec_cols.len())].0;
            let idx2 = num_dec_cols[rng.gen_range(0..num_dec_cols.len())].0;
            let op = match rng.gen_range(0..3) {
                0 => BinOp::Add,
                1 => BinOp::Sub,
                _ => BinOp::Mul,
            };
            exprs.push((
                Expr::Binary {
                    op,
                    left: Box::new(Expr::Column(idx1)),
                    right: Box::new(Expr::Column(idx2)),
                },
                format!("calc_{}", computed_added),
            ));
            computed_added += 1;
        }

        // Try EXTRACT(YEAR) on date column
        let date_cols: Vec<usize> = cur_schema
            .fields
            .iter()
            .enumerate()
            .filter(|(_, f)| matches!(f.dtype, DataType::Date32))
            .map(|(i, _)| i)
            .collect();

        if !date_cols.is_empty() && rng.gen_bool(0.5) {
            let d_idx = date_cols[rng.gen_range(0..date_cols.len())];
            exprs.push((
                Expr::Extract {
                    field: DateField::Year,
                    expr: Box::new(Expr::Column(d_idx)),
                },
                format!("year_{}", computed_added),
            ));
            computed_added += 1;
        }

        // Try SUBSTRING on Utf8 column
        let str_cols: Vec<usize> = cur_schema
            .fields
            .iter()
            .enumerate()
            .filter(|(_, f)| matches!(f.dtype, DataType::Utf8))
            .map(|(i, _)| i)
            .collect();

        if !str_cols.is_empty() && rng.gen_bool(0.5) {
            let s_idx = str_cols[rng.gen_range(0..str_cols.len())];
            exprs.push((
                Expr::Substring {
                    expr: Box::new(Expr::Column(s_idx)),
                    start: 1,
                    len: 5,
                },
                format!("sub_{}", computed_added),
            ));
        }

        if !exprs.is_empty() {
            plan = LogicalPlan::Project {
                input: Box::new(plan),
                exprs,
            };
            shape_parts.push("Project".to_string());
        }
    }

    // 7. Optional Aggregate
    let wants_agg = rng.gen_bool(0.5);
    if wants_agg {
        droppable_pieces.push("agg".to_string());
    }

    if wants_agg && !drop_mask.drop_agg {
        let cur_schema = plan.schema()?;
        let num_in = cur_schema.fields.len();

        // Find candidate group keys: low cardinality fields
        let mut group_key_candidates = Vec::new();
        for (idx, field) in cur_schema.fields.iter().enumerate() {
            let name = field.name.to_lowercase();
            if name.contains("flag")
                || name.contains("status")
                || name.contains("priority")
                || name.contains("nationkey")
                || name.contains("regionkey")
                || name.contains("mktsegment")
                || name.contains("year")
            {
                group_key_candidates.push(idx);
            }
        }
        if group_key_candidates.is_empty() && num_in > 0 {
            // fallback to first column
            group_key_candidates.push(0);
        }

        let num_group_keys = rng.gen_range(0..=2.min(group_key_candidates.len()));
        let mut group_by = Vec::new();
        for &col_idx in group_key_candidates.iter().take(num_group_keys) {
            group_by.push((
                Expr::Column(col_idx),
                cur_schema.fields[col_idx].name.clone(),
            ));
        }

        // Build 1-4 aggregate expressions
        let num_aggs = rng.gen_range(1..=4);
        let mut aggs = Vec::new();

        // Numeric columns for sum/avg
        let num_cols: Vec<usize> = cur_schema
            .fields
            .iter()
            .enumerate()
            .filter(|(_, f)| {
                matches!(
                    f.dtype,
                    DataType::Int32
                        | DataType::Int64
                        | DataType::Decimal { .. }
                        | DataType::Float64
                )
            })
            .map(|(i, _)| i)
            .collect();

        for a_idx in 0..num_aggs {
            let func_choice = rng.gen_range(0..7);
            let (func, arg) = match func_choice {
                0 => (AggFunc::CountStar, None),
                1 => {
                    let col_idx = rng.gen_range(0..num_in);
                    (AggFunc::Count, Some(Expr::Column(col_idx)))
                }
                2 => {
                    let col_idx = rng.gen_range(0..num_in);
                    (AggFunc::CountDistinct, Some(Expr::Column(col_idx)))
                }
                3 => {
                    if !num_cols.is_empty() {
                        let c = num_cols[rng.gen_range(0..num_cols.len())];
                        (AggFunc::Sum, Some(Expr::Column(c)))
                    } else {
                        (AggFunc::CountStar, None)
                    }
                }
                4 => {
                    if !num_cols.is_empty() {
                        let c = num_cols[rng.gen_range(0..num_cols.len())];
                        (AggFunc::Avg, Some(Expr::Column(c)))
                    } else {
                        (AggFunc::CountStar, None)
                    }
                }
                5 => {
                    let col_idx = rng.gen_range(0..num_in);
                    (AggFunc::Min, Some(Expr::Column(col_idx)))
                }
                _ => {
                    let col_idx = rng.gen_range(0..num_in);
                    (AggFunc::Max, Some(Expr::Column(col_idx)))
                }
            };

            aggs.push((AggExpr { func, arg }, format!("agg_{}", a_idx)));
        }

        plan = LogicalPlan::Aggregate {
            input: Box::new(plan),
            group_by,
            aggs,
        };
        shape_parts.push(format!("Agg({}, {})", num_group_keys, num_aggs));
    }

    // 8. Always finish with Sort on all output columns
    let out_schema = plan.schema()?;
    let mut sort_keys = Vec::new();
    for i in 0..out_schema.fields.len() {
        sort_keys.push(SortKey {
            expr: Expr::Column(i),
            asc: true,
            nulls_first: false,
        });
    }
    plan = LogicalPlan::Sort {
        input: Box::new(plan),
        keys: sort_keys,
    };
    shape_parts.push("Sort".to_string());

    // 9. Sometimes a Limit
    let wants_limit = rng.gen_bool(0.5);
    if wants_limit {
        droppable_pieces.push("limit".to_string());
    }

    if wants_limit && !drop_mask.drop_limit {
        let n = rng.gen_range(1..=100);
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            n,
        };
        shape_parts.push(format!("Limit({})", n));
    }

    let sql = plan_to_sql(&plan)?;
    let shape = shape_parts.join(" -> ");

    Ok(FuzzPlan {
        seed,
        plan,
        sql,
        shape,
        droppable_pieces,
    })
}

fn generate_conjunct(
    col_pos: usize,
    field: &Field,
    table_name: &str,
    actual_col_idx: usize,
    sampler: &ValueSampler,
    rng: &mut StdRng,
) -> Expr {
    let col_expr = Expr::Column(col_pos);

    match field.dtype {
        DataType::Date32 => {
            let choice = rng.gen_range(0..3);
            match choice {
                0 => {
                    // Comparison
                    let op = match rng.gen_range(0..6) {
                        0 => BinOp::Eq,
                        1 => BinOp::NotEq,
                        2 => BinOp::Lt,
                        3 => BinOp::LtEq,
                        4 => BinOp::Gt,
                        _ => BinOp::GtEq,
                    };
                    let lit = sampler.sample_literal(table_name, actual_col_idx, field.dtype, rng);
                    Expr::Binary {
                        op,
                        left: Box::new(col_expr),
                        right: Box::new(Expr::Literal(lit)),
                    }
                }
                1 => {
                    // BETWEEN
                    let lit1 = sampler.sample_literal(table_name, actual_col_idx, field.dtype, rng);
                    let lit2 = sampler.sample_literal(table_name, actual_col_idx, field.dtype, rng);
                    let (min_l, max_l) = match (&lit1, &lit2) {
                        (Scalar::Date32(d1), Scalar::Date32(d2)) => {
                            if d1 <= d2 {
                                (lit1, lit2)
                            } else {
                                (lit2, lit1)
                            }
                        }
                        _ => (lit1, lit2),
                    };
                    Expr::Binary {
                        op: BinOp::And,
                        left: Box::new(Expr::Binary {
                            op: BinOp::GtEq,
                            left: Box::new(col_expr.clone()),
                            right: Box::new(Expr::Literal(min_l)),
                        }),
                        right: Box::new(Expr::Binary {
                            op: BinOp::LtEq,
                            left: Box::new(col_expr),
                            right: Box::new(Expr::Literal(max_l)),
                        }),
                    }
                }
                _ => {
                    // IN list
                    let list = sampler.sample_distinct_values(
                        table_name,
                        actual_col_idx,
                        rng.gen_range(1..=4),
                        rng,
                    );
                    Expr::InList {
                        expr: Box::new(col_expr),
                        list,
                        negated: false,
                    }
                }
            }
        }
        DataType::Decimal { .. } => {
            let choice = rng.gen_range(0..3);
            match choice {
                0 => {
                    let op = match rng.gen_range(0..6) {
                        0 => BinOp::Eq,
                        1 => BinOp::NotEq,
                        2 => BinOp::Lt,
                        3 => BinOp::LtEq,
                        4 => BinOp::Gt,
                        _ => BinOp::GtEq,
                    };
                    let lit = sampler.sample_literal(table_name, actual_col_idx, field.dtype, rng);
                    Expr::Binary {
                        op,
                        left: Box::new(col_expr),
                        right: Box::new(Expr::Literal(lit)),
                    }
                }
                1 => {
                    // BETWEEN
                    let lit1 = sampler.sample_literal(table_name, actual_col_idx, field.dtype, rng);
                    let lit2 = sampler.sample_literal(table_name, actual_col_idx, field.dtype, rng);
                    let (min_l, max_l) = match (&lit1, &lit2) {
                        (Scalar::Decimal(v1, _), Scalar::Decimal(v2, _)) => {
                            if v1 <= v2 {
                                (lit1, lit2)
                            } else {
                                (lit2, lit1)
                            }
                        }
                        _ => (lit1, lit2),
                    };
                    Expr::Binary {
                        op: BinOp::And,
                        left: Box::new(Expr::Binary {
                            op: BinOp::GtEq,
                            left: Box::new(col_expr.clone()),
                            right: Box::new(Expr::Literal(min_l)),
                        }),
                        right: Box::new(Expr::Binary {
                            op: BinOp::LtEq,
                            left: Box::new(col_expr),
                            right: Box::new(Expr::Literal(max_l)),
                        }),
                    }
                }
                _ => {
                    let list = sampler.sample_distinct_values(
                        table_name,
                        actual_col_idx,
                        rng.gen_range(1..=4),
                        rng,
                    );
                    Expr::InList {
                        expr: Box::new(col_expr),
                        list,
                        negated: false,
                    }
                }
            }
        }
        DataType::Utf8 => {
            let choice = rng.gen_range(0..3);
            match choice {
                0 => {
                    // Equality or inequality
                    let op = if rng.gen_bool(0.7) {
                        BinOp::Eq
                    } else {
                        BinOp::NotEq
                    };
                    let lit = sampler.sample_literal(table_name, actual_col_idx, field.dtype, rng);
                    Expr::Binary {
                        op,
                        left: Box::new(col_expr),
                        right: Box::new(Expr::Literal(lit)),
                    }
                }
                1 => {
                    // LIKE with prefix, suffix or infix
                    let lit = sampler.sample_literal(table_name, actual_col_idx, field.dtype, rng);
                    let st = match lit {
                        Scalar::Utf8(s) => s,
                        _ => "SAMPLE".to_string(),
                    };
                    let chars: Vec<char> = st.chars().collect();
                    let pattern = if chars.len() <= 2 {
                        format!("%{}%", st)
                    } else {
                        let mid = chars.len() / 2;
                        match rng.gen_range(0..3) {
                            0 => format!("{}%", chars[..mid].iter().collect::<String>()),
                            1 => format!("%{}", chars[mid..].iter().collect::<String>()),
                            _ => {
                                let start = 1.min(chars.len() - 1);
                                let end = (start + mid).min(chars.len());
                                format!("%{}%", chars[start..end].iter().collect::<String>())
                            }
                        }
                    };
                    Expr::Like {
                        expr: Box::new(col_expr),
                        pattern,
                        negated: false,
                    }
                }
                _ => {
                    let list = sampler.sample_distinct_values(
                        table_name,
                        actual_col_idx,
                        rng.gen_range(1..=4),
                        rng,
                    );
                    Expr::InList {
                        expr: Box::new(col_expr),
                        list,
                        negated: false,
                    }
                }
            }
        }
        _ => {
            // Int32, Int64, Bool
            let choice = rng.gen_range(0..2);
            if choice == 0 {
                let op = match rng.gen_range(0..6) {
                    0 => BinOp::Eq,
                    1 => BinOp::NotEq,
                    2 => BinOp::Lt,
                    3 => BinOp::LtEq,
                    4 => BinOp::Gt,
                    _ => BinOp::GtEq,
                };
                let lit = sampler.sample_literal(table_name, actual_col_idx, field.dtype, rng);
                Expr::Binary {
                    op,
                    left: Box::new(col_expr),
                    right: Box::new(Expr::Literal(lit)),
                }
            } else {
                let list = sampler.sample_distinct_values(
                    table_name,
                    actual_col_idx,
                    rng.gen_range(1..=4),
                    rng,
                );
                Expr::InList {
                    expr: Box::new(col_expr),
                    list,
                    negated: false,
                }
            }
        }
    }
}
