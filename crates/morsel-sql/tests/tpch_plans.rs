use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use morsel_core::{DataType, Field, Schema};
use morsel_plan::{JoinType, LogicalPlan, SchemaProvider};
use morsel_sql::{optimize, plan_sql};

struct MockCatalog {
    schemas: HashMap<String, Schema>,
}

impl SchemaProvider for MockCatalog {
    fn table_schema(&self, name: &str) -> Option<Schema> {
        self.schemas.get(name).cloned().or_else(|| {
            for (k, v) in &self.schemas {
                if k.eq_ignore_ascii_case(name) {
                    return Some(v.clone());
                }
            }
            None
        })
    }
}

fn tpch_schemas() -> MockCatalog {
    let mut schemas = HashMap::new();
    schemas.insert(
        "region".into(),
        Schema::new(vec![
            Field {
                name: "r_regionkey".into(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "r_name".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "r_comment".into(),
                dtype: DataType::Utf8,
                nullable: true,
            },
        ]),
    );
    schemas.insert(
        "nation".into(),
        Schema::new(vec![
            Field {
                name: "n_nationkey".into(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "n_name".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "n_regionkey".into(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "n_comment".into(),
                dtype: DataType::Utf8,
                nullable: true,
            },
        ]),
    );
    schemas.insert(
        "supplier".into(),
        Schema::new(vec![
            Field {
                name: "s_suppkey".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            Field {
                name: "s_name".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "s_address".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "s_nationkey".into(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "s_phone".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "s_acctbal".into(),
                dtype: DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                nullable: false,
            },
            Field {
                name: "s_comment".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
        ]),
    );
    schemas.insert(
        "customer".into(),
        Schema::new(vec![
            Field {
                name: "c_custkey".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            Field {
                name: "c_name".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "c_address".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "c_nationkey".into(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "c_phone".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "c_acctbal".into(),
                dtype: DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                nullable: false,
            },
            Field {
                name: "c_mktsegment".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "c_comment".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
        ]),
    );
    schemas.insert(
        "part".into(),
        Schema::new(vec![
            Field {
                name: "p_partkey".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            Field {
                name: "p_name".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "p_mfgr".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "p_brand".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "p_type".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "p_size".into(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "p_container".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "p_retailprice".into(),
                dtype: DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                nullable: false,
            },
            Field {
                name: "p_comment".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
        ]),
    );
    schemas.insert(
        "partsupp".into(),
        Schema::new(vec![
            Field {
                name: "ps_partkey".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            Field {
                name: "ps_suppkey".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            Field {
                name: "ps_availqty".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            Field {
                name: "ps_supplycost".into(),
                dtype: DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                nullable: false,
            },
            Field {
                name: "ps_comment".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
        ]),
    );
    schemas.insert(
        "orders".into(),
        Schema::new(vec![
            Field {
                name: "o_orderkey".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            Field {
                name: "o_custkey".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            Field {
                name: "o_orderstatus".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "o_totalprice".into(),
                dtype: DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                nullable: false,
            },
            Field {
                name: "o_orderdate".into(),
                dtype: DataType::Date32,
                nullable: false,
            },
            Field {
                name: "o_orderpriority".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "o_clerk".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "o_shippriority".into(),
                dtype: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "o_comment".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
        ]),
    );
    schemas.insert(
        "lineitem".into(),
        Schema::new(vec![
            Field {
                name: "l_orderkey".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            Field {
                name: "l_partkey".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            Field {
                name: "l_suppkey".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            Field {
                name: "l_linenumber".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            Field {
                name: "l_quantity".into(),
                dtype: DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                nullable: false,
            },
            Field {
                name: "l_extendedprice".into(),
                dtype: DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                nullable: false,
            },
            Field {
                name: "l_discount".into(),
                dtype: DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                nullable: false,
            },
            Field {
                name: "l_tax".into(),
                dtype: DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
                nullable: false,
            },
            Field {
                name: "l_returnflag".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "l_linestatus".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "l_shipdate".into(),
                dtype: DataType::Date32,
                nullable: false,
            },
            Field {
                name: "l_commitdate".into(),
                dtype: DataType::Date32,
                nullable: false,
            },
            Field {
                name: "l_receiptdate".into(),
                dtype: DataType::Date32,
                nullable: false,
            },
            Field {
                name: "l_shipinstruct".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "l_shipmode".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "l_comment".into(),
                dtype: DataType::Utf8,
                nullable: false,
            },
        ]),
    );
    MockCatalog { schemas }
}

fn tpch_stats() -> HashMap<String, usize> {
    let mut stats = HashMap::new();
    stats.insert("region".into(), 5);
    stats.insert("nation".into(), 25);
    stats.insert("supplier".into(), 10000);
    stats.insert("customer".into(), 150000);
    stats.insert("part".into(), 200000);
    stats.insert("partsupp".into(), 800000);
    stats.insert("orders".into(), 1500000);
    stats.insert("lineitem".into(), 6001215);
    stats
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn has_cross_join(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::CrossJoin { .. } => true,
        LogicalPlan::Scan { .. } => false,
        LogicalPlan::Filter { input, .. } => has_cross_join(input),
        LogicalPlan::Project { input, .. } => has_cross_join(input),
        LogicalPlan::Aggregate { input, .. } => has_cross_join(input),
        LogicalPlan::Sort { input, .. } => has_cross_join(input),
        LogicalPlan::Limit { input, .. } => has_cross_join(input),
        LogicalPlan::Join { left, right, .. } => has_cross_join(left) || has_cross_join(right),
    }
}

fn scans_have_filter_ancestor(plan: &LogicalPlan) -> bool {
    // In Q3, all base scans should have a Filter directly on top of them
    // (pushed down through joins)
    fn check(plan: &LogicalPlan, direct_filter: bool) -> bool {
        match plan {
            LogicalPlan::Scan { .. } => direct_filter,
            LogicalPlan::Filter { input, .. } => check(input, true),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Aggregate { input, .. } => check(input, false),
            LogicalPlan::Join { left, right, .. } | LogicalPlan::CrossJoin { left, right } => {
                check(left, false) && check(right, false)
            }
        }
    }
    check(plan, false)
}

#[test]
fn all_22_queries_plan() {
    let catalog = tpch_schemas();
    let stats = tpch_stats();
    let root = repo_root();

    for q in 1..=22 {
        let sql_path = root.join("queries/tpch").join(format!("q{:02}.sql", q));
        let sql = fs::read_to_string(&sql_path)
            .unwrap_or_else(|e| panic!("failed to read {:?}: {}", sql_path, e));

        let plan =
            plan_sql(&sql, &catalog).unwrap_or_else(|e| panic!("failed to plan Q{:02}: {}", q, e));

        let optimized = optimize(plan, &stats)
            .unwrap_or_else(|e| panic!("failed to optimize Q{:02}: {}", q, e));

        let plan_schema = optimized
            .schema()
            .unwrap_or_else(|e| panic!("failed to get schema for Q{:02}: {}", q, e));

        let ref_path = root.join("bench/refs/sf1").join(format!("q{:02}.csv", q));
        let ref_content = fs::read_to_string(&ref_path)
            .unwrap_or_else(|e| panic!("failed to read {:?}: {}", ref_path, e));
        let header = ref_content.lines().next().expect("ref csv header");
        let expected_col_count = header.split(',').count();

        assert_eq!(
            plan_schema.fields.len(),
            expected_col_count,
            "Q{:02} col count mismatch: plan has {}, ref has {}",
            q,
            plan_schema.fields.len(),
            expected_col_count
        );
    }
}

#[test]
fn no_cross_join_left_in_q5_q7_q8_q9() {
    let catalog = tpch_schemas();
    let stats = tpch_stats();
    let root = repo_root();

    for q in [5, 7, 8, 9] {
        let sql_path = root.join("queries/tpch").join(format!("q{:02}.sql", q));
        let sql = fs::read_to_string(&sql_path).unwrap();
        let plan = plan_sql(&sql, &catalog).unwrap();
        let optimized = optimize(plan, &stats).unwrap();
        assert!(
            !has_cross_join(&optimized),
            "Q{:02} contains CrossJoin after optimization: {:#?}",
            q,
            optimized
        );
    }
}

#[test]
fn filters_pushed_to_scans_in_q3() {
    let catalog = tpch_schemas();
    let stats = tpch_stats();
    let root = repo_root();

    let sql_path = root.join("queries/tpch/q03.sql");
    let sql = fs::read_to_string(&sql_path).unwrap();
    let plan = plan_sql(&sql, &catalog).unwrap();
    let optimized = optimize(plan, &stats).unwrap();

    assert!(
        scans_have_filter_ancestor(&optimized),
        "Q03 scans do not all have direct filter ancestors: {:#?}",
        optimized
    );
}

fn is_base_scan(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Scan { .. } => true,
        LogicalPlan::Filter { input, .. } => is_base_scan(input),
        LogicalPlan::Project { input, .. } => is_base_scan(input),
        _ => false,
    }
}

fn assert_no_base_cross_join(plan: &LogicalPlan, q: usize) {
    match plan {
        LogicalPlan::CrossJoin { left, right } => {
            assert!(
                !(is_base_scan(left) && is_base_scan(right)),
                "Q{:02} contains CrossJoin with base-table Scan on both sides:\nleft: {:#?}\nright: {:#?}",
                q,
                left,
                right
            );
            assert_no_base_cross_join(left, q);
            assert_no_base_cross_join(right, q);
        }
        LogicalPlan::Scan { .. } => {}
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => {
            assert_no_base_cross_join(input, q);
        }
        LogicalPlan::Join { left, right, .. } => {
            assert_no_base_cross_join(left, q);
            assert_no_base_cross_join(right, q);
        }
    }
}

#[test]
fn no_cross_join_has_base_scans_on_both_sides() {
    let catalog = tpch_schemas();
    let stats = tpch_stats();
    let root = repo_root();

    for q in 1..=22 {
        let sql_path = root.join("queries/tpch").join(format!("q{:02}.sql", q));
        let sql = fs::read_to_string(&sql_path)
            .unwrap_or_else(|e| panic!("failed to read {:?}: {}", sql_path, e));
        let plan =
            plan_sql(&sql, &catalog).unwrap_or_else(|e| panic!("failed to plan Q{:02}: {}", q, e));
        let optimized = optimize(plan, &stats)
            .unwrap_or_else(|e| panic!("failed to optimize Q{:02}: {}", q, e));
        assert_no_base_cross_join(&optimized, q);
    }
}

#[test]
fn test_optimizer_steps_preserve_schema_and_column_ranges() {
    let catalog = tpch_schemas();
    let stats = tpch_stats();
    let root = repo_root();

    for q in 1..=22 {
        let sql_path = root.join("queries/tpch").join(format!("q{:02}.sql", q));
        let sql = fs::read_to_string(&sql_path)
            .unwrap_or_else(|e| panic!("failed to read {:?}: {}", sql_path, e));
        let raw_plan =
            plan_sql(&sql, &catalog).unwrap_or_else(|e| panic!("failed to plan Q{:02}: {}", q, e));
        let raw_schema = raw_plan.schema().unwrap();
        morsel_sql::optimize::validate_plan_columns(&raw_plan).unwrap();

        // 1. Fold constants
        let folded = morsel_sql::optimize::fold_constants(raw_plan).unwrap();
        morsel_sql::optimize::check_optimizer_step("fold_constants", &folded, &raw_schema).unwrap();

        // 2. Factor disjunctions
        let factored = morsel_sql::optimize::factor_disjunctions(folded).unwrap();
        morsel_sql::optimize::check_optimizer_step("factor_disjunctions", &factored, &raw_schema)
            .unwrap();

        // 3. Push predicates
        let pushed = morsel_sql::optimize::push_predicates(factored).unwrap();
        morsel_sql::optimize::check_optimizer_step("push_predicates", &pushed, &raw_schema)
            .unwrap();

        // 4. Order joins
        let ordered = morsel_sql::optimize::order_joins(pushed, &stats).unwrap();
        morsel_sql::optimize::check_optimizer_step("order_joins", &ordered, &raw_schema).unwrap();

        // 5. Prune projections
        let pruned = morsel_sql::optimize::prune_projections(ordered).unwrap();
        morsel_sql::optimize::check_optimizer_step("prune_projections", &pruned, &raw_schema)
            .unwrap();
    }
}

#[test]
fn test_or_factoring_q19_plan_level() {
    let catalog = tpch_schemas();
    let stats = tpch_stats();
    let root = repo_root();

    let sql_path = root.join("queries/tpch/q19.sql");
    let sql = fs::read_to_string(&sql_path).expect("read q19.sql");
    let raw_plan = plan_sql(&sql, &catalog).expect("plan q19");

    // After factoring disjunctions, the filter should have the common join key factored out
    let factored = morsel_sql::optimize::factor_disjunctions(raw_plan).expect("factor q19");

    // Verify optimized plan has an Inner Join with non-empty on keys (no CrossJoin)
    let optimized = optimize(factored, &stats).expect("optimize q19");
    assert_no_base_cross_join(&optimized, 19);

    fn has_inner_join_with_keys(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Join {
                join_type,
                on,
                left,
                right,
                ..
            } => {
                (*join_type == JoinType::Inner && !on.is_empty())
                    || has_inner_join_with_keys(left)
                    || has_inner_join_with_keys(right)
            }
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => has_inner_join_with_keys(input),
            _ => false,
        }
    }

    assert!(
        has_inner_join_with_keys(&optimized),
        "optimized Q19 plan must have inner join with join keys"
    );
}

#[test]
fn test_q5_join_order_avoids_customer_supplier_nationkey_cross() {
    let catalog = tpch_schemas();
    let stats = tpch_stats();
    let root = repo_root();

    let sql_path = root.join("queries/tpch/q05.sql");
    let sql = fs::read_to_string(&sql_path).unwrap();
    let plan = plan_sql(&sql, &catalog).unwrap();
    let optimized = optimize(plan, &stats).unwrap();

    fn plan_tables(p: &LogicalPlan) -> Vec<String> {
        let mut tbls = Vec::new();
        match p {
            LogicalPlan::Scan { table, .. } => tbls.push(table.to_lowercase()),
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => tbls.extend(plan_tables(input)),
            LogicalPlan::Join { left, right, .. } | LogicalPlan::CrossJoin { left, right } => {
                tbls.extend(plan_tables(left));
                tbls.extend(plan_tables(right));
            }
        }
        tbls
    }

    fn check_joins(plan: &LogicalPlan) {
        match plan {
            LogicalPlan::Join {
                left, right, on, ..
            } => {
                let left_tables = plan_tables(left);
                let right_tables = plan_tables(right);

                // Ensure customer and supplier are never joined directly without orders present
                if (left_tables.contains(&"customer".to_string())
                    && !left_tables.contains(&"orders".to_string())
                    && right_tables.contains(&"supplier".to_string())
                    && !right_tables.contains(&"orders".to_string()))
                    || (right_tables.contains(&"customer".to_string())
                        && !right_tables.contains(&"orders".to_string())
                        && left_tables.contains(&"supplier".to_string())
                        && !left_tables.contains(&"orders".to_string()))
                {
                    panic!(
                        "Customer and supplier joined directly without orders: on={:?}",
                        on
                    );
                }

                if right_tables == vec!["customer".to_string()] {
                    assert!(
                        on.len() >= 2,
                        "Customer should join with orders (custkey) AND supplier (nationkey), but got {} on-keys: {:?}",
                        on.len(),
                        on
                    );
                }

                check_joins(left);
                check_joins(right);
            }
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => check_joins(input),
            _ => {}
        }
    }

    check_joins(&optimized);
}

#[test]
fn test_q9_and_q17_build_hash_table_on_smaller_side() {
    let catalog = tpch_schemas();
    let mut stats = HashMap::new();
    stats.insert("region".into(), 5);
    stats.insert("nation".into(), 25);
    stats.insert("supplier".into(), 100_000);
    stats.insert("customer".into(), 1_500_000);
    stats.insert("part".into(), 2_000_000);
    stats.insert("partsupp".into(), 8_000_000);
    stats.insert("orders".into(), 15_000_000);
    stats.insert("lineitem".into(), 60_000_000);

    let root = repo_root();

    fn check_build_side_not_lineitem(plan: &LogicalPlan, q: usize) {
        match plan {
            LogicalPlan::Join {
                left,
                right,
                join_type,
                ..
            } => {
                if *join_type == JoinType::Inner {
                    // Right is the build side in morsel-exec.
                    // Lineitem (60M rows) should never be the build side when joined with smaller tables.
                    if let LogicalPlan::Scan { table, .. } = right.as_ref() {
                        assert_ne!(
                            table.to_lowercase(),
                            "lineitem",
                            "Q{:02} has lineitem on the right (build) side of an inner join!",
                            q
                        );
                    }
                }
                check_build_side_not_lineitem(left, q);
                check_build_side_not_lineitem(right, q);
            }
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => {
                check_build_side_not_lineitem(input, q);
            }
            _ => {}
        }
    }

    for &q in &[9, 17] {
        let sql_path = root.join("queries/tpch").join(format!("q{:02}.sql", q));
        let sql = fs::read_to_string(&sql_path).unwrap();
        let plan = plan_sql(&sql, &catalog).unwrap();
        let optimized = optimize(plan, &stats).unwrap();
        check_build_side_not_lineitem(&optimized, q);
    }
}

#[test]
fn test_q21_projection_pruning_decorrelated_subqueries() {
    let catalog = tpch_schemas();
    let stats = tpch_stats();
    let root = repo_root();

    let sql_path = root.join("queries/tpch/q21.sql");
    let sql = fs::read_to_string(&sql_path).unwrap();
    let plan = plan_sql(&sql, &catalog).unwrap();
    let optimized = optimize(plan, &stats).unwrap();

    fn collect_scans(plan: &LogicalPlan, scans: &mut Vec<(String, Vec<usize>)>) {
        match plan {
            LogicalPlan::Scan { table, columns, .. } => {
                scans.push((table.to_lowercase(), columns.clone()));
            }
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => {
                collect_scans(input, scans);
            }
            LogicalPlan::Join { left, right, .. } | LogicalPlan::CrossJoin { left, right } => {
                collect_scans(left, scans);
                collect_scans(right, scans);
            }
        }
    }

    let mut scans = Vec::new();
    collect_scans(&optimized, &mut scans);

    for (table, cols) in &scans {
        if table == "lineitem" {
            assert!(
                cols.len() <= 4,
                "lineitem scan has {} columns ({:?}), expected <= 4",
                cols.len(),
                cols
            );
            assert!(
                *cols == vec![0, 2] || *cols == vec![0, 2, 11, 12],
                "unexpected lineitem scan columns: {:?}",
                cols
            );
        } else if table == "supplier" {
            assert_eq!(*cols, vec![0, 1, 3]);
        } else if table == "orders" {
            assert_eq!(*cols, vec![0, 2]);
        } else if table == "nation" {
            assert_eq!(*cols, vec![0, 1]);
        }
    }
    let lineitem_scans: Vec<_> = scans.iter().filter(|(t, _)| t == "lineitem").collect();
    assert_eq!(lineitem_scans.len(), 3);
}
