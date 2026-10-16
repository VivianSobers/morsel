use std::collections::HashMap;

use morsel_core::{DataType, Field, Schema};
use morsel_plan::{JoinType, LogicalPlan, SchemaProvider};
use morsel_sql::plan_sql;

struct SubqueryMockCatalog {
    schemas: HashMap<String, Schema>,
}

impl SubqueryMockCatalog {
    fn new() -> Self {
        let mut schemas = HashMap::new();
        schemas.insert(
            "orders".to_string(),
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
                    name: "o_orderdate".into(),
                    dtype: DataType::Date32,
                    nullable: false,
                },
                Field {
                    name: "o_orderpriority".into(),
                    dtype: DataType::Utf8,
                    nullable: false,
                },
            ]),
        );
        schemas.insert(
            "lineitem".to_string(),
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
            ]),
        );
        schemas.insert(
            "customer".to_string(),
            Schema::new(vec![
                Field {
                    name: "c_custkey".into(),
                    dtype: DataType::Int64,
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
                    name: "c_phone".into(),
                    dtype: DataType::Utf8,
                    nullable: false,
                },
            ]),
        );
        schemas.insert(
            "supplier".to_string(),
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
            ]),
        );
        schemas.insert(
            "part".to_string(),
            Schema::new(vec![
                Field {
                    name: "p_partkey".into(),
                    dtype: DataType::Int64,
                    nullable: false,
                },
                Field {
                    name: "p_brand".into(),
                    dtype: DataType::Utf8,
                    nullable: false,
                },
                Field {
                    name: "p_container".into(),
                    dtype: DataType::Utf8,
                    nullable: false,
                },
            ]),
        );
        Self { schemas }
    }
}

impl SchemaProvider for SubqueryMockCatalog {
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

fn contains_join_type(plan: &LogicalPlan, expected: JoinType) -> bool {
    match plan {
        LogicalPlan::Join {
            join_type,
            left,
            right,
            ..
        } => {
            *join_type == expected
                || contains_join_type(left, expected)
                || contains_join_type(right, expected)
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => contains_join_type(input, expected),
        LogicalPlan::CrossJoin { left, right } => {
            contains_join_type(left, expected) || contains_join_type(right, expected)
        }
        LogicalPlan::Scan { .. } => false,
    }
}

fn contains_cross_join(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::CrossJoin { .. } => true,
        LogicalPlan::Join { left, right, .. } => {
            contains_cross_join(left) || contains_cross_join(right)
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => contains_cross_join(input),
        LogicalPlan::Scan { .. } => false,
    }
}

#[test]
fn exists_becomes_semi() {
    let cat = SubqueryMockCatalog::new();
    let sql = "SELECT o_orderpriority, count(*) AS order_count FROM orders WHERE o_orderdate >= date '1993-07-01' AND o_orderdate < date '1993-10-01' AND EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate) GROUP BY o_orderpriority ORDER BY o_orderpriority";
    let plan = plan_sql(sql, &cat).expect("plan q4 exists");
    assert!(
        contains_join_type(&plan, JoinType::Semi),
        "Q4 EXISTS should produce a Semi join in plan: {:?}",
        plan
    );
}

#[test]
fn not_exists_with_residual_becomes_anti() {
    let cat = SubqueryMockCatalog::new();
    let sql = "SELECT s_name FROM supplier, lineitem l1 WHERE s_suppkey = l1.l_suppkey AND NOT EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey) ORDER BY s_name";
    let plan = plan_sql(sql, &cat).expect("plan q21 not exists");
    assert!(
        contains_join_type(&plan, JoinType::Anti),
        "NOT EXISTS should produce an Anti join in plan: {:?}",
        plan
    );
}

#[test]
fn in_subquery_becomes_semi() {
    let cat = SubqueryMockCatalog::new();
    let sql = "SELECT o_orderkey FROM orders WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 300)";
    let plan = plan_sql(sql, &cat).expect("plan q18 in subquery");
    assert!(
        contains_join_type(&plan, JoinType::Semi),
        "IN (subquery) should produce a Semi join in plan: {:?}",
        plan
    );
}

#[test]
fn scalar_uncorrelated_becomes_cross() {
    let cat = SubqueryMockCatalog::new();
    let sql =
        "SELECT c_custkey FROM customer WHERE c_acctbal > (SELECT avg(c_acctbal) FROM customer)";
    let plan = plan_sql(sql, &cat).expect("plan uncorrelated scalar subquery");
    assert!(
        contains_cross_join(&plan),
        "uncorrelated scalar subquery should produce a CrossJoin: {:?}",
        plan
    );
}

#[test]
fn correlated_scalar_becomes_agg_join() {
    let cat = SubqueryMockCatalog::new();
    let sql = "SELECT l_extendedprice FROM lineitem WHERE l_quantity < (SELECT 0.2 * avg(l_quantity) FROM lineitem l2 WHERE l2.l_partkey = lineitem.l_partkey)";
    let plan = plan_sql(sql, &cat).expect("plan q17 correlated scalar subquery");
    // Should decorrelate into Join (Inner or Left) with an Aggregate input grouped by correlation key
    fn has_join_with_agg(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Join { left, right, .. } => {
                matches!(
                    **right,
                    LogicalPlan::Aggregate { .. } | LogicalPlan::Project { .. }
                ) || matches!(
                    **left,
                    LogicalPlan::Aggregate { .. } | LogicalPlan::Project { .. }
                ) || has_join_with_agg(left)
                    || has_join_with_agg(right)
            }
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => has_join_with_agg(input),
            _ => false,
        }
    }
    assert!(
        has_join_with_agg(&plan),
        "correlated scalar subquery should produce a Join with Aggregate: {:?}",
        plan
    );
}

#[test]
fn test_correlated_scalar_multiple_keys_decorrelates() {
    let mut cat = SubqueryMockCatalog::new();
    cat.schemas.insert(
        "partsupp".to_string(),
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
        ]),
    );
    let sql = "SELECT ps_suppkey FROM partsupp WHERE ps_availqty > (SELECT 0.5 * sum(l_quantity) FROM lineitem WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey)";
    let plan = plan_sql(sql, &cat).expect("plan q20 correlated scalar subquery");

    fn find_join_key_count(plan: &LogicalPlan) -> Option<usize> {
        match plan {
            LogicalPlan::Join {
                on, left, right, ..
            } => {
                if !on.is_empty() {
                    Some(on.len())
                } else {
                    find_join_key_count(left).or_else(|| find_join_key_count(right))
                }
            }
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_join_key_count(input),
            _ => None,
        }
    }
    assert_eq!(
        find_join_key_count(&plan),
        Some(2),
        "should join on both correlation keys: {:?}",
        plan
    );
}
