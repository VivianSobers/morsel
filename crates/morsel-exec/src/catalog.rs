use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use morsel_core::{Batch, Schema};
use morsel_plan::SchemaProvider;

use crate::parquet_load::load_parquet_dir_impl;

#[derive(Debug)]
pub struct Table {
    pub schema: Arc<Schema>,
    pub batches: Vec<Batch>,
    pub rows: usize,
}

#[derive(Debug)]
pub struct Catalog {
    pub tables: HashMap<String, Table>,
}

impl Catalog {
    pub fn get_table(&self, name: &str) -> Option<&Table> {
        if let Some(t) = self.tables.get(name) {
            return Some(t);
        }
        for (tbl_name, table) in &self.tables {
            if tbl_name.eq_ignore_ascii_case(name) {
                return Some(table);
            }
        }
        None
    }
}

impl SchemaProvider for Catalog {
    fn table_schema(&self, name: &str) -> Option<Schema> {
        self.get_table(name).map(|t| (*t.schema).clone())
    }
}

pub fn load_parquet_dir(dir: &Path, threads: usize) -> Result<Catalog> {
    load_parquet_dir_impl(dir, threads)
}
