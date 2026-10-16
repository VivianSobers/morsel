use std::sync::Arc;

use crate::column::Column;
use crate::types::Schema;

#[derive(Clone, Debug)]
pub struct Batch {
    pub schema: std::sync::Arc<Schema>,
    pub columns: Vec<std::sync::Arc<Column>>,
    pub len: usize,
}

impl Batch {
    pub fn new(schema: Arc<Schema>, columns: Vec<Arc<Column>>) -> Batch {
        assert_eq!(
            schema.fields.len(),
            columns.len(),
            "batch column count {} does not match schema field count {}",
            columns.len(),
            schema.fields.len()
        );
        let len = if columns.is_empty() {
            0
        } else {
            let first_len = columns[0].len();
            for (i, col) in columns.iter().enumerate() {
                assert_eq!(
                    col.len(),
                    first_len,
                    "column {} length mismatch: {} != {}",
                    i,
                    col.len(),
                    first_len
                );
            }
            first_len
        };
        Batch {
            schema,
            columns,
            len,
        }
    }

    pub fn slice(&self, offset: usize, len: usize) -> Batch {
        let sliced_cols = self
            .columns
            .iter()
            .map(|col| Arc::new(col.slice(offset, len)))
            .collect();
        Batch {
            schema: Arc::clone(&self.schema),
            columns: sliced_cols,
            len,
        }
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn column(&self, i: usize) -> &Arc<Column> {
        &self.columns[i]
    }
}
