mod kv;
pub mod raft;

use super::schema::Catalog;
use super::types::{Expression, Row, Value};
use crate::error::{Error, Result};

/// An SQL transaction
pub trait Transaction: Catalog {
    /// The transaction ID
    fn id(&self) -> u64;
    /// The transaction mode
    fn mode(&self) -> Mode;
    /// Commits the transaction
    fn commit(self) -> Result<()>;
    /// Rolls back the transaction
    fn rollback(self) -> Result<()>;

    /// Creates a new table row
    fn create(&mut self, table: &str, row: Row) -> Result<()>;
    /// Deletes a table row
    fn delete(&mut self, table: &str, id: &Value) -> Result<()>;
    /// Reads a table row, if it exists
    fn read(&self, table: &str, id: &Value) -> Result<Option<Row>>;
    /// Reads an index entry, if it exists
    fn read_index(&self, table: &str, column: &str, value: &Value) -> Result<HashSet<Value>>;
    /// Scans a table's rows
    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<Scan>;
    /// Scans a column's index entries
    fn scan_index(&self, table: &str, column: &str) -> Result<IndexScan>;
    /// Updates a table row
    fn update(&mut self, table: &str, id: &Value, row: Row) -> Result<()>;
}
