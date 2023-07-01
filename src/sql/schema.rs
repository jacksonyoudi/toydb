use super::engine::Transaction;
use super::parser::format_ident;
use super::types::{DataType, Value};
use crate::error::{Error, Result};

use serde_derive::{Deserialize, Serialize};
use std::fmt::{self, Display};

/// The catalog stores schema information
//数据库中的“catalog”通常指存储和管理数据库、表、列、索引和其他数据库对象的元数据和模式信息的系统或组件。
// Catalog 提供了一种组织和访问元数据的方式，允许用户和应用程序查询和操作数据库结构。
// Catalog 存储的信息包括：
// 1. 数据库元数据：关于数据库的信息，包括名称、所有者、创建日期和其他属性。
// 2. 表元数据：关于表的信息，包括名称、列定义、数据类型、约束、索引和其他属性。
// 3. 列元数据：关于表中各个列的信息，包括名称、数据类型、约束和其他属性。
// 4. 索引元数据：关于在表上创建的索引的信息，包括索引列和排序方式。
// 5. 视图元数据：关于视图的信息，包括定义和关联的权限。
// 6. 函数元数据：关于数据库函数的信息，包括名称、参数、返回类型和定义。
// 7. 权限和访问控制：关于用户权限和对各个数据库对象的访问权限的信息。
// Catalog 在数据库管理系统中扮演着重要角色，因为它允许用户、管理员和应用程序了解数据库中存储的数据结构和属性。它提供了一个集中存储的元数据库，可以查询和操作以执行数据建模、查询、优化和安全管理等任务。
pub trait Catalog {
    fn create_table(&mut self, table: Table) -> Result<()>;

    fn detele_table(&mut self, table: &str) -> Result<()>;

    fn read_table(&mut self, table: &str) -> Result<Option<Table>>;

    /// Iterates over all tables
    fn scan_tables(&self) -> Result<Tables>;

    /// Reads a table, and errors if it does not exist
    fn must_read_table(&self, table: &str) -> Result<Table> {
        self.read_table(table)?
            .ok_or_else(|| Error::Value(format!("Table {} does not exist", table)))
    }

    /// Returns all references to a table, as table,column pairs.
    fn table_references(&self, table: &str, with_self: bool) -> Result<Vec<(String, Vec<String>)>> {
        Ok(self
            .scan_tables()?
            .filter(|t| with_self || t.name != table)
            .map(|t| {
                (
                    t.name,
                    t.columns
                        .iter()
                        .filter(|c| c.references.as_deref() == Some(table))
                        .map(|c| c.name.clone())
                        .collect::<Vec<_>>(),
                )
            })
            .filter(|(_, cs)| !cs.is_empty())
            .collect())
    }
}

/// A table scan iterator
pub type Tables = Box<dyn DoubleEndedIterator<Item = Table> + Send>;
/// A table schema
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    /// Creates a new table schema
    pub fn new(name: String, columns: Vec<Column>) -> Result<Self> {
        Ok(Self {
            name: name,
            columns: columns,
        })
    }

    /// Fetches a column by name
    /// iter() 方法返回一个不可变的迭代器（iterator），
    /// 该迭代器以借用（borrow）的方式访问集合中的元素。它是对集合的不可变引用进行迭代，因此集合本身保持不可变性。
    ///
    /// into_iter() 方法将集合的所有权转移（move）给迭代器，
    /// 使得迭代器可以拥有集合并消费它。这意味着在迭代过程中，集合本身将不再可用。
    pub fn get_column(&self, name: &str) -> Result<&Column> {
        self.columns.iter().find(|c| c.name == name).ok_or_else(|| {
            Error::Value(format!("Column {} not found in table {}", name, self.name))
        })
    }

    /// Fetches a column index by name
    /// 第几个位置
    pub fn get_column_index(&self, name: &str) -> Result<usize> {
        self.columns
            .iter()
            .position(|c| c.name = name.to_owned())
            .ok_or_else(|| {
                Error::Value(format!("Column {} not found in table {}", name, self.name))
            })
    }

    /// Returns the primary key column of the table
    pub fn get_primary_key(&self) -> Result<&Column> {
        self.columns
            .iter()
            .find(|c: &&Column| c.primary_primary)
            .ok_or_else(|| Error::Value(format!("Primary key not found in table {}", self.name)))
    }

    /// Returns the primary key value of a row
    pub fn get_row_key(&self, row: &[Value]) -> Result<Value> {
        row.get(
            self.columns
                .iter()
                .position(|c| c.primary_key)
                .ok_or_else(|| Error::Value("Primary key not found".into()))?,
        )
        .cloned()
        .ok_or_else(|| Error::Value("Primary key value not found for row".into()))
    }

    /// Validates the table schema
    pub fn validate(&self, txn: &mut dyn Transaction) -> Result<()> {
        if self.columns.is_empty() {
            return Err(Error::Value(format!("Table {} has no columns", self.name)));
        }
        match self.columns.iter().filter(|c| c.primary_key).count() {
            1 => {}
            0 => {
                return Err(Error::Value(format!(
                    "No primary key in table {}",
                    self.name
                )))
            }
            _ => {
                return Err(Error::Value(format!(
                    "Multiple primary keys in table {}",
                    self.name
                )))
            }
        };
        for column in &self.columns {
            column.validate(self, txn)?;
        }
        Ok(())
    }

    /// Validates a row
    pub fn validate_row(&self, row: &[Value], txn: &mut dyn Transaction) -> Result<()> {
        if row.len() != self.columns.len() {
            return Err(Error::Value(format!(
                "Invalid row size for table {}",
                self.name
            )));
        }
        let pk = self.get_row_key(row)?;
        for (column, value) in self.columns.iter().zip(row.iter()) {
            column.validate_value(self, &pk, value, txn)?;
        }
        Ok(())
    }
}

impl Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CREATE TABLE {} (\n{}\n)",
            format_ident(&self.name),
            self.columns
                .iter()
                .map(|c| format!("  {}", c))
                .collect::<Vec<String>>()
                .join(",\n")
        )
    }
}

/// A table column schema
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Column {
    /// Column name
    pub name: String,
    /// Column datatype
    pub datatype: DataType,
    /// Whether the column is a primary key
    pub primary_key: bool,
    /// Whether the column allows null values
    pub nullable: bool,
    /// The default value of the column
    pub default: Option<Value>,
    /// Whether the column should only take unique values
    pub unique: bool,
    /// The table which is referenced by this foreign key
    pub references: Option<String>,
    /// Whether the column should be indexed
    pub index: bool,
}

impl Column {
    /// Validates the column schema
    pub fn validate(&self, table: &Table, txn: &mut dyn Transaction) -> Result<()> {
        // Validate primary key
        if self.primary_key && self.nullable {
            return Err(Error::Value(format!(
                "Primary key {} cannot be nullable",
                self.name
            )));
        }
        if self.primary_key && !self.unique {
            return Err(Error::Value(format!(
                "Primary key {} must be unique",
                self.name
            )));
        }

        // Validate default value
        if let Some(default) = &self.default {
            if let Some(datatype) = default.datatype() {
                if datatype != self.datatype {
                    return Err(Error::Value(format!(
                        "Default value for column {} has datatype {}, must be {}",
                        self.name, datatype, self.datatype
                    )));
                }
            } else if !self.nullable {
                return Err(Error::Value(format!(
                    "Can't use NULL as default value for non-nullable column {}",
                    self.name
                )));
            }
        } else if self.nullable {
            return Err(Error::Value(format!(
                "Nullable column {} must have a default value",
                self.name
            )));
        }

        // Validate references
        if let Some(reference) = &self.references {
            let target = if reference == &table.name {
                table.clone()
            } else if let Some(table) = txn.read_table(reference)? {
                table
            } else {
                return Err(Error::Value(format!(
                    "Table {} referenced by column {} does not exist",
                    reference, self.name
                )));
            };
            if self.datatype != target.get_primary_key()?.datatype {
                return Err(Error::Value(format!(
                    "Can't reference {} primary key of table {} from {} column {}",
                    target.get_primary_key()?.datatype,
                    target.name,
                    self.datatype,
                    self.name
                )));
            }
        }

        Ok(())
    }

    /// Validates a column value
    pub fn validate_value(
        &self,
        table: &Table,
        pk: &Value,
        value: &Value,
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        // Validate datatype
        match value.datatype() {
            None if self.nullable => Ok(()),
            None => Err(Error::Value(format!(
                "NULL value not allowed for column {}",
                self.name
            ))),
            Some(ref datatype) if datatype != &self.datatype => Err(Error::Value(format!(
                "Invalid datatype {} for {} column {}",
                datatype, self.datatype, self.name
            ))),
            _ => Ok(()),
        }?;

        // Validate value
        match value {
            Value::String(s) if s.len() > 1024 => Err(Error::Value(
                "Strings cannot be more than 1024 bytes".into(),
            )),
            _ => Ok(()),
        }?;

        // Validate outgoing references
        if let Some(target) = &self.references {
            match value {
                Value::Null => Ok(()),
                Value::Float(f) if f.is_nan() => Ok(()),
                v if target == &table.name && v == pk => Ok(()),
                v if txn.read(target, v)?.is_none() => Err(Error::Value(format!(
                    "Referenced primary key {} in table {} does not exist",
                    v, target,
                ))),
                _ => Ok(()),
            }?;
        }

        // Validate uniqueness constraints
        if self.unique && !self.primary_key && value != &Value::Null {
            let index = table.get_column_index(&self.name)?;
            let mut scan = txn.scan(&table.name, None)?;
            while let Some(row) = scan.next().transpose()? {
                if row.get(index).unwrap_or(&Value::Null) == value
                    && &table.get_row_key(&row)? != pk
                {
                    return Err(Error::Value(format!(
                        "Unique value {} already exists for column {}",
                        value, self.name
                    )));
                }
            }
        }

        Ok(())
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut sql = format_ident(&self.name);
        sql += &format!(" {}", self.datatype);
        if self.primary_key {
            sql += " PRIMARY KEY";
        }
        if !self.nullable && !self.primary_key {
            sql += " NOT NULL";
        }
        if let Some(default) = &self.default {
            sql += &format!(" DEFAULT {}", default);
        }
        if self.unique && !self.primary_key {
            sql += " UNIQUE";
        }
        if let Some(reference) = &self.references {
            sql += &format!(" REFERENCES {}", reference);
        }
        if self.index {
            sql += " INDEX";
        }
        write!(f, "{}", sql)
    }
}
