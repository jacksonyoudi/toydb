mod optimizer;
mod planner;

use crate::error::Result;

use serde_derive::{Deserialize, Serialize};
use std::fmt::{self, Display};



/// A plan node
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Node {
    Aggregation {
        source: Box<Node>,
        aggregates: Vec<Aggregate>,
    },
    CreateTable {
        schema: Table,
    },
    Delete {
        table: String,
        source: Box<Node>,
    },
    DropTable {
        table: String,
    },
    Filter {
        source: Box<Node>,
        predicate: Expression,
    },
    HashJoin {
        left: Box<Node>,
        left_field: (usize, Option<(Option<String>, String)>),
        right: Box<Node>,
        right_field: (usize, Option<(Option<String>, String)>),
        outer: bool,
    },
    IndexLookup {
        table: String,
        alias: Option<String>,
        column: String,
        values: Vec<Value>,
    },
    Insert {
        table: String,
        columns: Vec<String>,
        expressions: Vec<Vec<Expression>>,
    },
    KeyLookup {
        table: String,
        alias: Option<String>,
        keys: Vec<Value>,
    },
    Limit {
        source: Box<Node>,
        limit: u64,
    },
    NestedLoopJoin {
        left: Box<Node>,
        left_size: usize,
        right: Box<Node>,
        predicate: Option<Expression>,
        outer: bool,
    },
    Nothing,
    Offset {
        source: Box<Node>,
        offset: u64,
    },
    Order {
        source: Box<Node>,
        orders: Vec<(Expression, Direction)>,
    },
    Projection {
        source: Box<Node>,
        expressions: Vec<(Expression, Option<String>)>,
    },
    Scan {
        table: String,
        alias: Option<String>,
        filter: Option<Expression>,
    },
    Update {
        table: String,
        source: Box<Node>,
        expressions: Vec<(usize, Option<String>, Expression)>,
    },
}

/// A query plan
#[derive(Debug)]
pub struct Plan(pub Node);






