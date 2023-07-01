use super::super::schema::{Catalog, Column, Table};
use crate::error::Result;

// 定义一个 plan 结构体
pub struct Planner<'a, C: Catalog> {
    catalog: &'a mut C,
}

impl<'a, C: Catalog> Planner<'a, C> {
    /// Creates a new planner.
    pub fn new(catalog: &'a mut C) -> Self {
        Self { catalog: catalog }
    }


    /// Builds a plan for an AST statement.
    pub fn build(&mut self, statement: ast::Statement) -> Result<Plan> {
        Ok(
            Plan(self.build_statement(statement)?)
        )
    }
}
