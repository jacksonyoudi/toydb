pub mod ast;
mod lexer;

use crate::error::{Error, Result};

use self::lexer::{Keyword, Lexer, Token};

/// SQL 解析
pub struct Parser<'a> {
    // 词法分析器
    lexer: std::iter::Peekable<Lexer<'a>>,
}

impl<'a> Parser<'a> {
    // 创建一个词法解析器
    pub fn new(query: &str) -> Parser {
        Parser {
            lexer: Lexer::new(query).peekable(),
        }
    }

    // 返回一个表达式
    // Parses the input string into an AST statement
    pub fn parse(&mut self) -> Result<ast::Statement> {
        // 解析得到 statement
        // Semicolon 分号
        let statement: ast::Statement = self.parse_statement()?;
        self.next_if_token(Token::Semicolon);
        self.next_expect(None)?;
        Ok(statement)
    }

    /// 获取下一个词法分析器标记，如果没有找到则抛出错误。
    fn next(&mut self) -> Result<Token> {
        self.lexer
            .next()
            .unwrap_or_else(|| Err(Error::Parse("Unexpected end of input".into())))
    }

    /// 获取下一个词法分析器标记，如果它是预期的，则返回它，否则抛出错误。
    fn next_expect(&mut self, expect: Option<Token>) -> Result<Option<Token>> {
        if let Some(t) = self.expect {
            let token = self.next()?;
            if token == t {
                Ok(Some(token))
            } else {
                Err(Error::Parse(format!(
                    "Expected token {}, found {}",
                    t, token
                )))
            }
        } else if let Some(token) = self.peek()? {
            Err(Error::Parse(format!("Unexpected token {}", token)))
        } else {
            Ok(None)
        }
    }

    /// 获取下一个标识符，如果没有找到则报错。
    fn next_ident(&mut self) -> Result<String> {
        match self.next()? {
            Token::Ident(ident) => return Ok(ident),
            toke => Err(Error::Parse(format!("Expected identifier, got {}", token))),
        }
    }

    /// 如果下一个词法分析器标记满足谓词函数，则获取它。
    fn next_if<F: Fn(&Token) -> bool>(&mut self, predicate: F) -> Option<Token> {
        // predicate 谓词
        self.peek().unwrap_or(None).filter(|t| predicate(t))?;
        self.next().ok()
    }

    /// 如果有的话，查看下一个词法分析器标记，
    /// 但将其从 Option<Result<Token>> 转换为 Result<Option<Token>>，
    /// 这更方便使用（Iterator trait 要求 Option<T>）.
    fn peek(&mut self) -> Result<Option<Token>> {
        self.lexer.peek().cloned().transpose()
    }

    // 解析出一个parse_statement
    fn parse_statement(&mut self) -> Result<ast::Statement> {
        match self.peek()? {
            Some(Token::Keyword(lexer::Keyword::Begin)) => self.parse_translation(),
        }
    }

    // 解析ddl
    fn parse_ddl(&mut self) -> Result<ast::Statement> {
        match self.next()? {
            // 第一个关键词是 create
            Token::Keyword(Keyword::Create) => match self.next()? {
                // 关键词是 table
                Token::Keyword(Keyword::Table) => self.parse_ddl_create_table(),
                // 解析失败
                token => Err(Error::Parse(format!("Unexpected token {}", token))),
            },
            Token::Keyword(Keyword::Drop) => match self.next()? {
                Token::Keyword(Keyword::Table) => self.parse_ddl_drop_table(),
                token => Err(Error::Parse(format!("Unexpected token {}", token))),
            },
            token => Err(Error::Parse(format!("Unexpected token {}", token))),
        }
    }

    /// Parses a CREATE TABLE DDL statement. The CREATE TABLE prefix has
    /// already been consumed.
    fn parse_ddl_create_table(&mut self) -> Result<ast::Statement> {
        self.next_ident()?
    }
}
