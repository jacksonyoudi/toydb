pub mod ast;
mod lexer;

use lazy_static::lazy_static;

use crate::error::{ Error, Result };
use self::lexer::{ Keyword, Lexer, Token };

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
        self.lexer.next().unwrap_or_else(|| Err(Error::Parse("Unexpected end of input".into())))
    }

    /// 获取下一个词法分析器标记，如果它是预期的，则返回它，否则抛出错误。
    fn next_expect(&mut self, expect: Option<Token>) -> Result<Option<Token>> {
        if let Some(t) = expect {
            let token = self.next()?;
            if token == t {
                Ok(Some(token))
            } else {
                Err(Error::Parse(format!("Expected token {}, found {}", t, token)))
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
            Token::Ident(ident) => {
                return Ok(ident);
            }
            token => Err(Error::Parse(format!("Expected identifier, got {}", token))),
        }
    }

    /// 如果下一个词法分析器标记满足谓词函数，则获取它。
    fn next_if<F: Fn(&Token) -> bool>(&mut self, predicate: F) -> Option<Token> {
        // predicate 谓词
        self
            .peek()
            .unwrap_or(None)
            .filter(|t| predicate(t))?;
        self.next().ok()
    }

    ///如果下一个操作符满足类型和优先级，则获取它。
    fn next_if_operator<O: Operator>(&mut self, min_prec: u8) -> Result<Option<O>> {
        if
            let Some(operator) = self
                .peek()
                .unwrap_or(None)
                .and_then(|token| O::from(&token))
                .filter(|op| op.prec() >= min_prec)
        {
            self.next()?;
            Ok(Some(operator.augment(self)?))
        } else {
            Ok(None)
        }
    }

    /// 如果下一个词法标记是关键字，则获取它。
    fn next_if_keyword(&mut self) -> Option<Token> {
        self.next_if(|t| matches!(t, Token::Keyword(_)))
    }

    // 获取下一个词法标记
    fn next_if_token(&mut self, token: Token) -> Option<Token> {
        self.next_if(|t| t == &token)
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
            Some(Token::Keyword(Keyword::Begin)) => self.parse_transaction(),
            Some(Token::Keyword(Keyword::Commit)) => self.parse_transaction(),
            Some(Token::Keyword(Keyword::Rollback)) => self.parse_transaction(),

            Some(Token::Keyword(Keyword::Create)) => self.parse_ddl(),
            Some(Token::Keyword(Keyword::Drop)) => self.parse_ddl(),

            Some(Token::Keyword(Keyword::Delete)) => self.parse_statement_delete(),
            Some(Token::Keyword(Keyword::Insert)) => self.parse_statement_insert(),
            Some(Token::Keyword(Keyword::Select)) => self.parse_statement_select(),
            Some(Token::Keyword(Keyword::Update)) => self.parse_statement_update(),

            Some(Token::Keyword(Keyword::Explain)) => self.parse_statement_explain(),

            Some(token) => Err(Error::Parse(format!("Unexpected token {}", token))),
            None => Err(Error::Parse("Unexpected end of input".into())),
        }
    }

    // 解析ddl
    fn parse_ddl(&mut self) -> Result<ast::Statement> {
        match self.next()? {
            // 第一个关键词是 create
            Token::Keyword(Keyword::Create) =>
                match self.next()? {
                    // 关键词是 table
                    Token::Keyword(Keyword::Table) => self.parse_ddl_create_table(),
                    // 解析失败
                    token => Err(Error::Parse(format!("Unexpected token {}", token))),
                }
            Token::Keyword(Keyword::Drop) =>
                match self.next()? {
                    Token::Keyword(Keyword::Table) => self.parse_ddl_drop_table(),
                    token => Err(Error::Parse(format!("Unexpected token {}", token))),
                }
            token => Err(Error::Parse(format!("Unexpected token {}", token))),
        }
    }

    /// Parses a CREATE TABLE DDL statement. The CREATE TABLE prefix has
    /// already been consumed.
    fn parse_ddl_create_table(&mut self) -> Result<ast::Statement> {
        self.next_ident()?
    }
}

/// An operator trait, to help with parsing of operators
trait Operator: Sized {
    /// Looks up the corresponding operator for a token, if one exists
    fn from(token: &Token) -> Option<Self>;
    /// Augments an operator by allowing it to parse any modifiers.
    fn augment(self, parser: &mut Parser) -> Result<Self>;
    /// Returns the operator's associativity
    fn assoc(&self) -> u8;
    /// Returns the operator's precedence
    fn prec(&self) -> u8;
}

const ASSOC_LEFT: u8 = 1;
const ASSOC_RIGHT: u8 = 0;

/// Prefix operators
enum PrefixOperator {
    Minus,
    Not,
    Plus,
}

impl PrefixOperator {
    fn build(&self, rhs: ast::Expression) -> ast::Expression {
        match self {
            Self::Plus => ast::Operation::Assert(Box::new(rhs)).into(),
            Self::Minus => ast::Operation::Negate(Box::new(rhs)).into(),
            Self::Not => ast::Operation::Not(Box::new(rhs)).into(),
        }
    }
}

impl Operator for PrefixOperator {
    fn from(token: &Token) -> Option<Self> {
        match token {
            Token::Keyword(Keyword::Not) => Some(Self::Not),
            Token::Minus => Some(Self::Minus),
            Token::Plus => Some(Self::Plus),
            _ => None,
        }
    }

    fn augment(self, parser: &mut Parser) -> Result<Self> {
        Ok(self)
    }

    fn assoc(&self) -> u8 {
        ASSOC_RIGHT
    }

    fn prec(&self) -> u8 {
        9
    }
}

// 中缀操作符
enum InfixOperator {
    Add,
    And,
    Divide,
    Equal,
    Exponentiate,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Like,
    Modulo,
    Multiply,
    NotEqual,
    Or,
    Subtract,
}

impl InfixOperator {
    fn build(&self, lhs: ast::Expression, rhs: ast::Expression) -> ast::Expression {
        let (lhs, rhs) = (Box::new(lhs), Box::new(rhs));
        (
            match self {
                Self::Add => ast::Operation::Add(lhs, rhs),
                Self::And => ast::Operation::And(lhs, rhs),
                Self::Divide => ast::Operation::Divide(lhs, rhs),
                Self::Equal => ast::Operation::Equal(lhs, rhs),
                Self::Exponentiate => ast::Operation::Exponentiate(lhs, rhs),
                Self::GreaterThan => ast::Operation::GreaterThan(lhs, rhs),
                Self::GreaterThanOrEqual => ast::Operation::GreaterThanOrEqual(lhs, rhs),
                Self::LessThan => ast::Operation::LessThan(lhs, rhs),
                Self::LessThanOrEqual => ast::Operation::LessThanOrEqual(lhs, rhs),
                Self::Like => ast::Operation::Like(lhs, rhs),
                Self::Modulo => ast::Operation::Modulo(lhs, rhs),
                Self::Multiply => ast::Operation::Multiply(lhs, rhs),
                Self::NotEqual => ast::Operation::NotEqual(lhs, rhs),
                Self::Or => ast::Operation::Or(lhs, rhs),
                Self::Subtract => ast::Operation::Subtract(lhs, rhs),
            }
        ).into()
    }
}

impl Operator for InfixOperator {
    fn from(token: &Token) -> Option<Self> {
        Some(match token {
            Token::Asterisk => Self::Multiply,
            Token::Caret => Self::Exponentiate,
            Token::Equal => Self::Equal,
            Token::GreaterThan => Self::GreaterThan,
            Token::GreaterThanOrEqual => Self::GreaterThanOrEqual,
            Token::Keyword(Keyword::And) => Self::And,
            Token::Keyword(Keyword::Like) => Self::Like,
            Token::Keyword(Keyword::Or) => Self::Or,
            Token::LessOrGreaterThan => Self::NotEqual,
            Token::LessThan => Self::LessThan,
            Token::LessThanOrEqual => Self::LessThanOrEqual,
            Token::Minus => Self::Subtract,
            Token::NotEqual => Self::NotEqual,
            Token::Percent => Self::Modulo,
            Token::Plus => Self::Add,
            Token::Slash => Self::Divide,
            _ => {
                return None;
            }
        })
    }

    fn augment(self, parser: &mut Parser) -> Result<Self> {
        Ok(self)
    }

    fn assoc(&self) -> u8 {
        match self {
            Self::Exponentiate => ASSOC_RIGHT,
            _ => ASSOC_LEFT,
        }
    }

    fn prec(&self) -> u8 {
        todo!()
    }
}

enum PostfixOperator {
    Factorial,
    // FIXME Compiler bug? Why is this considered dead code?
    #[allow(dead_code)] IsNull {
        not: bool,
    },
}

impl PostfixOperator {
    fn build(&self, lhs: ast::Expression, rhs: ast::Expression) -> ast::Expression {
        let lhs = Box::new(lhs);
        (
            match self {
                Self::IsNull { not } =>
                    match not {
                        true => ast::Operation::Not(Box::new(ast::Operation::IsNull(lhs).into())),
                        false => ast::Operation::IsNull(lhs),
                    }
                Self::Factorial => ast::Operation::Factorial(lhs),
            }
        ).into()
    }
}

impl Operator for PostfixOperator {
    fn from(token: &Token) -> Option<Self> {
        match token {
            Token::Exclamation => Some(Self::Factorial),
            Token::Keyword(Keyword::Is) => Some(Self::IsNull { not: false }),
            _ => None,
        }
    }

    fn augment(mut self, parser: &mut Parser) -> Result<Self> {
        #[allow(clippy::single_match)]
        match &mut self {
            Self::IsNull { ref mut not } => {
                if parser.next_if_token(Keyword::Not.into()).is_some() {
                    *not = true;
                }
                parser.next_expect(Some(Keyword::Null.into()))?;
            }
            _ => {}
        }
        Ok(self)
    }

    fn assoc(&self) -> u8 {
        ASSOC_LEFT
    }

    fn prec(&self) -> u8 {
        8
    }
}

// Formats an identifier by quoting it as appropriate
// 根据需要将标识符引用起来进行格式化
pub(super) fn format_ident(ident: &str) -> String {
    lazy_static! {
        static ref RE_IDENT: Regex = Regex::new(r#"^\w[\w_]*$"#).unwrap();
    }

    if RE_IDENT.is_match(ident) && Keyword::from_str(ident).is_none() {
        ident.to_string()
    } else {
        format!("\"{}\"", ident.replace("\"", "\"\""))
    }
}
