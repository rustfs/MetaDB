//! Tokenizing SQL lexer supporting single-quoted string literals (`''` and `\'` escaping),
//! signed/unsigned integer literals, comparison operators, and `?` positional bind parameters.

use metadb_core::{MetaDbError, Result};

/// Positional bind parameter value passed alongside a parameterized SQL template.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlParam {
    U64(u64),
    I64(i64),
    Str(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlToken {
    Word(String), // Uppercase normalized keyword or identifier (`META_TABLE` / `PID`)
    StrLiteral(String),
    IntLiteral(i128),
    Placeholder, // `?`
    LParen,
    RParen,
    Comma,
    Eq,
    Gt,
    Ge,
    Star,
    Semicolon,
}

pub struct SqlLexer;

impl SqlLexer {
    pub fn tokenize(sql: &str) -> Result<Vec<SqlToken>> {
        let chars: Vec<char> = sql.chars().collect();
        let len = chars.len();
        let mut i = 0usize;
        let mut tokens = Vec::new();

        while i < len {
            let c = chars[i];
            if c.is_whitespace() {
                i += 1;
                continue;
            }
            match c {
                '(' => {
                    tokens.push(SqlToken::LParen);
                    i += 1;
                }
                ')' => {
                    tokens.push(SqlToken::RParen);
                    i += 1;
                }
                ',' => {
                    tokens.push(SqlToken::Comma);
                    i += 1;
                }
                '=' => {
                    tokens.push(SqlToken::Eq);
                    i += 1;
                }
                '>' => {
                    if i + 1 < len && chars[i + 1] == '=' {
                        tokens.push(SqlToken::Ge);
                        i += 2;
                    } else {
                        tokens.push(SqlToken::Gt);
                        i += 1;
                    }
                }
                '*' => {
                    tokens.push(SqlToken::Star);
                    i += 1;
                }
                ';' => {
                    tokens.push(SqlToken::Semicolon);
                    i += 1;
                }
                '?' => {
                    tokens.push(SqlToken::Placeholder);
                    i += 1;
                }
                '\'' => {
                    i += 1;
                    let mut s = String::new();
                    let mut closed = false;
                    while i < len {
                        if chars[i] == '\\' && i + 1 < len {
                            s.push(chars[i + 1]);
                            i += 2;
                        } else if chars[i] == '\'' {
                            if i + 1 < len && chars[i + 1] == '\'' {
                                s.push('\'');
                                i += 2;
                            } else {
                                i += 1;
                                closed = true;
                                break;
                            }
                        } else {
                            s.push(chars[i]);
                            i += 1;
                        }
                    }
                    if !closed {
                        return Err(MetaDbError::SqlParseError(
                            "unterminated single-quoted string literal".to_string(),
                        ));
                    }
                    tokens.push(SqlToken::StrLiteral(s));
                }
                '-' | '0'..='9' => {
                    let start = i;
                    if c == '-' {
                        i += 1;
                    }
                    if i >= len || !chars[i].is_ascii_digit() {
                        return Err(MetaDbError::SqlParseError(format!(
                            "invalid number near position {}",
                            start
                        )));
                    }
                    while i < len && chars[i].is_ascii_digit() {
                        i += 1;
                    }
                    let num_str: String = chars[start..i].iter().collect();
                    let val = num_str.parse::<i128>().map_err(|e| {
                        MetaDbError::SqlParseError(format!(
                            "integer parse error '{}': {}",
                            num_str, e
                        ))
                    })?;
                    tokens.push(SqlToken::IntLiteral(val));
                }
                _ if c.is_ascii_alphabetic() || c == '_' => {
                    let start = i;
                    while i < len && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                        i += 1;
                    }
                    let word: String = chars[start..i].iter().collect();
                    tokens.push(SqlToken::Word(word.to_ascii_uppercase()));
                }
                other => {
                    return Err(MetaDbError::SqlParseError(format!(
                        "unexpected character '{}' in SQL template",
                        other
                    )))
                }
            }
        }

        if matches!(tokens.last(), Some(SqlToken::Semicolon)) {
            tokens.pop();
        }
        Ok(tokens)
    }
}
