//! Complete 20-Template Metadata SQL Pattern Compiler (`sharding_and_control_plane.md` §7)
//! enforcing strict `"_attr_"` isolation (`Resolution N2` / `G-P1`).

use metadb_core::schema::validate_user_entry_name;
use metadb_core::{FileId, HlcTimestamp, InodeId, MetaDbError, Result, UserId, ATTR_ENTRY_NAME};
use metadb_tx::SetAttrPatch;

use crate::sql_lexer::{SqlLexer, SqlParam, SqlToken};

/// Compiled metadata execution plan covering all 20 canonical metadata SQL templates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataSqlCommand {
    /// #1 `INSERT INTO MetaTable (pid, name, id, type, fid, uid, mode, size) VALUES (?, ?, ?, 'FILE', ?, ?, ?, ?)`
    CreateFile {
        pid: InodeId,
        name: String,
        inode_id: InodeId,
        fid: FileId,
        uid: UserId,
        mode: u32,
        size_bytes: i64,
    },
    /// #2 `INSERT INTO MetaTable (pid, name, id, type, uid, mode) VALUES (?, ?, ?, 'DIR', ?, ?)`
    Mkdir {
        pid: InodeId,
        name: String,
        dir_id: InodeId,
        uid: UserId,
        mode: u32,
    },
    /// #3 `DELETE FROM MetaTable WHERE pid = ? AND name = ?`
    Unlink { pid: InodeId, name: String },
    /// #4 `RMDIR FROM MetaTable WHERE pid = ? AND name = ?`
    Rmdir { pid: InodeId, name: String },
    /// #5 (exclusive) & #6 (`REPLACE INTO ...` POSIX overwrite)
    Rename {
        src_pid: InodeId,
        src_name: String,
        dst_pid: InodeId,
        dst_name: String,
        overwrite: bool,
    },
    /// #7 `SELECT * FROM MetaTable WHERE pid = ? AND name = '_attr_'`
    DirStat { pid: InodeId },
    /// #8 `SELECT * FROM MetaTable WHERE pid = ? AND name = ?`
    Lookup { pid: InodeId, name: String },
    /// #9, #10 (cursor paginated), #11 (prefix `LIKE 'abc%'`)
    ListDir {
        pid: InodeId,
        after_name: Option<String>,
        name_prefix: Option<String>,
        limit: usize,
    },
    /// #12 (`mode`), #13 (`uid`), #14 (`size, fid`), #15 (`modify_time`)
    SetAttr {
        pid: InodeId,
        name: String,
        patch: SetAttrPatch,
    },
    /// #16 `SELECT * FROM QuotaTable WHERE uid = ?`
    GetQuota { uid: UserId },
    /// #17 `INSERT INTO QuotaTable (uid, quota_limit_bytes) VALUES (?, ?) ON CONFLICT (uid) DO UPDATE SET quota_limit_bytes = ?`
    SetQuotaLimit { uid: UserId, quota_limit_bytes: u64 },
    /// #18 `SELECT * FROM QuotaTable WHERE uid >= ? ORDER BY uid ASC LIMIT ?`
    ListQuotas { start_uid: UserId, limit: usize },
    /// #19 `SELECT * FROM SecondaryIndexTable WHERE fid = ?`
    GetSecondaryIndex { fid: FileId },
    /// #20 `SELECT * FROM SecondaryIndexTable WHERE fid > ? ORDER BY fid ASC LIMIT ?`
    ScanSecondaryIndex { after_fid: FileId, limit: usize },
}

struct TokenCursor<'a> {
    tokens: &'a [SqlToken],
    pos: usize,
    params: &'a [SqlParam],
    param_idx: usize,
}

impl<'a> TokenCursor<'a> {
    fn new(tokens: &'a [SqlToken], params: &'a [SqlParam]) -> Self {
        Self {
            tokens,
            pos: 0,
            params,
            param_idx: 0,
        }
    }

    fn peek(&self) -> Option<&SqlToken> {
        self.tokens.get(self.pos)
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.tokens.len()
    }

    fn expect_word(&mut self, expected_upper: &str) -> Result<()> {
        match self.tokens.get(self.pos) {
            Some(SqlToken::Word(w)) if w == expected_upper => {
                self.pos += 1;
                Ok(())
            }
            other => Err(MetaDbError::SqlParseError(format!(
                "expected keyword '{}', got {:?}",
                expected_upper, other
            ))),
        }
    }

    fn expect_token(&mut self, expected: &SqlToken) -> Result<()> {
        match self.tokens.get(self.pos) {
            Some(tok) if tok == expected => {
                self.pos += 1;
                Ok(())
            }
            other => Err(MetaDbError::SqlParseError(format!(
                "expected {:?}, got {:?}",
                expected, other
            ))),
        }
    }

    fn parse_u64_arg(&mut self) -> Result<u64> {
        match self.tokens.get(self.pos) {
            Some(SqlToken::IntLiteral(v)) if *v >= 0 && *v <= u64::MAX as i128 => {
                self.pos += 1;
                Ok(*v as u64)
            }
            Some(SqlToken::Placeholder) => {
                self.pos += 1;
                let p = self.next_param()?;
                match p {
                    SqlParam::U64(v) => Ok(*v),
                    SqlParam::I64(v) if *v >= 0 => Ok(*v as u64),
                    other => Err(MetaDbError::SqlParseError(format!(
                        "expected u64 bind param, got {:?}",
                        other
                    ))),
                }
            }
            other => Err(MetaDbError::SqlParseError(format!(
                "expected u64 value, got {:?}",
                other
            ))),
        }
    }

    fn parse_i64_arg(&mut self) -> Result<i64> {
        match self.tokens.get(self.pos) {
            Some(SqlToken::IntLiteral(v)) if *v >= i64::MIN as i128 && *v <= i64::MAX as i128 => {
                self.pos += 1;
                Ok(*v as i64)
            }
            Some(SqlToken::Placeholder) => {
                self.pos += 1;
                let p = self.next_param()?;
                match p {
                    SqlParam::I64(v) => Ok(*v),
                    SqlParam::U64(v) if *v <= i64::MAX as u64 => Ok(*v as i64),
                    other => Err(MetaDbError::SqlParseError(format!(
                        "expected i64 bind param, got {:?}",
                        other
                    ))),
                }
            }
            other => Err(MetaDbError::SqlParseError(format!(
                "expected i64 value, got {:?}",
                other
            ))),
        }
    }

    /// Returns `(value, was_literal_not_placeholder)`.
    fn parse_str_arg(&mut self) -> Result<(String, bool)> {
        match self.tokens.get(self.pos) {
            Some(SqlToken::StrLiteral(s)) => {
                let out = s.clone();
                self.pos += 1;
                Ok((out, true))
            }
            Some(SqlToken::Placeholder) => {
                self.pos += 1;
                let p = self.next_param()?;
                match p {
                    SqlParam::Str(s) => Ok((s.clone(), false)),
                    other => Err(MetaDbError::SqlParseError(format!(
                        "expected string bind param, got {:?}",
                        other
                    ))),
                }
            }
            other => Err(MetaDbError::SqlParseError(format!(
                "expected string literal or '?', got {:?}",
                other
            ))),
        }
    }

    fn next_param(&mut self) -> Result<&'a SqlParam> {
        let p = self.params.get(self.param_idx).ok_or_else(|| {
            MetaDbError::SqlParseError(format!(
                "missing bind parameter at position {}",
                self.param_idx
            ))
        })?;
        self.param_idx += 1;
        Ok(p)
    }

    fn ensure_complete(&self) -> Result<()> {
        if !self.is_eof() {
            return Err(MetaDbError::SqlParseError(format!(
                "unexpected trailing tokens starting at {:?}",
                self.peek()
            )));
        }
        if self.param_idx != self.params.len() {
            return Err(MetaDbError::SqlParseError(format!(
                "unused bind parameters: used {}, supplied {}",
                self.param_idx,
                self.params.len()
            )));
        }
        Ok(())
    }
}

/// Stateless compiler translating all 20 canonical metadata SQL templates into `MetadataSqlCommand`.
pub struct SqlTemplateParser;

impl SqlTemplateParser {
    pub fn parse(sql: &str) -> Result<MetadataSqlCommand> {
        Self::parse_with_params(sql, &[])
    }

    pub fn parse_with_params(sql: &str, params: &[SqlParam]) -> Result<MetadataSqlCommand> {
        let tokens = SqlLexer::tokenize(sql)?;
        if tokens.is_empty() {
            return Err(MetaDbError::SqlParseError(
                "empty SQL statement".to_string(),
            ));
        }
        let mut c = TokenCursor::new(&tokens, params);

        let Some(SqlToken::Word(first)) = c.peek().cloned() else {
            return Err(MetaDbError::SqlParseError(
                "SQL template must begin with a verb keyword".to_string(),
            ));
        };

        let cmd = match first.as_str() {
            "INSERT" => Self::parse_insert(&mut c)?,
            "DELETE" => Self::parse_delete(&mut c)?,
            "RMDIR" => Self::parse_rmdir(&mut c)?,
            "REPLACE" => Self::parse_replace_rename(&mut c)?,
            "UPDATE" => Self::parse_update(&mut c)?,
            "SELECT" => Self::parse_select(&mut c)?,
            _ => {
                return Err(MetaDbError::SqlParseError(format!(
                    "unsupported SQL verb '{}'",
                    first
                )))
            }
        };
        c.ensure_complete()?;
        Ok(cmd)
    }

    fn parse_insert(c: &mut TokenCursor<'_>) -> Result<MetadataSqlCommand> {
        c.expect_word("INSERT")?;
        c.expect_word("INTO")?;
        match c.peek().cloned() {
            Some(SqlToken::Word(tbl)) if tbl == "METATABLE" => {
                c.pos += 1;
                c.expect_token(&SqlToken::LParen)?;
                c.expect_word("PID")?;
                c.expect_token(&SqlToken::Comma)?;
                c.expect_word("NAME")?;
                c.expect_token(&SqlToken::Comma)?;
                c.expect_word("ID")?;
                c.expect_token(&SqlToken::Comma)?;
                c.expect_word("TYPE")?;
                c.expect_token(&SqlToken::Comma)?;

                match c.peek().cloned() {
                    Some(SqlToken::Word(col5)) if col5 == "FID" => {
                        // Template #1: (pid, name, id, type, fid, uid, mode, size)
                        c.pos += 1;
                        c.expect_token(&SqlToken::Comma)?;
                        c.expect_word("UID")?;
                        c.expect_token(&SqlToken::Comma)?;
                        c.expect_word("MODE")?;
                        c.expect_token(&SqlToken::Comma)?;
                        c.expect_word("SIZE")?;
                        c.expect_token(&SqlToken::RParen)?;
                        c.expect_word("VALUES")?;
                        c.expect_token(&SqlToken::LParen)?;

                        let pid = c.parse_u64_arg()?;
                        c.expect_token(&SqlToken::Comma)?;
                        let (name, _) = c.parse_str_arg()?;
                        validate_user_entry_name(&name)?;
                        c.expect_token(&SqlToken::Comma)?;
                        let inode_id = c.parse_u64_arg()?;
                        c.expect_token(&SqlToken::Comma)?;
                        let (ty, _) = c.parse_str_arg()?;
                        if !ty.eq_ignore_ascii_case("FILE") {
                            return Err(MetaDbError::SqlParseError(
                                "Template #1 requires type 'FILE'".to_string(),
                            ));
                        }
                        c.expect_token(&SqlToken::Comma)?;
                        let fid = c.parse_u64_arg()?;
                        c.expect_token(&SqlToken::Comma)?;
                        let uid = c.parse_u64_arg()?;
                        c.expect_token(&SqlToken::Comma)?;
                        let mode = c.parse_u64_arg()? as u32;
                        c.expect_token(&SqlToken::Comma)?;
                        let size_bytes = c.parse_i64_arg()?;
                        c.expect_token(&SqlToken::RParen)?;

                        Ok(MetadataSqlCommand::CreateFile {
                            pid,
                            name,
                            inode_id,
                            fid,
                            uid,
                            mode,
                            size_bytes,
                        })
                    }
                    Some(SqlToken::Word(col5)) if col5 == "UID" => {
                        // Template #2: (pid, name, id, type, uid, mode)
                        c.pos += 1;
                        c.expect_token(&SqlToken::Comma)?;
                        c.expect_word("MODE")?;
                        c.expect_token(&SqlToken::RParen)?;
                        c.expect_word("VALUES")?;
                        c.expect_token(&SqlToken::LParen)?;

                        let pid = c.parse_u64_arg()?;
                        c.expect_token(&SqlToken::Comma)?;
                        let (name, _) = c.parse_str_arg()?;
                        validate_user_entry_name(&name)?;
                        c.expect_token(&SqlToken::Comma)?;
                        let dir_id = c.parse_u64_arg()?;
                        c.expect_token(&SqlToken::Comma)?;
                        let (ty, _) = c.parse_str_arg()?;
                        if !ty.eq_ignore_ascii_case("DIR") {
                            return Err(MetaDbError::SqlParseError(
                                "Template #2 requires type 'DIR'".to_string(),
                            ));
                        }
                        c.expect_token(&SqlToken::Comma)?;
                        let uid = c.parse_u64_arg()?;
                        c.expect_token(&SqlToken::Comma)?;
                        let mode = c.parse_u64_arg()? as u32;
                        c.expect_token(&SqlToken::RParen)?;

                        Ok(MetadataSqlCommand::Mkdir {
                            pid,
                            name,
                            dir_id,
                            uid,
                            mode,
                        })
                    }
                    other => Err(MetaDbError::SqlParseError(format!(
                        "unrecognized MetaTable INSERT column {:?}",
                        other
                    ))),
                }
            }
            Some(SqlToken::Word(tbl)) if tbl == "QUOTATABLE" => {
                // Template #17: INSERT INTO QuotaTable (uid, quota_limit_bytes) VALUES (?, ?) ON CONFLICT (uid) DO UPDATE SET quota_limit_bytes = ?
                c.pos += 1;
                c.expect_token(&SqlToken::LParen)?;
                c.expect_word("UID")?;
                c.expect_token(&SqlToken::Comma)?;
                c.expect_word("QUOTA_LIMIT_BYTES")?;
                c.expect_token(&SqlToken::RParen)?;
                c.expect_word("VALUES")?;
                c.expect_token(&SqlToken::LParen)?;
                let uid = c.parse_u64_arg()?;
                c.expect_token(&SqlToken::Comma)?;
                let limit_1 = c.parse_u64_arg()?;
                c.expect_token(&SqlToken::RParen)?;
                c.expect_word("ON")?;
                c.expect_word("CONFLICT")?;
                c.expect_token(&SqlToken::LParen)?;
                c.expect_word("UID")?;
                c.expect_token(&SqlToken::RParen)?;
                c.expect_word("DO")?;
                c.expect_word("UPDATE")?;
                c.expect_word("SET")?;
                c.expect_word("QUOTA_LIMIT_BYTES")?;
                c.expect_token(&SqlToken::Eq)?;
                let limit_2 = c.parse_u64_arg()?;
                if limit_1 != limit_2 {
                    return Err(MetaDbError::SqlParseError(
                        "Template #17 requires VALUES quota_limit_bytes to equal the ON CONFLICT UPDATE value; SetQuotaLimit stores one absolute limit".to_string(),
                    ));
                }
                Ok(MetadataSqlCommand::SetQuotaLimit {
                    uid,
                    quota_limit_bytes: limit_1,
                })
            }
            other => Err(MetaDbError::SqlParseError(format!(
                "unsupported INSERT table {:?}",
                other
            ))),
        }
    }

    fn parse_delete(c: &mut TokenCursor<'_>) -> Result<MetadataSqlCommand> {
        // Template #3: DELETE FROM MetaTable WHERE pid = ? AND name = ?
        c.expect_word("DELETE")?;
        c.expect_word("FROM")?;
        c.expect_word("METATABLE")?;
        let (pid, name) = Self::parse_where_pid_and_user_name(c)?;
        Ok(MetadataSqlCommand::Unlink { pid, name })
    }

    fn parse_rmdir(c: &mut TokenCursor<'_>) -> Result<MetadataSqlCommand> {
        // Template #4: RMDIR FROM MetaTable WHERE pid = ? AND name = ?
        c.expect_word("RMDIR")?;
        c.expect_word("FROM")?;
        c.expect_word("METATABLE")?;
        let (pid, name) = Self::parse_where_pid_and_user_name(c)?;
        Ok(MetadataSqlCommand::Rmdir { pid, name })
    }

    fn parse_replace_rename(c: &mut TokenCursor<'_>) -> Result<MetadataSqlCommand> {
        // Template #6: REPLACE INTO MetaTable (pid, name) SELECT ?, ? FROM MetaTable WHERE pid = ? AND name = ?
        c.expect_word("REPLACE")?;
        c.expect_word("INTO")?;
        c.expect_word("METATABLE")?;
        c.expect_token(&SqlToken::LParen)?;
        c.expect_word("PID")?;
        c.expect_token(&SqlToken::Comma)?;
        c.expect_word("NAME")?;
        c.expect_token(&SqlToken::RParen)?;
        c.expect_word("SELECT")?;
        let dst_pid = c.parse_u64_arg()?;
        c.expect_token(&SqlToken::Comma)?;
        let (dst_name, _) = c.parse_str_arg()?;
        validate_user_entry_name(&dst_name)?;
        c.expect_word("FROM")?;
        c.expect_word("METATABLE")?;
        let (src_pid, src_name) = Self::parse_where_pid_and_user_name(c)?;
        Ok(MetadataSqlCommand::Rename {
            src_pid,
            src_name,
            dst_pid,
            dst_name,
            overwrite: true,
        })
    }

    fn parse_update(c: &mut TokenCursor<'_>) -> Result<MetadataSqlCommand> {
        c.expect_word("UPDATE")?;
        c.expect_word("METATABLE")?;
        c.expect_word("SET")?;

        let Some(SqlToken::Word(col)) = c.peek().cloned() else {
            return Err(MetaDbError::SqlParseError(
                "expected column name after SET".to_string(),
            ));
        };
        c.pos += 1;
        c.expect_token(&SqlToken::Eq)?;

        match col.as_str() {
            "PID" => {
                // Template #5: UPDATE MetaTable SET pid = ?, name = ? WHERE pid = ? AND name = ?
                let dst_pid = c.parse_u64_arg()?;
                c.expect_token(&SqlToken::Comma)?;
                c.expect_word("NAME")?;
                c.expect_token(&SqlToken::Eq)?;
                let (dst_name, _) = c.parse_str_arg()?;
                validate_user_entry_name(&dst_name)?;
                let (src_pid, src_name) = Self::parse_where_pid_and_user_name(c)?;
                Ok(MetadataSqlCommand::Rename {
                    src_pid,
                    src_name,
                    dst_pid,
                    dst_name,
                    overwrite: false,
                })
            }
            "MODE" => {
                // Template #12: chmod
                let mode = c.parse_u64_arg()? as u32;
                let (pid, name) = Self::parse_where_pid_and_user_name(c)?;
                Ok(MetadataSqlCommand::SetAttr {
                    pid,
                    name,
                    patch: SetAttrPatch {
                        mode: Some(mode),
                        ..Default::default()
                    },
                })
            }
            "UID" => {
                // Template #13: chown
                let uid = c.parse_u64_arg()?;
                let (pid, name) = Self::parse_where_pid_and_user_name(c)?;
                Ok(MetadataSqlCommand::SetAttr {
                    pid,
                    name,
                    patch: SetAttrPatch {
                        uid: Some(uid),
                        ..Default::default()
                    },
                })
            }
            "SIZE" => {
                // Template #14: UPDATE MetaTable SET size = ?, fid = ? WHERE pid = ? AND name = ?
                let size_bytes = c.parse_i64_arg()?;
                c.expect_token(&SqlToken::Comma)?;
                c.expect_word("FID")?;
                c.expect_token(&SqlToken::Eq)?;
                let fid = c.parse_u64_arg()?;
                let (pid, name) = Self::parse_where_pid_and_user_name(c)?;
                Ok(MetadataSqlCommand::SetAttr {
                    pid,
                    name,
                    patch: SetAttrPatch {
                        size_bytes: Some(size_bytes),
                        fid: Some(fid),
                        ..Default::default()
                    },
                })
            }
            "MODIFY_TIME" => {
                // Template #15: utimes
                let raw_hlc = c.parse_u64_arg()?;
                let (pid, name) = Self::parse_where_pid_and_user_name(c)?;
                Ok(MetadataSqlCommand::SetAttr {
                    pid,
                    name,
                    patch: SetAttrPatch {
                        modify_time: Some(HlcTimestamp(raw_hlc)),
                        ..Default::default()
                    },
                })
            }
            _ => Err(MetaDbError::SqlParseError(format!(
                "unsupported UPDATE column '{}'",
                col
            ))),
        }
    }

    fn parse_select(c: &mut TokenCursor<'_>) -> Result<MetadataSqlCommand> {
        c.expect_word("SELECT")?;
        c.expect_token(&SqlToken::Star)?;
        c.expect_word("FROM")?;

        match c.peek().cloned() {
            Some(SqlToken::Word(tbl)) if tbl == "METATABLE" => {
                c.pos += 1;
                c.expect_word("WHERE")?;
                c.expect_word("PID")?;
                c.expect_token(&SqlToken::Eq)?;
                let pid = c.parse_u64_arg()?;

                match c.peek().cloned() {
                    Some(SqlToken::Word(w)) if w == "ORDER" => {
                        // Template #9: SELECT * FROM MetaTable WHERE pid = ? ORDER BY name ASC
                        c.pos += 1;
                        c.expect_word("BY")?;
                        c.expect_word("NAME")?;
                        c.expect_word("ASC")?;
                        Ok(MetadataSqlCommand::ListDir {
                            pid,
                            after_name: None,
                            name_prefix: None,
                            limit: 10_000,
                        })
                    }
                    Some(SqlToken::Word(w)) if w == "AND" => {
                        c.pos += 1;
                        c.expect_word("NAME")?;
                        match c.peek().cloned() {
                            Some(SqlToken::Eq) => {
                                c.pos += 1;
                                let (name, was_literal) = c.parse_str_arg()?;
                                // Strict `_attr_` Isolation Rule (`Resolution N2` / `G-P1`):
                                // Literal `'_attr_'` is Template #7 (`DirStat`); bound `?` param in
                                // Template #8 (`Lookup`) rejects `'_attr_'`!
                                if was_literal && name == ATTR_ENTRY_NAME {
                                    Ok(MetadataSqlCommand::DirStat { pid })
                                } else {
                                    validate_user_entry_name(&name)?;
                                    Ok(MetadataSqlCommand::Lookup { pid, name })
                                }
                            }
                            Some(SqlToken::Gt) => {
                                // Template #10: AND name > ? ORDER BY name ASC LIMIT ?
                                c.pos += 1;
                                let (after, _) = c.parse_str_arg()?;
                                c.expect_word("ORDER")?;
                                c.expect_word("BY")?;
                                c.expect_word("NAME")?;
                                c.expect_word("ASC")?;
                                c.expect_word("LIMIT")?;
                                let limit = c.parse_u64_arg()? as usize;
                                Ok(MetadataSqlCommand::ListDir {
                                    pid,
                                    after_name: Some(after),
                                    name_prefix: None,
                                    limit,
                                })
                            }
                            Some(SqlToken::Word(lw)) if lw == "LIKE" => {
                                // Template #11: AND name LIKE ? ORDER BY name ASC LIMIT ?
                                c.pos += 1;
                                let (pattern, _) = c.parse_str_arg()?;
                                let prefix = pattern
                                    .strip_suffix('%')
                                    .ok_or_else(|| {
                                        MetaDbError::SqlParseError(
                                            "LIKE pattern must end with trailing '%'".to_string(),
                                        )
                                    })?
                                    .to_string();
                                if prefix.contains('%') {
                                    return Err(MetaDbError::SqlParseError(
                                        "only prefix 'abc%' LIKE patterns are supported"
                                            .to_string(),
                                    ));
                                }
                                c.expect_word("ORDER")?;
                                c.expect_word("BY")?;
                                c.expect_word("NAME")?;
                                c.expect_word("ASC")?;
                                c.expect_word("LIMIT")?;
                                let limit = c.parse_u64_arg()? as usize;
                                Ok(MetadataSqlCommand::ListDir {
                                    pid,
                                    after_name: None,
                                    name_prefix: Some(prefix),
                                    limit,
                                })
                            }
                            other => Err(MetaDbError::SqlParseError(format!(
                                "unexpected operator after WHERE pid = ? AND name: {:?}",
                                other
                            ))),
                        }
                    }
                    other => Err(MetaDbError::SqlParseError(format!(
                        "unexpected clause in MetaTable SELECT: {:?}",
                        other
                    ))),
                }
            }
            Some(SqlToken::Word(tbl)) if tbl == "QUOTATABLE" => {
                c.pos += 1;
                c.expect_word("WHERE")?;
                c.expect_word("UID")?;
                match c.peek().cloned() {
                    Some(SqlToken::Eq) => {
                        // Template #16: SELECT * FROM QuotaTable WHERE uid = ?
                        c.pos += 1;
                        let uid = c.parse_u64_arg()?;
                        Ok(MetadataSqlCommand::GetQuota { uid })
                    }
                    Some(SqlToken::Ge) => {
                        // Template #18: SELECT * FROM QuotaTable WHERE uid >= ? ORDER BY uid ASC LIMIT ?
                        c.pos += 1;
                        let start_uid = c.parse_u64_arg()?;
                        c.expect_word("ORDER")?;
                        c.expect_word("BY")?;
                        c.expect_word("UID")?;
                        c.expect_word("ASC")?;
                        c.expect_word("LIMIT")?;
                        let limit = c.parse_u64_arg()? as usize;
                        Ok(MetadataSqlCommand::ListQuotas { start_uid, limit })
                    }
                    other => Err(MetaDbError::SqlParseError(format!(
                        "unexpected QuotaTable WHERE operator {:?}",
                        other
                    ))),
                }
            }
            Some(SqlToken::Word(tbl)) if tbl == "SECONDARYINDEXTABLE" => {
                c.pos += 1;
                c.expect_word("WHERE")?;
                c.expect_word("FID")?;
                match c.peek().cloned() {
                    Some(SqlToken::Eq) => {
                        // Template #19: SELECT * FROM SecondaryIndexTable WHERE fid = ?
                        c.pos += 1;
                        let fid = c.parse_u64_arg()?;
                        Ok(MetadataSqlCommand::GetSecondaryIndex { fid })
                    }
                    Some(SqlToken::Gt) => {
                        // Template #20: SELECT * FROM SecondaryIndexTable WHERE fid > ? ORDER BY fid ASC LIMIT ?
                        c.pos += 1;
                        let after_fid = c.parse_u64_arg()?;
                        c.expect_word("ORDER")?;
                        c.expect_word("BY")?;
                        c.expect_word("FID")?;
                        c.expect_word("ASC")?;
                        c.expect_word("LIMIT")?;
                        let limit = c.parse_u64_arg()? as usize;
                        Ok(MetadataSqlCommand::ScanSecondaryIndex { after_fid, limit })
                    }
                    other => Err(MetaDbError::SqlParseError(format!(
                        "unexpected SecondaryIndexTable WHERE operator {:?}",
                        other
                    ))),
                }
            }
            other => Err(MetaDbError::SqlParseError(format!(
                "unsupported SELECT table {:?}",
                other
            ))),
        }
    }

    fn parse_where_pid_and_user_name(c: &mut TokenCursor<'_>) -> Result<(InodeId, String)> {
        c.expect_word("WHERE")?;
        c.expect_word("PID")?;
        c.expect_token(&SqlToken::Eq)?;
        let pid = c.parse_u64_arg()?;
        c.expect_word("AND")?;
        c.expect_word("NAME")?;
        c.expect_token(&SqlToken::Eq)?;
        let (name, _) = c.parse_str_arg()?;
        validate_user_entry_name(&name)?;
        Ok((pid, name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql_lexer::SqlParam;

    const SET_QUOTA: &str = "INSERT INTO QuotaTable (uid, quota_limit_bytes) VALUES (?, ?) \
         ON CONFLICT (uid) DO UPDATE SET quota_limit_bytes = ?";

    #[test]
    fn template_17_requires_equal_insert_and_update_limits() {
        let cmd = SqlTemplateParser::parse_with_params(
            SET_QUOTA,
            &[
                SqlParam::U64(77),
                SqlParam::U64(9_000_000),
                SqlParam::U64(9_000_000),
            ],
        )
        .unwrap();
        assert_eq!(
            cmd,
            MetadataSqlCommand::SetQuotaLimit {
                uid: 77,
                quota_limit_bytes: 9_000_000,
            }
        );

        let err = SqlTemplateParser::parse_with_params(
            SET_QUOTA,
            &[
                SqlParam::U64(77),
                SqlParam::U64(9_000_000),
                SqlParam::U64(1_000),
            ],
        )
        .unwrap_err();
        assert!(matches!(err, MetaDbError::SqlParseError(_)));
    }
}
