//! Lightweight SQL Template Execution Framework (§3.2 & §8 of `arXiv:2610.32247v1`).
//!
//! As described in §8 (*"SQL Interface vs. Dedicated API?"*), metadata services use only
//! ~20 fixed SQL patterns. Instead of running a heavy general-purpose SQL parser, semantic
//! analyzer, and cost-based query optimizer on every request, `metadb-proxy` maps fixed
//! metadata SQL templates directly to storage execution commands.

use metadb_core::{FileId, InodeId, MetaDbError, Result, UserId};

/// Compiled metadata command mapped directly from a lightweight SQL template.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataSqlCommand {
    /// `CREATE FILE (pid, name, inode_id, fid, uid, mode, size_bytes)`
    CreateFile {
        pid: InodeId,
        name: String,
        inode_id: InodeId,
        fid: FileId,
        uid: UserId,
        mode: u32,
        size_bytes: i64,
    },
    /// `MKDIR (pid, name, dir_id, uid, mode)`
    Mkdir {
        pid: InodeId,
        name: String,
        dir_id: InodeId,
        uid: UserId,
        mode: u32,
    },
    /// `DELETE FROM MetaTable WHERE pid = <pid> AND name = '<name>'`
    Unlink { pid: InodeId, name: String },
    /// `RMDIR FROM MetaTable WHERE pid = <pid> AND name = '<name>'`
    Rmdir { pid: InodeId, name: String },
    /// `SELECT * FROM MetaTable WHERE pid = <pid> AND name = '<name>'`
    Lookup { pid: InodeId, name: String },
    /// `SELECT * FROM MetaTable WHERE pid = <pid> AND name = '_attr_'`
    DirStat { pid: InodeId },
    /// `SELECT * FROM MetaTable WHERE pid = <pid> ORDER BY name ASC`
    ListDir { pid: InodeId },
    /// `RENAME FROM (<src_pid>, '<src_name>') TO (<dst_pid>, '<dst_name>')`
    Rename {
        src_pid: InodeId,
        src_name: String,
        dst_pid: InodeId,
        dst_name: String,
    },
}

/// Stateless pattern compiler translating fixed metadata SQL statements into `MetadataSqlCommand`.
pub struct SqlTemplateParser;

impl SqlTemplateParser {
    /// Parses a fixed metadata SQL statement into a direct `MetadataSqlCommand`.
    ///
    /// Supported fast-path SQL templates:
    /// - `INSERT INTO MetaTable (pid, name, id, type, fid, uid, mode, size) VALUES (1, 'a.txt', 100, 'FILE', 500, 10, 420, 4096)`
    /// - `INSERT INTO MetaTable (pid, name, id, type, uid, mode) VALUES (1, 'sub', 200, 'DIR', 10, 493)`
    /// - `SELECT * FROM MetaTable WHERE pid = 1 AND name = '_attr_'`
    /// - `SELECT * FROM MetaTable WHERE pid = 1 AND name = 'a.txt'`
    /// - `SELECT * FROM MetaTable WHERE pid = 1 ORDER BY name ASC`
    /// - `DELETE FROM MetaTable WHERE pid = 1 AND name = 'a.txt'`
    /// - `RMDIR FROM MetaTable WHERE pid = 1 AND name = 'sub'`
    /// - `UPDATE MetaTable SET pid = 2, name = 'b.txt' WHERE pid = 1 AND name = 'a.txt'`
    pub fn parse(sql: &str) -> Result<MetadataSqlCommand> {
        let trimmed = sql.trim().trim_end_matches(';').trim();

        if let Some(rest) = trimmed.strip_prefix(
            "INSERT INTO MetaTable (pid, name, id, type, fid, uid, mode, size) VALUES (",
        ) {
            let args = Self::split_tuple_args(rest)?;
            if args.len() != 8 || Self::unquote(&args[3]) != "FILE" {
                return Err(MetaDbError::SqlParseError(sql.to_string()));
            }
            return Ok(MetadataSqlCommand::CreateFile {
                pid: Self::parse_u64(&args[0])?,
                name: Self::unquote(&args[1]).to_string(),
                inode_id: Self::parse_u64(&args[2])?,
                fid: Self::parse_u64(&args[4])?,
                uid: Self::parse_u64(&args[5])?,
                mode: Self::parse_u64(&args[6])? as u32,
                size_bytes: Self::parse_i64(&args[7])?,
            });
        }

        if let Some(rest) =
            trimmed.strip_prefix("INSERT INTO MetaTable (pid, name, id, type, uid, mode) VALUES (")
        {
            let args = Self::split_tuple_args(rest)?;
            if args.len() != 6 || Self::unquote(&args[3]) != "DIR" {
                return Err(MetaDbError::SqlParseError(sql.to_string()));
            }
            return Ok(MetadataSqlCommand::Mkdir {
                pid: Self::parse_u64(&args[0])?,
                name: Self::unquote(&args[1]).to_string(),
                dir_id: Self::parse_u64(&args[2])?,
                uid: Self::parse_u64(&args[4])?,
                mode: Self::parse_u64(&args[5])? as u32,
            });
        }

        if let Some(rest) = trimmed.strip_prefix("SELECT * FROM MetaTable WHERE pid = ") {
            if let Some(pid_str) = rest.strip_suffix(" ORDER BY name ASC") {
                return Ok(MetadataSqlCommand::ListDir {
                    pid: Self::parse_u64(pid_str)?,
                });
            }
            if let Some((pid_str, name_part)) = rest.split_once(" AND name = ") {
                let pid = Self::parse_u64(pid_str)?;
                let name = Self::unquote(name_part);
                if name == "_attr_" {
                    return Ok(MetadataSqlCommand::DirStat { pid });
                }
                return Ok(MetadataSqlCommand::Lookup {
                    pid,
                    name: name.to_string(),
                });
            }
        }

        if let Some(rest) = trimmed.strip_prefix("DELETE FROM MetaTable WHERE pid = ") {
            if let Some((pid_str, name_part)) = rest.split_once(" AND name = ") {
                return Ok(MetadataSqlCommand::Unlink {
                    pid: Self::parse_u64(pid_str)?,
                    name: Self::unquote(name_part).to_string(),
                });
            }
        }

        if let Some(rest) = trimmed.strip_prefix("RMDIR FROM MetaTable WHERE pid = ") {
            if let Some((pid_str, name_part)) = rest.split_once(" AND name = ") {
                return Ok(MetadataSqlCommand::Rmdir {
                    pid: Self::parse_u64(pid_str)?,
                    name: Self::unquote(name_part).to_string(),
                });
            }
        }

        if let Some(rest) = trimmed.strip_prefix("UPDATE MetaTable SET pid = ") {
            if let Some((set_part, where_part)) = rest.split_once(" WHERE pid = ") {
                if let Some((dst_pid_str, dst_name_part)) = set_part.split_once(", name = ") {
                    if let Some((src_pid_str, src_name_part)) = where_part.split_once(" AND name = ")
                    {
                        return Ok(MetadataSqlCommand::Rename {
                            src_pid: Self::parse_u64(src_pid_str)?,
                            src_name: Self::unquote(src_name_part).to_string(),
                            dst_pid: Self::parse_u64(dst_pid_str)?,
                            dst_name: Self::unquote(dst_name_part).to_string(),
                        });
                    }
                }
            }
        }

        Err(MetaDbError::SqlParseError(format!(
            "unrecognized SQL template: {}",
            sql
        )))
    }

    fn split_tuple_args(s: &str) -> Result<Vec<String>> {
        let inner = s
            .strip_suffix(')')
            .ok_or_else(|| MetaDbError::SqlParseError("missing closing ')'".to_string()))?;
        Ok(inner.split(',').map(|p| p.trim().to_string()).collect())
    }

    fn unquote(s: &str) -> &str {
        s.trim().trim_matches('\'').trim_matches('"')
    }

    fn parse_u64(s: &str) -> Result<u64> {
        s.trim()
            .parse::<u64>()
            .map_err(|e| MetaDbError::SqlParseError(format!("invalid u64 '{}': {}", s, e)))
    }

    fn parse_i64(s: &str) -> Result<i64> {
        s.trim()
            .parse::<i64>()
            .map_err(|e| MetaDbError::SqlParseError(format!("invalid i64 '{}': {}", s, e)))
    }
}
