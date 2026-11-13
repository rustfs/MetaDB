//! Stateless Epoch-Aware SQL & Request Router (`StatelessProxyRouter`) over `DistributedTxRouter`.

use std::sync::Arc;

use metadb_core::{InodeAttr, MetaEntry, QuotaRecord, Result, SecondaryIndexRecord};
use metadb_tx::DistributedTxRouter;

use crate::sql_lexer::SqlParam;
use crate::sql_templates::{MetadataSqlCommand, SqlTemplateParser};

/// Unified result returned from executing any of the 20 metadata SQL templates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProxySqlResult {
    Entry(MetaEntry),
    OptionalEntry(Option<MetaEntry>),
    Attr(InodeAttr),
    Entries {
        entries: Vec<MetaEntry>,
        next_cursor: Option<String>,
    },
    Quota(Option<QuotaRecord>),
    Quotas(Vec<QuotaRecord>),
    SecondaryIndex(Option<SecondaryIndexRecord>),
    SecondaryIndices(Vec<SecondaryIndexRecord>),
}

/// Stateless proxy compiling and executing the 20 metadata SQL templates against `DistributedTxRouter`.
pub struct StatelessProxyRouter {
    tx_router: Arc<DistributedTxRouter>,
}

impl StatelessProxyRouter {
    pub fn new(tx_router: Arc<DistributedTxRouter>) -> Self {
        Self { tx_router }
    }

    pub fn tx_router(&self) -> &Arc<DistributedTxRouter> {
        &self.tx_router
    }

    pub async fn execute_sql(&self, sql: &str, params: &[SqlParam]) -> Result<ProxySqlResult> {
        let cmd = SqlTemplateParser::parse_with_params(sql, params)?;
        self.execute_command(cmd).await
    }

    pub async fn execute_command(&self, cmd: MetadataSqlCommand) -> Result<ProxySqlResult> {
        match cmd {
            MetadataSqlCommand::CreateFile {
                pid,
                name,
                inode_id,
                fid,
                uid,
                mode,
                size_bytes,
            } => Ok(ProxySqlResult::Entry(
                self.tx_router
                    .create_file(pid, &name, inode_id, fid, uid, mode, size_bytes)
                    .await?,
            )),
            MetadataSqlCommand::Mkdir {
                pid,
                name,
                dir_id,
                uid,
                mode,
            } => Ok(ProxySqlResult::Entry(
                self.tx_router.mkdir(pid, &name, dir_id, uid, mode).await?,
            )),
            MetadataSqlCommand::Unlink { pid, name } => Ok(ProxySqlResult::Attr(
                self.tx_router.unlink(pid, &name).await?,
            )),
            MetadataSqlCommand::Rmdir { pid, name } => Ok(ProxySqlResult::Attr(
                self.tx_router.rmdir(pid, &name).await?,
            )),
            MetadataSqlCommand::Rename {
                src_pid,
                src_name,
                dst_pid,
                dst_name,
                overwrite,
            } => Ok(ProxySqlResult::Entry(
                self.tx_router
                    .rename(src_pid, &src_name, dst_pid, &dst_name, overwrite)
                    .await?,
            )),
            MetadataSqlCommand::DirStat { pid } => {
                Ok(ProxySqlResult::Attr(self.tx_router.dirstat(pid).await?))
            }
            MetadataSqlCommand::Lookup { pid, name } => Ok(ProxySqlResult::OptionalEntry(
                self.tx_router.lookup(pid, &name).await?,
            )),
            MetadataSqlCommand::ListDir {
                pid,
                after_name,
                name_prefix,
                limit,
            } => {
                let (entries, next_cursor) = self
                    .tx_router
                    .listdir_paginated(pid, after_name.as_deref(), name_prefix.as_deref(), limit)
                    .await?;
                Ok(ProxySqlResult::Entries {
                    entries,
                    next_cursor,
                })
            }
            MetadataSqlCommand::SetAttr { pid, name, patch } => Ok(ProxySqlResult::Entry(
                self.tx_router.setattr(pid, &name, patch).await?,
            )),
            MetadataSqlCommand::GetQuota { uid } => {
                Ok(ProxySqlResult::Quota(self.tx_router.get_quota(uid).await?))
            }
            MetadataSqlCommand::SetQuotaLimit {
                uid,
                quota_limit_bytes,
            } => Ok(ProxySqlResult::Quota(Some(
                self.tx_router
                    .set_quota_limit(uid, quota_limit_bytes)
                    .await?,
            ))),
            MetadataSqlCommand::ListQuotas { start_uid, limit } => Ok(ProxySqlResult::Quotas(
                self.tx_router.list_quotas(start_uid, limit).await?,
            )),
            MetadataSqlCommand::GetSecondaryIndex { fid } => Ok(ProxySqlResult::SecondaryIndex(
                self.tx_router.get_secondary_index(fid).await?,
            )),
            MetadataSqlCommand::ScanSecondaryIndex { after_fid, limit } => {
                Ok(ProxySqlResult::SecondaryIndices(
                    self.tx_router
                        .scan_secondary_index(after_fid, limit)
                        .await?,
                ))
            }
        }
    }
}
