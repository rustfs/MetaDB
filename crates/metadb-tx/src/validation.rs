use metadb_core::{is_dir_inode_id, InodeId, MetaDbError, Result, ROOT_INODE_ID};

pub(crate) fn validate_size(size: i64) -> Result<()> {
    if size < 0 {
        return Err(MetaDbError::InvalidFileSize(size));
    }
    Ok(())
}

pub(crate) fn validate_new_directory(pid: InodeId, id: InodeId) -> Result<()> {
    if !is_dir_inode_id(id) || id == ROOT_INODE_ID || id == pid {
        return Err(MetaDbError::CodecError(
            "invalid new directory inode".into(),
        ));
    }
    Ok(())
}

pub(crate) fn checked_count(value: i128) -> Result<i64> {
    i64::try_from(value)
        .ok()
        .filter(|n| *n >= 0)
        .ok_or_else(|| {
            MetaDbError::CodecError(
                "directory accounting is outside the nonnegative i64 domain".into(),
            )
        })
}
