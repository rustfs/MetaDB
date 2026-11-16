//! Parent-chain cycle detection for directory rename.
//!
//! The walk fails closed: seeing the directory being moved, a repeated inode, or a
//! chain longer than [`MAX_RENAME_ANCESTORS`] rejects the rename. A fixed 1024-step
//! cap that then *allowed* the rename let a deep tree be moved into its own descendant.

use std::collections::HashSet;

use metadb_core::{InodeId, MetaDbError, Result, ROOT_INODE_ID};

/// Ancestor walks longer than this are rejected. The bound is a denial-of-service
/// guard, not a point at which the rename is assumed safe.
const MAX_RENAME_ANCESTORS: usize = 1_048_576;

/// Tracks inodes already visited while walking `dst_pid` toward the root.
pub struct RenameCycleWalker {
    src_dir_id: InodeId,
    dst_pid: InodeId,
    seen: HashSet<InodeId>,
}

impl RenameCycleWalker {
    pub fn new(src_dir_id: InodeId, dst_pid: InodeId) -> Self {
        Self {
            src_dir_id,
            dst_pid,
            seen: HashSet::new(),
        }
    }

    /// Records `cursor`. `Ok(true)` means the caller should load `cursor`'s parent and continue.
    /// `Ok(false)` means the root was reached without a cycle.
    pub fn observe(&mut self, cursor: InodeId) -> Result<bool> {
        if self.seen.len() >= MAX_RENAME_ANCESTORS
            || cursor == self.src_dir_id
            || cursor == 0
            || !self.seen.insert(cursor)
        {
            return Err(MetaDbError::InvalidRenameCycle {
                dir_id: self.src_dir_id,
                dst_pid: self.dst_pid,
            });
        }
        if cursor <= ROOT_INODE_ID {
            return Ok(false);
        }
        Ok(true)
    }

    /// Walks parents starting at `dst_pid`. `parent_of` returns `None` when that directory's
    /// authoritative record is not visible to the caller; the walk then stops without
    /// declaring a cycle (the caller could not see further ancestors).
    pub fn walk(
        &mut self,
        mut parent_of: impl FnMut(InodeId) -> Result<Option<InodeId>>,
    ) -> Result<()> {
        let mut cursor = self.dst_pid;
        loop {
            if !self.observe(cursor)? {
                return Ok(());
            }
            match parent_of(cursor)? {
                Some(parent) => cursor = parent,
                None => return Ok(()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deep_chain_into_descendant_is_rejected() {
        // 4999 parent hops from dst=5000 down to src=2. A 1024-step walk that then
        // succeeded would miss this cycle.
        let src = 2u64;
        let dst = 5_000u64;
        let err = RenameCycleWalker::new(src, dst)
            .walk(|cursor| {
                if cursor <= ROOT_INODE_ID {
                    Ok(None)
                } else {
                    Ok(Some(cursor - 1))
                }
            })
            .unwrap_err();
        assert_eq!(
            err,
            MetaDbError::InvalidRenameCycle {
                dir_id: src,
                dst_pid: dst
            }
        );
    }

    #[test]
    fn sibling_destination_reaches_root() {
        RenameCycleWalker::new(50, 51)
            .walk(|cursor| {
                if cursor == 51 {
                    Ok(Some(ROOT_INODE_ID))
                } else {
                    Ok(None)
                }
            })
            .unwrap();
    }
}
