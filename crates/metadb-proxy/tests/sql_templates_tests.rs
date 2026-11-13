use std::sync::Arc;

use metadb_cluster::{
    CompositeMetaCache, DurableCellManager, DurableGlobalManager, DurableIdAllocator,
};
use metadb_core::{CompositeKeyRange, HybridLogicalClock, MetaDbError, ROOT_INODE_ID};
use metadb_proxy::{ProxySqlResult, SqlParam, SqlTemplateParser, StatelessProxyRouter};
use metadb_raft::{RaftNodeOptions, ShardGroupHost};
use metadb_storage::{LsmEngineConfig, ShardedBlockCache};
use metadb_tx::{AuxDispatcher, DistributedTxRouter};
use tempfile::tempdir;

/// T-SQL-1: Exercises ALL 20 canonical metadata SQL templates (both escaped inline literals and
/// `?` positional bind parameters), cursor pagination, and strict `'_attr_'` isolation (`Resolution N2` / `G-P1`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_all_twenty_metadata_sql_templates_and_attr_isolation() {
    let dir = tempdir().unwrap();
    let alloc = Arc::new(DurableIdAllocator::open(dir.path().join("alloc")).unwrap());
    let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    let meta_cache = Arc::new(CompositeMetaCache::new());

    let host =
        Arc::new(ShardGroupHost::open(1, dir.path().join("be_1"), cache, clock.clone()).unwrap());
    let cell = DurableCellManager::open_or_bootstrap(
        1,
        dir.path().join("cell_1"),
        host.clone(),
        "127.0.0.1:0".to_string(),
        true,
        4 * 1024 * 1024,
        LsmEngineConfig::default(),
        RaftNodeOptions::default(),
        alloc.clone(),
        meta_cache.clone(),
    )
    .await
    .unwrap();

    let gm = Arc::new(DurableGlobalManager::new(clock.clone(), alloc, meta_cache));
    gm.register_cell(CompositeKeyRange::all_user_tables(), cell.clone());

    let tx = Arc::new(DistributedTxRouter::new(1, clock.clone(), gm.clone()));
    tx.init_dir_attr(ROOT_INODE_ID, ROOT_INODE_ID, 0, 0o755)
        .await
        .unwrap();
    let proxy = StatelessProxyRouter::new(tx);

    // Template #2: Mkdir (`?` params)
    proxy
        .execute_sql(
            "INSERT INTO MetaTable (pid, name, id, type, uid, mode) VALUES (?, ?, ?, 'DIR', ?, ?)",
            &[
                SqlParam::U64(1),
                SqlParam::Str("data_dir".to_string()),
                SqlParam::U64(100),
                SqlParam::U64(42),
                SqlParam::U64(0o755),
            ],
        )
        .await
        .unwrap();

    // Template #1: CreateFile with escaped single quote `'it''s_a_file.txt'` AND `_prefix` file!
    proxy
        .execute_sql(
            "INSERT INTO MetaTable (pid, name, id, type, fid, uid, mode, size) VALUES (100, 'it''s_a_file.txt', 1001, 'FILE', 7001, 42, 420, 1024)",
            &[],
        )
        .await
        .unwrap();
    proxy
        .execute_sql(
            "INSERT INTO MetaTable (pid, name, id, type, fid, uid, mode, size) VALUES (?, ?, ?, 'FILE', ?, ?, ?, ?)",
            &[
                SqlParam::U64(100),
                SqlParam::Str("_user_hidden.txt".to_string()),
                SqlParam::U64(1002),
                SqlParam::U64(7002),
                SqlParam::U64(42),
                SqlParam::U64(0o644),
                SqlParam::I64(2048),
            ],
        )
        .await
        .unwrap();

    // Verify reserved `'_attr_'` is rejected on CreateFile AND parameterized Lookup (#8)!
    assert!(matches!(
        SqlTemplateParser::parse(
            "INSERT INTO MetaTable (pid, name, id, type, fid, uid, mode, size) VALUES (100, '_attr_', 9, 'FILE', 9, 1, 420, 1)"
        ),
        Err(MetaDbError::ReservedEntryName(_))
    ));
    assert!(matches!(
        SqlTemplateParser::parse_with_params(
            "SELECT * FROM MetaTable WHERE pid = ? AND name = ?",
            &[SqlParam::U64(100), SqlParam::Str("_attr_".to_string())]
        ),
        Err(MetaDbError::ReservedEntryName(_))
    ));

    // Template #7: DirStat (`name = '_attr_'`)
    let ProxySqlResult::Attr(dstat) = proxy
        .execute_sql(
            "SELECT * FROM MetaTable WHERE pid = ? AND name = '_attr_'",
            &[SqlParam::U64(100)],
        )
        .await
        .unwrap()
    else {
        panic!("expected Attr");
    };
    assert_eq!(dstat.num_children, 2);

    // Template #8: Lookup escaped `'it''s_a_file.txt'`
    let ProxySqlResult::OptionalEntry(Some(found)) = proxy
        .execute_sql(
            "SELECT * FROM MetaTable WHERE pid = 100 AND name = 'it''s_a_file.txt'",
            &[],
        )
        .await
        .unwrap()
    else {
        panic!("expected Lookup hit");
    };
    assert_eq!(found.key.name, "it's_a_file.txt");

    // Templates #9, #10 (paginated), #11 (`LIKE '_%'` — MUST NOT leak internal `'_attr_'`!)
    let ProxySqlResult::Entries { entries: all_e, .. } = proxy
        .execute_sql(
            "SELECT * FROM MetaTable WHERE pid = 100 ORDER BY name ASC",
            &[],
        )
        .await
        .unwrap()
    else {
        panic!("expected Entries");
    };
    assert_eq!(all_e.len(), 2);

    let ProxySqlResult::Entries {
        entries: page_1,
        next_cursor,
    } = proxy
        .execute_sql(
            "SELECT * FROM MetaTable WHERE pid = ? AND name > ? ORDER BY name ASC LIMIT ?",
            &[
                SqlParam::U64(100),
                SqlParam::Str(String::new()),
                SqlParam::U64(1),
            ],
        )
        .await
        .unwrap()
    else {
        panic!("expected paginated Entries");
    };
    assert_eq!(page_1.len(), 1);
    assert!(next_cursor.is_some());

    let ProxySqlResult::Entries {
        entries: underscore_files,
        ..
    } = proxy
        .execute_sql(
            "SELECT * FROM MetaTable WHERE pid = 100 AND name LIKE '_%' ORDER BY name ASC LIMIT 100",
            &[],
        )
        .await
        .unwrap()
    else {
        panic!("expected LIKE Entries");
    };
    assert_eq!(underscore_files.len(), 1);
    assert_eq!(underscore_files[0].key.name, "_user_hidden.txt");

    // Templates #12 (chmod), #13 (chown), #14 (size+fid), #15 (modify_time)
    proxy
        .execute_sql(
            "UPDATE MetaTable SET mode = 384 WHERE pid = 100 AND name = '_user_hidden.txt'",
            &[],
        )
        .await
        .unwrap();
    proxy
        .execute_sql(
            "UPDATE MetaTable SET uid = 99 WHERE pid = 100 AND name = '_user_hidden.txt'",
            &[],
        )
        .await
        .unwrap();
    proxy
        .execute_sql(
            "UPDATE MetaTable SET size = 8192, fid = 7999 WHERE pid = 100 AND name = '_user_hidden.txt'",
            &[],
        )
        .await
        .unwrap();
    proxy
        .execute_sql(
            "UPDATE MetaTable SET modify_time = ? WHERE pid = 100 AND name = '_user_hidden.txt'",
            &[SqlParam::U64(clock.now().0)],
        )
        .await
        .unwrap();

    // Template #5 (exclusive rename) & Template #6 (POSIX overwrite rename)
    proxy
        .execute_sql(
            "UPDATE MetaTable SET pid = 100, name = 'renamed.txt' WHERE pid = 100 AND name = 'it''s_a_file.txt'",
            &[],
        )
        .await
        .unwrap();
    proxy
        .execute_sql(
            "REPLACE INTO MetaTable (pid, name) SELECT 100, 'renamed.txt' FROM MetaTable WHERE pid = 100 AND name = '_user_hidden.txt'",
            &[],
        )
        .await
        .unwrap();

    // Drain async outbox so QuotaTable & SecondaryIndexTable reflect all mutations, then test #16–#20!
    let (_, s1_grp, s1_h) = cell.lookup_local_handle(1).unwrap();
    AuxDispatcher::drain_shard_outbox(&s1_grp, &s1_h, gm.as_ref(), &clock)
        .await
        .unwrap();

    // Template #17 (SetQuotaLimit), #16 (GetQuota), #18 (ListQuotas)
    proxy
        .execute_sql(
            "INSERT INTO QuotaTable (uid, quota_limit_bytes) VALUES (99, 1000000) ON CONFLICT (uid) DO UPDATE SET quota_limit_bytes = 1000000",
            &[],
        )
        .await
        .unwrap();
    let ProxySqlResult::Quota(Some(q99)) = proxy
        .execute_sql("SELECT * FROM QuotaTable WHERE uid = 99", &[])
        .await
        .unwrap()
    else {
        panic!("expected Quota");
    };
    assert_eq!(q99.used_bytes, 8192);
    assert_eq!(q99.quota_limit_bytes, 1_000_000);

    let ProxySqlResult::Quotas(q_list) = proxy
        .execute_sql(
            "SELECT * FROM QuotaTable WHERE uid >= 1 ORDER BY uid ASC LIMIT 10",
            &[],
        )
        .await
        .unwrap()
    else {
        panic!("expected Quotas");
    };
    assert!(q_list.len() >= 2);

    // Template #19 (GetSecondaryIndex) & #20 (ScanSecondaryIndex)
    let ProxySqlResult::SecondaryIndex(Some(idx_rec)) = proxy
        .execute_sql("SELECT * FROM SecondaryIndexTable WHERE fid = 7999", &[])
        .await
        .unwrap()
    else {
        panic!("expected SecondaryIndex");
    };
    assert_eq!(idx_rec.name, "renamed.txt");

    let ProxySqlResult::SecondaryIndices(scanned_idx) = proxy
        .execute_sql(
            "SELECT * FROM SecondaryIndexTable WHERE fid > 0 ORDER BY fid ASC LIMIT 50",
            &[],
        )
        .await
        .unwrap()
    else {
        panic!("expected SecondaryIndices");
    };
    assert_eq!(scanned_idx.len(), 1);

    // Templates #3 (Unlink) & #4 (Rmdir)
    proxy
        .execute_sql(
            "DELETE FROM MetaTable WHERE pid = 100 AND name = 'renamed.txt'",
            &[],
        )
        .await
        .unwrap();
    proxy
        .execute_sql(
            "RMDIR FROM MetaTable WHERE pid = 1 AND name = 'data_dir'",
            &[],
        )
        .await
        .unwrap();

    host.shutdown_all().await.unwrap();
}
