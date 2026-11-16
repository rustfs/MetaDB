use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::Duration;

use metadb_core::{HlcTimestamp, Result, ShardEpoch};
use metadb_raft::{
    MetaDbRaftRequest, MetaDbTypeConfig, ShardApplyHandler, ShardStateMachine,
    ShardWatermarkTracker,
};
use metadb_storage::{
    DurableShardCheckpoint, LifecycleStorageEngine, LsmEngineConfig, ShardedBlockCache,
};
use openraft::storage::RaftStateMachine;
use openraft::type_config::alias::{CommittedLeaderIdOf, EntryOf, LogIdOf};
use openraft::vote::RaftLeaderId;
use openraft::EntryPayload;

struct PausedTwoKeyHandler {
    first_written: Mutex<Option<mpsc::Sender<()>>>,
    released: (Mutex<bool>, Condvar),
}

impl ShardApplyHandler for PausedTwoKeyHandler {
    fn current_epoch(&self) -> ShardEpoch {
        ShardEpoch::initial()
    }
    fn on_membership_changed(&self, _: ShardEpoch) {}
    fn apply_command(
        &self,
        index: u64,
        hlc: HlcTimestamp,
        _: &MetaDbRaftRequest,
        engine: &LifecycleStorageEngine,
    ) -> Result<Vec<u8>> {
        engine.apply_committed_put(b"first".to_vec(), b"present".to_vec(), hlc, index);
        self.first_written
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .send(())
            .unwrap();
        let (released, wake) = &self.released;
        let mut released = released.lock().unwrap();
        while !*released {
            released = wake.wait(released).unwrap();
        }
        engine.apply_committed_put(b"second".to_vec(), b"present".to_vec(), hlc, index);
        Ok(Vec::new())
    }
    fn on_snapshot_installed(&self, _: &DurableShardCheckpoint, _: &LifecycleStorageEngine) {}
}

/// A checkpoint must never include half of one committed state-machine command.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoint_waits_for_entire_apply_command() {
    let dir = tempfile::tempdir().unwrap();
    let (first_tx, first_rx) = mpsc::channel();
    let handler = Arc::new(PausedTwoKeyHandler {
        first_written: Mutex::new(Some(first_tx)),
        released: (Mutex::new(false), Condvar::new()),
    });
    let cache = Arc::new(ShardedBlockCache::new(1024 * 1024));
    let (sm, _) = ShardStateMachine::open(
        1,
        dir.path(),
        cache.clone(),
        LsmEngineConfig::default(),
        handler.clone(),
        Arc::new(ShardWatermarkTracker::default()),
    )
    .unwrap();
    let mut applying = sm.clone();
    let entry = EntryOf::<MetaDbTypeConfig> {
        log_id: LogIdOf::<MetaDbTypeConfig>::new(
            CommittedLeaderIdOf::<MetaDbTypeConfig>::new(1, 1),
            1,
        ),
        payload: EntryPayload::Normal(MetaDbRaftRequest {
            expected_epoch: ShardEpoch::initial(),
            hlc: HlcTimestamp(1),
            payload: Vec::new(),
        }),
    };
    let apply = tokio::spawn(async move {
        applying
            .apply(tokio_stream::iter([Ok((entry, None))]))
            .await
            .unwrap();
    });
    first_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let reading = sm.clone();
    let (read_tx, read_rx) = mpsc::channel();
    let read = tokio::task::spawn_blocking(move || {
        let guard = reading
            .acquire_snapshot(ShardEpoch::initial(), 0, None)
            .unwrap();
        read_tx.send(()).unwrap();
        reading
            .engine()
            .get_with_snapshot(b"second", &guard)
            .unwrap()
    });
    let read_completed_early = read_rx.recv_timeout(Duration::from_millis(200)).is_ok();
    let flushing = sm.clone();
    let (flush_tx, flush_rx) = mpsc::channel();
    let flush = tokio::task::spawn_blocking(move || {
        let result = flushing.flush_to_lsm();
        flush_tx.send(()).unwrap();
        result
    });
    let prematurely_finished = flush_rx.recv_timeout(Duration::from_millis(200)).is_ok();
    *handler.released.0.lock().unwrap() = true;
    handler.released.1.notify_all();
    apply.await.unwrap();
    assert_eq!(read.await.unwrap(), Some(b"present".to_vec()));
    assert!(!read_completed_early, "read cut exposed a partial command");
    let (mut flushed, _) = flush.await.unwrap().unwrap();
    // The reader may win the operation lock and briefly pin its snapshot when
    // the queued flush seals. Returning the old frontier is valid in that case.
    // Its guard has now been dropped, so complete the checkpoint before reopen.
    if flushed == 0 {
        flushed = sm.flush_to_lsm().unwrap().0;
    }
    assert_eq!(flushed, 1);
    drop(sm);
    let recovered_handler = Arc::new(metadb_raft::BasicKvApplyHandler::new(
        ShardEpoch::initial(),
        Arc::new(ShardWatermarkTracker::default()),
    ));
    let (recovered, _) = ShardStateMachine::open(
        1,
        dir.path(),
        cache,
        LsmEngineConfig::default(),
        recovered_handler,
        Arc::new(ShardWatermarkTracker::default()),
    )
    .unwrap();
    let guard = recovered.engine().acquire_linearizable_snapshot(1).unwrap();
    let second = recovered
        .engine()
        .get_with_snapshot(b"second", &guard)
        .unwrap();
    assert!(!prematurely_finished && second == Some(b"present".to_vec()), "checkpoint completed during partial apply: {prematurely_finished}; recovered second key: {second:?}; checkpoint index: {flushed}");
}
