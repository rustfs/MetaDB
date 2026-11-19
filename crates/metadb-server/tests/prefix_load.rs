//! Prefix create / SetAttr / delete load.
//!
//! `ci_prefix_load` is the GitHub Actions profile: 2 prefixes, 8 creates each,
//! then 8 SetAttr overwrites, then 8 deletes, concurrency 4. It ignores
//! `METADB_LOAD_*` and never runs the official shape.
//!
//! `official_prefix_load` is ignored. It dials `METADB_LOAD_ENDPOINTS` and, unless
//! a dry-run override is set, runs 100 prefixes × 10,000 creates (1,000,000),
//! then 100,000 SetAttr overwrites, then 100,000 deletes, at concurrency 64.
//! GitHub Actions refuses that create count. This file does not record a
//! throughput number, and it does not change server configuration to chase one.
//!
//! See `docs/en/testing.md` for the three-VM command.

use std::env;
use std::time::{Duration, Instant};

use metadb_core::ROOT_INODE_ID;
use metadb_proto::control_plane_service_client::ControlPlaneServiceClient;
use metadb_proto::metadata_service_client::MetadataServiceClient;
use metadb_proto::{
    AllocateIdBlockRequest, CreateFileRequest, ErrorDetailProto, LookupRequest, MkdirRequest,
    SetAttrRequest, UnlinkRequest,
};
use metadb_server::{BearerAuthClient, PersistentMetaDbCluster, ServerTomlConfig};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};
use tonic::Code;

const CREATE_SIZE: i64 = 4096;
const SETATTR_SIZE: i64 = 8192;
const FILE_UID: u64 = 7;
const FILE_MODE: u32 = 0o644;
const DIR_MODE: u32 = 0o755;

/// Actions must stay on the small profile. The official create count is 1_000_000.
const ACTIONS_CREATE_CEILING: usize = 10_000;

type MetaClient = MetadataServiceClient<InterceptedService<Channel, BearerAuthClient>>;
type CpClient = ControlPlaneServiceClient<InterceptedService<Channel, BearerAuthClient>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Create,
    SetAttr,
    Delete,
}

impl Phase {
    fn label(self) -> &'static str {
        match self {
            Phase::Create => "create",
            Phase::SetAttr => "setattr",
            Phase::Delete => "delete",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LoadProfile {
    name: &'static str,
    prefixes: usize,
    creates_per_prefix: usize,
    setattr_ops: usize,
    delete_ops: usize,
    concurrency: usize,
    request_timeout: Duration,
}

impl LoadProfile {
    fn ci() -> Self {
        Self {
            name: "ci",
            prefixes: 2,
            creates_per_prefix: 8,
            setattr_ops: 8,
            delete_ops: 8,
            concurrency: 4,
            request_timeout: Duration::from_secs(30),
        }
    }

    fn official() -> Self {
        Self {
            name: "official",
            prefixes: 100,
            creates_per_prefix: 10_000,
            setattr_ops: 100_000,
            delete_ops: 100_000,
            concurrency: 64,
            request_timeout: Duration::from_secs(120),
        }
    }

    fn total_creates(&self) -> usize {
        self.prefixes.saturating_mul(self.creates_per_prefix)
    }

    fn validate(&self) -> Result<(), String> {
        if self.prefixes == 0 || self.creates_per_prefix == 0 || self.concurrency == 0 {
            return Err("prefixes, creates_per_prefix, and concurrency must be non-zero".into());
        }
        let total = self
            .prefixes
            .checked_mul(self.creates_per_prefix)
            .ok_or_else(|| "prefix create count overflows".to_string())?;
        if self.setattr_ops > total || self.delete_ops > total {
            return Err(format!(
                "setattr ({}) and deletes ({}) must be <= creates ({total})",
                self.setattr_ops, self.delete_ops
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
struct LoadPlan {
    dir_ids: Vec<u64>,
    inode_start: u64,
    fid_start: u64,
    creates_per_prefix: usize,
}

struct FileSpec {
    pid: u64,
    name: String,
    inode_id: u64,
    fid: u64,
}

impl LoadPlan {
    fn file(&self, index: usize) -> FileSpec {
        let prefix = index / self.creates_per_prefix;
        let seq = index % self.creates_per_prefix;
        FileSpec {
            pid: self.dir_ids[prefix],
            name: format!("f{seq}"),
            inode_id: self.inode_start + index as u64,
            fid: self.fid_start + index as u64,
        }
    }
}

enum OpError {
    Retry(String),
    Fatal(String),
}

fn classify_status(status: tonic::Status) -> OpError {
    match status.code() {
        Code::Unavailable | Code::DeadlineExceeded | Code::Aborted | Code::ResourceExhausted => {
            OpError::Retry(status.to_string())
        }
        _ => OpError::Fatal(format!("{}: {}", status.code(), status.message())),
    }
}

fn classify_app(error: Option<ErrorDetailProto>) -> Result<(), OpError> {
    let Some(error) = error else {
        return Ok(());
    };
    if matches!(
        error.error_kind.as_str(),
        "NotLeader" | "RetryableConsensus" | "EpochMismatch"
    ) {
        Err(OpError::Retry(format!(
            "{}: {}",
            error.error_kind, error.message
        )))
    } else {
        Err(OpError::Fatal(format!(
            "{}: {}",
            error.error_kind, error.message
        )))
    }
}

fn shard_range(total: usize, workers: usize, tid: usize) -> std::ops::Range<usize> {
    let base = total / workers;
    let remainder = total % workers;
    let start = tid * base + tid.min(remainder);
    let count = base + usize::from(tid < remainder);
    start..start + count
}

/// First try uses `index`, so a successful run spreads RPCs across the voter list.
/// Later attempts rotate from that slot.
fn endpoint_index(client_count: usize, index: usize, attempt: usize) -> usize {
    (index + attempt) % client_count.max(1)
}

fn prefix_dir_name(dir_id: u64) -> String {
    format!("loadp{dir_id}")
}

/// `None` when the variable is unset. A present value that is empty or not an
/// unsigned integer is an error, so a typo cannot fall through to the official load.
fn parse_override(key: &str, raw: Option<String>) -> Result<Option<usize>, String> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(format!(
            "{key} is set but empty; refusing to fall back to the official load"
        ));
    }
    trimmed.parse::<usize>().map(Some).map_err(|_| {
        format!(
            "{key}={raw:?} is not an unsigned integer; refusing to fall back to the official load"
        )
    })
}

fn env_usize(key: &str) -> Result<Option<usize>, String> {
    let raw = match env::var(key) {
        Ok(value) => Some(value),
        Err(env::VarError::NotPresent) => None,
        Err(env::VarError::NotUnicode(value)) => {
            return Err(format!(
                "{key} is not Unicode ({value:?}); refusing to fall back to the official load"
            ));
        }
    };
    parse_override(key, raw)
}

fn refuse_actions_scale(profile: &LoadProfile) {
    if env::var("GITHUB_ACTIONS").ok().as_deref() == Some("true")
        && profile.total_creates() > ACTIONS_CREATE_CEILING
    {
        panic!(
            "refusing prefix load of {} creates inside GitHub Actions; the official 1_000_000 profile is manual",
            profile.total_creates()
        );
    }
}

fn normalize_endpoint(raw: &str) -> String {
    let raw = raw.trim();
    if raw.starts_with("http://") || raw.starts_with("https://") {
        raw.to_string()
    } else {
        format!("http://{raw}")
    }
}

async fn dial(url: &str, token: &str, timeout: Duration) -> Result<(MetaClient, CpClient), String> {
    let channel = Endpoint::from_shared(url.to_string())
        .map_err(|err| err.to_string())?
        .connect_timeout(Duration::from_secs(5))
        .timeout(timeout)
        .connect()
        .await
        .map_err(|err| format!("connect {url}: {err}"))?;
    let auth = BearerAuthClient::new(token);
    Ok((
        MetadataServiceClient::with_interceptor(channel.clone(), auth.clone()),
        ControlPlaneServiceClient::with_interceptor(channel, auth),
    ))
}

async fn allocate(cp: &mut CpClient, kind: u32, count: u64) -> Result<u64, OpError> {
    let grant = cp
        .allocate_id_block(AllocateIdBlockRequest {
            request_uuid_be: Vec::new(),
            id_kind: kind,
            count,
            preferred_shard_id: 0,
        })
        .await
        .map_err(classify_status)?
        .into_inner();
    classify_app(grant.error)?;
    if grant.count != count || (count > 0 && grant.start_id.checked_add(count - 1).is_none()) {
        return Err(OpError::Fatal(format!(
            "allocator kind {kind} returned start {} count {} for request {count}",
            grant.start_id, grant.count
        )));
    }
    Ok(grant.start_id)
}

async fn allocate_from_any(clients: &[CpClient], kind: u32, count: u64) -> Result<u64, String> {
    let mut last = String::from("no control-plane client");
    for mut client in clients.iter().cloned() {
        match allocate(&mut client, kind, count).await {
            Ok(start) => return Ok(start),
            Err(OpError::Fatal(msg)) => return Err(msg),
            Err(OpError::Retry(msg)) => last = msg,
        }
    }
    Err(format!(
        "allocate kind {kind} failed on every endpoint: {last}"
    ))
}

async fn apply_once(
    client: &mut MetaClient,
    plan: &LoadPlan,
    phase: Phase,
    index: usize,
) -> Result<(), OpError> {
    let spec = plan.file(index);
    match phase {
        Phase::Create => {
            let response = client
                .create_file(CreateFileRequest {
                    header: None,
                    pid: spec.pid,
                    name: spec.name,
                    inode_id: spec.inode_id,
                    fid: spec.fid,
                    uid: FILE_UID,
                    mode: FILE_MODE,
                    size_bytes: CREATE_SIZE,
                })
                .await
                .map_err(classify_status)?
                .into_inner();
            classify_app(response.error)
        }
        Phase::SetAttr => {
            let response = client
                .set_attr(SetAttrRequest {
                    header: None,
                    pid: spec.pid,
                    name: spec.name,
                    has_mode: false,
                    mode: 0,
                    has_uid: false,
                    uid: 0,
                    has_size_bytes: true,
                    size_bytes: SETATTR_SIZE,
                    has_fid: false,
                    fid: 0,
                    has_modify_time_hlc: false,
                    modify_time_hlc: 0,
                })
                .await
                .map_err(classify_status)?
                .into_inner();
            classify_app(response.error)
        }
        Phase::Delete => {
            let response = client
                .unlink(UnlinkRequest {
                    header: None,
                    pid: spec.pid,
                    name: spec.name,
                })
                .await
                .map_err(classify_status)?
                .into_inner();
            classify_app(response.error)
        }
    }
}

async fn apply_with_retry(
    clients: &[MetaClient],
    plan: &LoadPlan,
    phase: Phase,
    index: usize,
) -> Result<(), String> {
    let attempts = clients.len().saturating_mul(4).max(4);
    let mut last = String::from("no attempt");
    for attempt in 0..attempts {
        let mut client = clients[endpoint_index(clients.len(), index, attempt)].clone();
        match apply_once(&mut client, plan, phase, index).await {
            Ok(()) => return Ok(()),
            Err(OpError::Fatal(msg)) => {
                return Err(format!(
                    "{} index {index} ({}) failed: {msg}",
                    phase.label(),
                    plan.file(index).name
                ));
            }
            Err(OpError::Retry(msg)) => last = msg,
        }
        tokio::time::sleep(Duration::from_millis(15 * (attempt as u64 + 1))).await;
    }
    Err(format!(
        "{} index {index} exhausted retries: {last}",
        phase.label()
    ))
}

async fn run_phase(
    clients: &[MetaClient],
    plan: &LoadPlan,
    phase: Phase,
    total: usize,
    concurrency: usize,
) -> Result<(), String> {
    if total == 0 {
        println!(
            "prefix_load phase={} requested=0 succeeded=0 elapsed_ms=0",
            phase.label()
        );
        return Ok(());
    }
    let started = Instant::now();
    let workers = concurrency.min(total);
    let clients = clients.to_vec();
    let plan = plan.clone();
    let mut joins = Vec::with_capacity(workers);
    for tid in 0..workers {
        let range = shard_range(total, workers, tid);
        let clients = clients.clone();
        let plan = plan.clone();
        joins.push(tokio::spawn(async move {
            for index in range {
                apply_with_retry(&clients, &plan, phase, index).await?;
            }
            Ok::<(), String>(())
        }));
    }
    for join in joins {
        join.await
            .map_err(|err| format!("{} worker panicked: {err}", phase.label()))??;
    }
    println!(
        "prefix_load phase={} requested={total} succeeded={total} elapsed_ms={}",
        phase.label(),
        started.elapsed().as_millis()
    );
    Ok(())
}

async fn lookup_size(
    clients: &[MetaClient],
    plan: &LoadPlan,
    index: usize,
) -> Result<Option<i64>, String> {
    let spec = plan.file(index);
    let attempts = clients.len().saturating_mul(4).max(4);
    let mut last = String::from("no attempt");
    for attempt in 0..attempts {
        let mut client = clients[endpoint_index(clients.len(), index, attempt)].clone();
        match client
            .lookup(LookupRequest {
                header: None,
                pid: spec.pid,
                name: spec.name.clone(),
            })
            .await
        {
            Ok(response) => {
                let body = response.into_inner();
                match classify_app(body.error) {
                    Ok(()) => {
                        if !body.found {
                            return Ok(None);
                        }
                        let size = body
                            .entry
                            .as_ref()
                            .and_then(|entry| entry.attr.as_ref())
                            .map(|attr| attr.size_bytes)
                            .ok_or_else(|| {
                                format!("lookup {} found an entry without attributes", spec.name)
                            })?;
                        return Ok(Some(size));
                    }
                    Err(OpError::Fatal(msg)) => return Err(format!("lookup {}: {msg}", spec.name)),
                    Err(OpError::Retry(msg)) => last = msg,
                }
            }
            Err(status) => match classify_status(status) {
                OpError::Fatal(msg) => return Err(format!("lookup {}: {msg}", spec.name)),
                OpError::Retry(msg) => last = msg,
            },
        }
        tokio::time::sleep(Duration::from_millis(15 * (attempt as u64 + 1))).await;
    }
    Err(format!("lookup {} exhausted retries: {last}", spec.name))
}

async fn run_load(endpoints: &[String], token: &str, profile: &LoadProfile) -> Result<(), String> {
    profile.validate()?;
    refuse_actions_scale(profile);
    if endpoints.is_empty() {
        return Err("prefix load requires at least one endpoint".into());
    }
    println!(
        "prefix_load profile={} endpoints={} prefixes={} creates_per_prefix={} creates={} setattr={} deletes={} concurrency={}",
        profile.name,
        endpoints.len(),
        profile.prefixes,
        profile.creates_per_prefix,
        profile.total_creates(),
        profile.setattr_ops,
        profile.delete_ops,
        profile.concurrency
    );

    let mut metadata = Vec::with_capacity(endpoints.len());
    let mut control = Vec::with_capacity(endpoints.len());
    for endpoint in endpoints {
        let (meta, cp) = dial(endpoint, token, profile.request_timeout).await?;
        metadata.push(meta);
        control.push(cp);
    }

    let dir_start = allocate_from_any(&control, 2, profile.prefixes as u64).await?;
    let inode_start = allocate_from_any(&control, 1, profile.total_creates() as u64).await?;
    let fid_start = allocate_from_any(&control, 3, profile.total_creates() as u64).await?;

    let mut dir_ids = Vec::with_capacity(profile.prefixes);
    for index in 0..profile.prefixes {
        let dir_id = dir_start + index as u64;
        let name = prefix_dir_name(dir_id);
        let attempts = metadata.len().saturating_mul(4).max(4);
        let mut last = String::from("no attempt");
        let mut created = false;
        for attempt in 0..attempts {
            let slot = endpoint_index(metadata.len(), index, attempt);
            match metadata[slot]
                .mkdir(MkdirRequest {
                    header: None,
                    pid: ROOT_INODE_ID,
                    name: name.clone(),
                    dir_id,
                    uid: FILE_UID,
                    mode: DIR_MODE,
                })
                .await
            {
                Ok(response) => match classify_app(response.into_inner().error) {
                    Ok(()) => {
                        created = true;
                        break;
                    }
                    Err(OpError::Fatal(msg)) => return Err(format!("mkdir {name}: {msg}")),
                    Err(OpError::Retry(msg)) => last = msg,
                },
                Err(status) => match classify_status(status) {
                    OpError::Fatal(msg) => return Err(format!("mkdir {name}: {msg}")),
                    OpError::Retry(msg) => last = msg,
                },
            }
            tokio::time::sleep(Duration::from_millis(15 * (attempt as u64 + 1))).await;
        }
        if !created {
            return Err(format!("mkdir {name} exhausted retries: {last}"));
        }
        dir_ids.push(dir_id);
    }

    let plan = LoadPlan {
        dir_ids,
        inode_start,
        fid_start,
        creates_per_prefix: profile.creates_per_prefix,
    };
    run_phase(
        &metadata,
        &plan,
        Phase::Create,
        profile.total_creates(),
        profile.concurrency,
    )
    .await?;
    run_phase(
        &metadata,
        &plan,
        Phase::SetAttr,
        profile.setattr_ops,
        profile.concurrency,
    )
    .await?;
    if profile.setattr_ops > 0 {
        let size = lookup_size(&metadata, &plan, 0).await?;
        if size != Some(SETATTR_SIZE) {
            return Err(format!(
                "setattr sample file size is {size:?}, expected {SETATTR_SIZE}"
            ));
        }
    }
    run_phase(
        &metadata,
        &plan,
        Phase::Delete,
        profile.delete_ops,
        profile.concurrency,
    )
    .await?;
    if profile.delete_ops > 0 {
        let size = lookup_size(&metadata, &plan, 0).await?;
        if size.is_some() {
            return Err(format!(
                "deleted sample file is still visible with size {size:?}"
            ));
        }
    }
    let total = profile.total_creates();
    if total > profile.delete_ops {
        let index = total - 1;
        let size = lookup_size(&metadata, &plan, index).await?;
        let expected = if index < profile.setattr_ops {
            SETATTR_SIZE
        } else {
            CREATE_SIZE
        };
        if size != Some(expected) {
            return Err(format!(
                "survivor file size is {size:?}, expected {expected}"
            ));
        }
    }
    Ok(())
}

fn official_profile_from_env() -> Result<LoadProfile, String> {
    let mut profile = LoadProfile::official();
    let mut overridden = false;
    for (key, slot) in [
        ("METADB_LOAD_PREFIXES", &mut profile.prefixes),
        (
            "METADB_LOAD_CREATES_PER_PREFIX",
            &mut profile.creates_per_prefix,
        ),
        ("METADB_LOAD_SETATTR", &mut profile.setattr_ops),
        ("METADB_LOAD_DELETES", &mut profile.delete_ops),
        ("METADB_LOAD_CONCURRENCY", &mut profile.concurrency),
    ] {
        if let Some(value) = env_usize(key)? {
            *slot = value;
            overridden = true;
        }
    }
    if overridden {
        profile.name = "official-override";
    }
    profile.validate()?;
    Ok(profile)
}

#[test]
fn ci_profile_stays_below_the_official_load() {
    let ci = LoadProfile::ci();
    let official = LoadProfile::official();
    ci.validate().unwrap();
    official.validate().unwrap();
    assert_eq!(ci.total_creates(), 16);
    assert!(ci.total_creates() <= ACTIONS_CREATE_CEILING);
    assert_eq!(official.prefixes, 100);
    assert_eq!(official.creates_per_prefix, 10_000);
    assert_eq!(official.total_creates(), 1_000_000);
    assert_eq!(official.setattr_ops, 100_000);
    assert_eq!(official.delete_ops, 100_000);
    assert_eq!(official.concurrency, 64);
    assert_ne!(ci, official);
}

#[test]
fn invalid_override_does_not_select_the_official_profile() {
    let err = parse_override("METADB_LOAD_PREFIXES", Some("2x".into())).unwrap_err();
    assert!(
        err.contains("2x") && err.contains("refusing to fall back"),
        "{err}"
    );
    assert!(parse_override("METADB_LOAD_PREFIXES", None)
        .unwrap()
        .is_none());
    assert_eq!(
        parse_override("METADB_LOAD_PREFIXES", Some(" 2 ".into())).unwrap(),
        Some(2)
    );
    assert!(parse_override("METADB_LOAD_PREFIXES", Some("".into())).is_err());
}

#[test]
fn successful_ops_start_on_different_endpoints() {
    assert_eq!(endpoint_index(3, 0, 0), 0);
    assert_eq!(endpoint_index(3, 1, 0), 1);
    assert_eq!(endpoint_index(3, 2, 0), 2);
    assert_eq!(endpoint_index(3, 3, 0), 0);
    assert_eq!(endpoint_index(3, 1, 1), 2);
}

#[test]
fn prefix_names_include_the_allocated_directory_id() {
    assert_eq!(prefix_dir_name(40), "loadp40");
    assert_ne!(prefix_dir_name(40), prefix_dir_name(41));
    assert_ne!(prefix_dir_name(40), "loadp0");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ci_prefix_load() {
    let profile = LoadProfile::ci();
    assert!(profile.total_creates() <= ACTIONS_CREATE_CEILING);
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = dir.path().to_path_buf();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.security.allow_unauthenticated = true;
    let (cluster, addr) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    let endpoints = vec![format!("http://{addr}")];
    let result = run_load(&endpoints, "", &profile).await;
    cluster.shutdown().await.unwrap();
    result.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "official 100 prefixes x 10000 creates, then 100000 SetAttr, then 100000 deletes, concurrency 64; set METADB_LOAD_ENDPOINTS outside GitHub Actions"]
async fn official_prefix_load() {
    let raw = env::var("METADB_LOAD_ENDPOINTS").unwrap_or_default();
    let endpoints: Vec<String> = raw
        .split(',')
        .map(normalize_endpoint)
        .filter(|endpoint| endpoint != "http://")
        .collect();
    assert!(
        !endpoints.is_empty(),
        "set METADB_LOAD_ENDPOINTS to the three voter addresses; see docs/en/testing.md"
    );
    let token = env::var("METADB_LOAD_TOKEN")
        .or_else(|_| env::var("METADB_AUTH_TOKEN"))
        .unwrap_or_default();
    let profile = official_profile_from_env().unwrap();
    run_load(&endpoints, &token, &profile).await.unwrap();
}
