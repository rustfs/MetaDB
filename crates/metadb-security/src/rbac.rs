//! Installed method grants.
//!
//! A grant is not the shared bearer and not a certificate. TLS is not
//! terminated, so a principal is a configured name plus its own credential.
//! An unknown principal, a credential mismatch, or a method that is not on
//! that principal's list is denied. There is no wildcard grant.

use std::collections::{BTreeMap, BTreeSet};

use tonic::metadata::MetadataMap;

use crate::spec::SecurityError;
use crate::AuthenticatedPrincipal;

pub const PRINCIPAL_HEADER: &str = "x-metadb-principal";
pub const CREDENTIAL_HEADER: &str = "x-metadb-credential";

/// One configured principal and the exact methods it may call.
pub struct RoleGrantSpec {
    pub principal: String,
    pub credential: String,
    pub allow: Vec<String>,
}

/// The principal this process presents on its own outbound calls.
#[derive(Clone)]
pub struct RolePresentation {
    principal: String,
    credential: String,
}

impl std::fmt::Debug for RolePresentation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RolePresentation")
            .field("principal", &self.principal)
            .finish()
    }
}

impl RolePresentation {
    pub fn from_parts(principal: impl Into<String>, credential: impl Into<String>) -> Self {
        Self {
            principal: principal.into(),
            credential: credential.into(),
        }
    }

    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// Adds the principal and credential headers. The credential is sensitive.
    pub fn apply_metadata(&self, metadata: &mut MetadataMap) -> Result<(), SecurityError> {
        let principal =
            tonic::metadata::MetadataValue::try_from(self.principal.as_str()).map_err(|_| {
                SecurityError::new(
                    "refusing to dial: the RBAC principal is not valid header text".to_string(),
                )
            })?;
        let mut credential = tonic::metadata::MetadataValue::try_from(self.credential.as_str())
            .map_err(|_| {
                SecurityError::new(
                    "refusing to dial: the RBAC credential is not valid header text".to_string(),
                )
            })?;
        credential.set_sensitive(true);
        metadata.insert(PRINCIPAL_HEADER, principal);
        metadata.insert(CREDENTIAL_HEADER, credential);
        Ok(())
    }
}

#[derive(Clone)]
struct Grant {
    credential: String,
    allow: BTreeSet<(&'static str, &'static str)>,
}

/// Roles installed for this process. Debug output omits credentials.
pub struct InstalledRoles {
    local: RolePresentation,
    grants: BTreeMap<String, Grant>,
}

impl std::fmt::Debug for InstalledRoles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstalledRoles")
            .field("local_principal", &self.local.principal)
            .field("principals", &self.grants.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl InstalledRoles {
    /// Installs the table. Fails closed on an empty table, a duplicate, a
    /// credential that repeats `shared_secret`, or a method that is not in
    /// the catalog. `*` is not a method.
    pub fn install(
        local_principal: &str,
        grants: &[RoleGrantSpec],
        shared_secret: &str,
    ) -> Result<Self, SecurityError> {
        if grants.is_empty() {
            return Err(SecurityError::new(
                "refusing to start: [security.rbac] is installed with no grants. An empty role table is not allow-all".to_string(),
            ));
        }
        let mut compiled = BTreeMap::new();
        let mut seen_credentials = BTreeSet::new();
        for grant in grants {
            let principal = check_principal(&grant.principal)?;
            if compiled.contains_key(principal) {
                return Err(SecurityError::new(format!(
                    "refusing to start: RBAC principal `{principal}` is listed more than once"
                )));
            }
            check_credential(&grant.credential, shared_secret)?;
            if !seen_credentials.insert(grant.credential.clone()) {
                return Err(SecurityError::new(
                    "refusing to start: two RBAC principals use the same credential".to_string(),
                ));
            }
            let mut allow = BTreeSet::new();
            for entry in &grant.allow {
                allow.insert(parse_allow(entry)?);
            }
            compiled.insert(
                principal.to_string(),
                Grant {
                    credential: grant.credential.clone(),
                    allow,
                },
            );
        }
        let local_name = check_principal(local_principal)?;
        let local_grant = compiled.get(local_name).ok_or_else(|| {
            SecurityError::new(format!(
                "refusing to start: [security.rbac].local_principal `{local_name}` has no grant"
            ))
        })?;
        Ok(Self {
            local: RolePresentation {
                principal: local_name.to_string(),
                credential: local_grant.credential.clone(),
            },
            grants: compiled,
        })
    }

    pub fn local(&self) -> &RolePresentation {
        &self.local
    }

    /// Checks the presented credential only. A method check is separate so a
    /// known probe path can return 404 after a valid principal.
    pub fn authenticate(
        &self,
        principal: &str,
        credential: &str,
    ) -> Result<AuthenticatedPrincipal, SecurityError> {
        let grant = self.grants.get(principal);
        let expected = grant.map(|item| item.credential.as_str()).unwrap_or("");
        if grant.is_some() && constant_time_eq(credential, expected) {
            Ok(AuthenticatedPrincipal::from_name(principal))
        } else {
            Err(SecurityError::denied())
        }
    }

    /// Checks the presented credential and the exact method. Denial does not
    /// say which of the principal, credential, or method failed, and it does
    /// not include either secret.
    pub fn authenticate_and_authorize(
        &self,
        principal: &str,
        credential: &str,
        service: &str,
        method: &str,
    ) -> Result<AuthenticatedPrincipal, SecurityError> {
        let principal = self.authenticate(principal, credential)?;
        self.authorize(&principal, service, method)?;
        Ok(principal)
    }

    pub fn authorize(
        &self,
        principal: &AuthenticatedPrincipal,
        service: &str,
        method: &str,
    ) -> Result<(), SecurityError> {
        let Some(grant) = self.grants.get(principal.as_str()) else {
            return Err(SecurityError::denied());
        };
        if grant
            .allow
            .iter()
            .any(|(s, m)| *s == service && *m == method)
        {
            Ok(())
        } else {
            Err(SecurityError::denied())
        }
    }
}

/// Exact `service/Method` authorization. The service and method must be in the catalog.
pub fn authorize_method(
    roles: &InstalledRoles,
    principal: &AuthenticatedPrincipal,
    service: &str,
    method: &str,
) -> Result<(), SecurityError> {
    roles.authorize(principal, service, method)
}

fn check_principal(value: &str) -> Result<&str, SecurityError> {
    if value.is_empty()
        || value.len() > 128
        || value.contains('*')
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
    {
        return Err(SecurityError::new(
            "refusing to start: an RBAC principal must be 1 to 128 characters from [A-Za-z0-9._:-] and must not contain '*'".to_string(),
        ));
    }
    Ok(value)
}

fn check_credential(credential: &str, shared_secret: &str) -> Result<(), SecurityError> {
    if credential.is_empty() || credential.trim().is_empty() || credential.len() > 512 {
        return Err(SecurityError::new(
            "refusing to start: an RBAC credential must contain a non-whitespace character and be at most 512 bytes".to_string(),
        ));
    }
    if !credential.is_ascii() || credential.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(SecurityError::new(
            "refusing to start: an RBAC credential must be printable ASCII so it can be a header"
                .to_string(),
        ));
    }
    if constant_time_eq(credential, shared_secret) {
        return Err(SecurityError::new(
            "refusing to start: an RBAC credential must not be the shared bearer. The shared secret is not a role".to_string(),
        ));
    }
    Ok(())
}

fn parse_allow(entry: &str) -> Result<(&'static str, &'static str), SecurityError> {
    let Some((service, method)) = entry.split_once('/') else {
        return Err(SecurityError::new(format!(
            "refusing to start: RBAC allow entry `{entry}` must be service/Method"
        )));
    };
    KNOWN
        .binary_search(&(service, method))
        .ok()
        .map(|index| KNOWN[index])
        .ok_or_else(|| {
            SecurityError::new(format!(
                "refusing to start: RBAC allow entry `{entry}` is not a known method. '*' is not accepted"
            ))
        })
}

fn constant_time_eq(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    let mut diff = left.len() ^ right.len();
    let n = left.len().max(right.len());
    for i in 0..n {
        let x = left.get(i).copied().unwrap_or(0);
        let y = right.get(i).copied().unwrap_or(0);
        diff |= usize::from(x ^ y);
    }
    diff == 0
}

/// Sorted catalog. A method that is not listed cannot be granted.
const KNOWN: &[(&str, &str)] = &[
    ("grpc.health.v1.Health", "Check"),
    ("grpc.health.v1.Health", "Watch"),
    ("metadb.http", "GetHealthz"),
    ("metadb.http", "GetLivez"),
    ("metadb.http", "GetMetrics"),
    ("metadb.http", "GetReadyz"),
    ("metadb.v1.ControlPlaneService", "AcquireDirRenameLease"),
    ("metadb.v1.ControlPlaneService", "ActivateSplit"),
    ("metadb.v1.ControlPlaneService", "AllocSplitPlan"),
    ("metadb.v1.ControlPlaneService", "AllocateIdBlock"),
    ("metadb.v1.ControlPlaneService", "CloseRemovedShard"),
    ("metadb.v1.ControlPlaneService", "EnableDualModeSplit"),
    ("metadb.v1.ControlPlaneService", "ExtendDirRenameLease"),
    ("metadb.v1.ControlPlaneService", "GetRoutes"),
    ("metadb.v1.ControlPlaneService", "OpenJoiningShard"),
    ("metadb.v1.ControlPlaneService", "RegisterCell"),
    ("metadb.v1.ControlPlaneService", "RegisterNode"),
    ("metadb.v1.ControlPlaneService", "ReleaseDirRenameLease"),
    ("metadb.v1.ControlPlaneService", "RemoveShardVoter"),
    ("metadb.v1.ControlPlaneService", "ScaleShardReplicas"),
    ("metadb.v1.ControlPlaneService", "StoreHeartbeat"),
    ("metadb.v1.ControlPlaneService", "TransferShardLeader"),
    ("metadb.v1.ControlPlaneService", "TriggerShardSplit"),
    ("metadb.v1.MetadataService", "CreateFile"),
    ("metadb.v1.MetadataService", "DirStat"),
    ("metadb.v1.MetadataService", "ExecuteSql"),
    ("metadb.v1.MetadataService", "GetDirPartitionStat"),
    ("metadb.v1.MetadataService", "GetQuota"),
    ("metadb.v1.MetadataService", "GetSecondaryIndex"),
    ("metadb.v1.MetadataService", "ListDir"),
    ("metadb.v1.MetadataService", "ListQuotas"),
    ("metadb.v1.MetadataService", "Lookup"),
    ("metadb.v1.MetadataService", "Mkdir"),
    ("metadb.v1.MetadataService", "Rmdir"),
    ("metadb.v1.MetadataService", "ScanSecondaryIndex"),
    ("metadb.v1.MetadataService", "SetAttr"),
    ("metadb.v1.MetadataService", "SetQuotaLimit"),
    ("metadb.v1.MetadataService", "Unlink"),
    ("metadb.v1.RaftTransportService", "AppendEntries"),
    ("metadb.v1.RaftTransportService", "ClientWrite"),
    ("metadb.v1.RaftTransportService", "InstallSnapshot"),
    ("metadb.v1.RaftTransportService", "ProbeTransferLeader"),
    ("metadb.v1.RaftTransportService", "TransferLeader"),
    ("metadb.v1.RaftTransportService", "Vote"),
    ("metadb.v1.TransactionService", "Abort2pc"),
    ("metadb.v1.TransactionService", "ApplyAuxBatch"),
    ("metadb.v1.TransactionService", "CheckTxnStatus"),
    ("metadb.v1.TransactionService", "CommitPrimary2pc"),
    ("metadb.v1.TransactionService", "CommitSecondary2pc"),
    ("metadb.v1.TransactionService", "Extend2pcLease"),
    ("metadb.v1.TransactionService", "ForwardRouter"),
    ("metadb.v1.TransactionService", "Prepare2pc"),
    ("metadb.v1.TransactionService", "Rename"),
];

#[cfg(test)]
mod tests {
    use super::KNOWN;

    #[test]
    fn catalog_is_sorted_for_binary_search() {
        assert!(KNOWN.windows(2).all(|pair| pair[0] < pair[1]));
    }
}
