//! White-box checks for [`plan_membership_voters`](metadb_cluster::plan_membership_voters).
//!
//! Growth takes the lowest registered ids. Shrink drops the highest ids. `ensure_node`
//! is inserted even when `target_voters` is already satisfied. These cases exercise the
//! planner directly and do not start Raft.

use std::collections::BTreeSet;

use metadb_cluster::plan_membership_voters;
use metadb_core::MetaDbError;

fn set_of(ids: &[u64]) -> BTreeSet<u64> {
    ids.iter().copied().collect()
}

fn plan(
    current: &[u64],
    registered: &[u64],
    target_voters: usize,
    ensure_node: Option<u64>,
) -> Result<BTreeSet<u64>, MetaDbError> {
    plan_membership_voters(
        &set_of(current),
        &set_of(registered),
        target_voters,
        ensure_node,
    )
}

#[test]
fn grow_fills_the_lowest_registered_ids() {
    let grown = plan(&[1], &[1, 4, 2, 3], 3, None).unwrap();
    assert_eq!(grown, set_of(&[1, 2, 3]));

    // 2 and 4 are already voters. The next id is the lowest registered id that is absent.
    let skipped = plan(&[2, 4], &[1, 2, 3, 4, 5], 3, None).unwrap();
    assert_eq!(skipped, set_of(&[1, 2, 4]));

    let unchanged = plan(&[1, 2, 3], &[1, 2, 3, 9], 3, None).unwrap();
    assert_eq!(unchanged, set_of(&[1, 2, 3]));

    let from_empty = plan(&[], &[5, 1, 4], 2, None).unwrap();
    assert_eq!(from_empty, set_of(&[1, 4]));
}

#[test]
fn shrink_drops_the_highest_ids() {
    let to_two = plan(&[1, 2, 3], &[1, 2, 3], 2, None).unwrap();
    assert_eq!(to_two, set_of(&[1, 2]));

    let to_one = plan(&[1, 2, 3], &[1, 2, 3], 1, None).unwrap();
    assert_eq!(to_one, set_of(&[1]));

    // Registered ids do not choose who is removed. The highest current ids go first.
    let unordered = plan(&[9, 2, 5, 7], &[1], 2, None).unwrap();
    assert_eq!(unordered, set_of(&[2, 5]));

    let already = plan(&[1, 2], &[9, 8, 7], 2, None).unwrap();
    assert_eq!(already, set_of(&[1, 2]));
}

#[test]
fn ensure_node_is_kept_even_when_the_target_is_already_met() {
    let kept = plan(&[1, 2], &[1, 2, 3], 2, Some(3)).unwrap();
    assert_eq!(kept, set_of(&[1, 2, 3]));

    let filled = plan(&[1], &[1, 2, 3], 3, Some(3)).unwrap();
    assert_eq!(filled, set_of(&[1, 2, 3]));

    let already_present = plan(&[1, 2, 3], &[1, 2, 3], 2, Some(2)).unwrap();
    assert_eq!(already_present, set_of(&[1, 2, 3]));

    // `ensure_node` does not shrink. A target below the ensured set stays at that set.
    let no_shrink = plan(&[1, 2, 3], &[1, 2, 3], 1, Some(3)).unwrap();
    assert_eq!(no_shrink, set_of(&[1, 2, 3]));

    // A node that is already a voter is valid even when the registry omits it.
    let current_only = plan(&[1, 7], &[1], 1, Some(7)).unwrap();
    assert_eq!(current_only, set_of(&[1, 7]));
}

#[test]
fn target_zero_without_ensure_node_is_refused() {
    let refused = plan(&[1, 2, 3], &[1, 2, 3], 0, None).unwrap_err();
    assert!(
        matches!(refused, MetaDbError::ConsensusError(ref message) if message.contains("at least 1")),
        "{refused}"
    );

    let empty = plan(&[], &[], 0, None).unwrap_err();
    assert!(
        matches!(empty, MetaDbError::ConsensusError(ref message) if message.contains("at least 1")),
        "{empty}"
    );
}

#[test]
fn ensure_node_zero_is_refused() {
    let refused = plan(&[1], &[1, 2], 2, Some(0)).unwrap_err();
    assert!(
        matches!(refused, MetaDbError::IdentityRejected(ref message) if message.contains("node id 0")),
        "{refused}"
    );
}

#[test]
fn not_enough_registered_nodes_is_an_error() {
    let short = plan(&[1], &[1, 2], 4, None).unwrap_err();
    assert!(
        matches!(short, MetaDbError::ConsensusError(ref message) if message.contains("need 4") && message.contains("can form 2")),
        "{short}"
    );

    let none_registered = plan(&[], &[], 1, None).unwrap_err();
    assert!(
        matches!(none_registered, MetaDbError::ConsensusError(ref message) if message.contains("need 1") && message.contains("can form 0")),
        "{none_registered}"
    );

    let unknown = plan(&[1], &[1, 2], 2, Some(9)).unwrap_err();
    assert!(
        matches!(unknown, MetaDbError::ConsensusError(ref message) if message.contains("not registered")),
        "{unknown}"
    );

    let ensure_still_short = plan(&[1], &[1, 2], 5, Some(2)).unwrap_err();
    assert!(
        matches!(ensure_still_short, MetaDbError::ConsensusError(ref message) if message.contains("need 5") && message.contains("can form 2")),
        "{ensure_still_short}"
    );
}
