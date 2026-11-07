//! Leader schedule of an epoch from that epoch's stakes, as agave v4.3.0 computes it.
//!
//! Copied from agave `leader-schedule/src/lib.rs` (`stake_weighted_slot_leaders`, `sort_stakes`)
//! and `LeaderSchedule::new`. Mainnet keys the schedule by vote account (SIMD-0180,
//! `enable_vote_address_leader_schedule`): v4.3.0 has no identity-keyed variant left.

use {
    agave_random::weighted::WeightedU64Index,
    rand_chacha::{ChaChaRng, rand_core::SeedableRng},
    solana_leader_schedule::NUM_CONSECUTIVE_LEADER_SLOTS,
    solana_pubkey::Pubkey,
    std::iter,
};

/// `stakes`: (vote account, node identity, stake). Returns the leader identity of every slot of
/// the epoch (`slots_in_epoch` entries, indexed by slot index). Zero-stake accounts are ignored;
/// all-zero stakes panic, as in agave.
#[allow(dead_code)]
pub fn leader_schedule(epoch: u64, slots_in_epoch: usize, stakes: &[(Pubkey, Pubkey, u64)]) -> Vec<Pubkey> {
    let repeat = NUM_CONSECUTIVE_LEADER_SLOTS.get();
    let mut stakes: Vec<_> = stakes.iter().filter(|s| s.2 > 0).copied().collect();
    // Stake descending, ties by vote address descending (agave `sort_stakes`).
    stakes.sort_unstable_by(|l, r| r.2.cmp(&l.2).then(r.0.cmp(&l.0)));
    stakes.dedup_by(|l, r| l.2 == r.2 && l.0 == r.0);
    let (leaders, weights): (Vec<_>, Vec<_>) = stakes.into_iter().map(|(_, id, s)| (id, s)).unzip();
    let index = WeightedU64Index::new(weights).unwrap();
    let mut seed = [0u8; 32];
    seed[0..8].copy_from_slice(&epoch.to_le_bytes());
    let rng = &mut ChaChaRng::from_seed(seed);
    iter::repeat_with(|| leaders[index.sample(rng)])
        .take(slots_in_epoch / repeat)
        .flat_map(|l| iter::repeat_n(l, repeat))
        .collect()
}

#[cfg(test)]
mod tests {
    use {super::*, std::str::FromStr};

    /// agave `test_stake_leader_schedule_exact_order` case (epoch 1, stakes 10/20/30, 12 slots).
    #[test]
    fn agave_vector() {
        let ids: Vec<_> = (1u8..=3).map(|i| Pubkey::new_from_array([i; 32])).collect();
        let stakes: Vec<_> = ids.iter().zip([10, 20, 30]).map(|(id, s)| (*id, *id, s)).collect();
        let got: Vec<_> = leader_schedule(1, 12, &stakes).iter().step_by(NUM_CONSECUTIVE_LEADER_SLOTS.get()).map(|l| ids.iter().position(|i| i == l).unwrap()).collect();
        assert_eq!(got, [1, 1, 2]);
    }

    /// Needs RPC_URL in the environment. Epoch E+1's schedule comes from the stakes frozen at the
    /// start of E, so current `getVoteAccounts` -> next epoch. Exact only in the first minutes of
    /// an epoch: later, stakes, identities and balances have drifted from the frozen snapshot.
    /// Run: cargo test -p colibri --release leader_schedule -- --ignored --nocapture
    #[test]
    #[ignore]
    fn matches_rpc() {
        let url = std::env::var("RPC_URL").unwrap();
        let rpc = |body: String| -> serde_json::Value {
            let r = ureq::post(&url).header("content-type", "application/json").send(body).unwrap();
            serde_json::from_str(&r.into_body().read_to_string().unwrap()).unwrap()
        };
        let info = rpc(r#"{"jsonrpc":"2.0","id":1,"method":"getEpochInfo","params":[]}"#.into());
        let (epoch, slots) = (info["result"]["epoch"].as_u64().unwrap(), info["result"]["slotsInEpoch"].as_u64().unwrap());
        let abs = info["result"]["absoluteSlot"].as_u64().unwrap();
        let first = abs - info["result"]["slotIndex"].as_u64().unwrap();
        let next_first = first + slots;
        let va = rpc(r#"{"jsonrpc":"2.0","id":1,"method":"getVoteAccounts","params":[]}"#.into());
        let want = rpc(format!(r#"{{"jsonrpc":"2.0","id":1,"method":"getLeaderSchedule","params":[{next_first}]}}"#));
        // Agave drops vote accounts without a BLS key or with too little balance from epoch stakes
        // (SIMD-0357, bank.rs:1793). getVoteAccounts shows neither, and balances have moved since the
        // epoch boundary, so take the surviving set from the oracle: identities that lead a slot.
        let stakes: Vec<_> = ["current", "delinquent"].iter().flat_map(|k| va["result"][k].as_array().unwrap())
            .filter(|v| want["result"].get(v["nodePubkey"].as_str().unwrap()).is_some())
            .map(|v| {
                let p = |k: &str| Pubkey::from_str(v[k].as_str().unwrap()).unwrap();
                (p("votePubkey"), p("nodePubkey"), v["activatedStake"].as_u64().unwrap())
            }).collect();
        // Identities that changed since the boundary lead slots but own no vote account now: give
        // each the not yet taken vote account whose stake share is closest to its slot share.
        let mut stakes = stakes;
        let total: u64 = stakes.iter().map(|s| s.2).sum();
        let ids: std::collections::HashSet<_> = stakes.iter().map(|s| s.1.to_string()).collect();
        for (id, idxs) in want["result"].as_object().unwrap().iter().filter(|(id, _)| !ids.contains(*id)) {
            let target = total as f64 * idxs.as_array().unwrap().len() as f64 / slots as f64;
            let v = ["current", "delinquent"].iter().flat_map(|k| va["result"][k].as_array().unwrap())
                .filter(|v| want["result"].get(v["nodePubkey"].as_str().unwrap()).is_none())
                .min_by_key(|v| (v["activatedStake"].as_u64().unwrap() as f64 - target).abs() as u64).unwrap();
                stakes.push((Pubkey::from_str(v["votePubkey"].as_str().unwrap()).unwrap(), Pubkey::from_str(id).unwrap(), v["activatedStake"].as_u64().unwrap()));
        }
        let mine = leader_schedule(epoch + 1, slots as usize, &stakes);
        let (mut matched, mut total) = (0, 0);
        for (id, idxs) in want["result"].as_object().unwrap() {
            let id = Pubkey::from_str(id).unwrap();
            for i in idxs.as_array().unwrap() {
                total += 1;
                matched += (mine[i.as_u64().unwrap() as usize] == id) as u64;
            }
        }
        println!("epoch {}: {matched}/{total} slots match, {} vote accounts", epoch + 1, stakes.len());
        assert_eq!(matched, total);
    }
}
