#![no_main]

//! Fuzz target: random posting sets must never be accepted unless balanced.
//!
//! Run (nightly + cargo-fuzz):
//! ```bash
//! cargo install cargo-fuzz
//! cargo +nightly fuzz run journal_balance
//! ```

use libfuzzer_sys::fuzz_target;
use ironledger_domain::{
    AccountId, AssetCode, AtomicAmount, JournalEntry, Posting, CausationId, CorrelationId,
    IdempotencyKey, TransactionId,
};

fuzz_target!(|data: &[u8]| {
    if data.len() < 4 {
        return;
    }
    let a = AccountId::new();
    let b = AccountId::new();
    let left = i64::from(data[0]) as i128 + 1;
    let right = i64::from(data[1]) as i128 + 1;
    let asset = if data[2] % 2 == 0 {
        AssetCode::btc()
    } else {
        AssetCode::usd()
    };
    let postings = vec![
        Posting::debit(a, asset.clone(), AtomicAmount::from_raw(left)).ok(),
        Posting::credit(b, asset, AtomicAmount::from_raw(right)).ok(),
    ];
    let Some(postings) = postings.into_iter().collect::<Option<Vec<_>>>() else {
        return;
    };
    let entry = JournalEntry::new(
        TransactionId::new(),
        IdempotencyKey::new("fuzz").unwrap(),
        "fuzz",
        postings,
        CorrelationId::new(),
        CausationId::new(),
    );
    if left == right {
        assert!(entry.is_ok());
    } else {
        assert!(entry.is_err());
    }
});
