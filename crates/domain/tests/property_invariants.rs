//! Property-based tests for conservation and balance invariants.

use ironledger_domain::amount::AtomicAmount;
use ironledger_domain::asset::AssetCode;
use ironledger_domain::ids::{
    AccountId, CausationId, CorrelationId, IdempotencyKey, TransactionId,
};
use ironledger_domain::journal::{validate_balanced, JournalEntry, Posting};
use proptest::prelude::*;

fn arb_amount() -> impl Strategy<Value = i128> {
    1i128..=1_000_000i128
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Every accepted two-leg transfer balances and conserves the asset total at zero residual.
    #[test]
    fn accepted_transfers_balance(
        amount in arb_amount(),
        seed in any::<u8>(),
    ) {
        let a = AccountId::new();
        let b = AccountId::new();
        let asset = if seed % 2 == 0 {
            AssetCode::btc()
        } else {
            AssetCode::usd()
        };
        let amt = AtomicAmount::from_raw(amount);
        let postings = vec![
            Posting::debit(a, asset.clone(), amt).unwrap(),
            Posting::credit(b, asset, amt).unwrap(),
        ];
        prop_assert!(validate_balanced(&postings).is_ok());
        let totals = JournalEntry::asset_totals(&postings).unwrap();
        for (_, total) in totals {
            prop_assert!(total.is_zero());
        }
    }

    /// Unbalanced residuals are always rejected.
    #[test]
    fn unbalanced_always_rejected(
        amount in arb_amount(),
        delta in 1i128..=100i128,
    ) {
        let a = AccountId::new();
        let b = AccountId::new();
        let postings = vec![
            Posting::debit(a, AssetCode::btc(), AtomicAmount::from_raw(amount)).unwrap(),
            Posting::credit(
                b,
                AssetCode::btc(),
                AtomicAmount::from_raw(amount.saturating_sub(delta).max(1)),
            )
            .unwrap(),
        ];
        // Only assert when truly unbalanced
        let totals = JournalEntry::asset_totals(&postings).unwrap();
        let residual = totals.values().next().unwrap().raw();
        if residual != 0 {
            prop_assert!(validate_balanced(&postings).is_err());
        }
    }

    /// JournalEntry::new rejects unbalanced input (no partial accept).
    #[test]
    fn constructor_rejects_unbalanced(amount in arb_amount()) {
        let a = AccountId::new();
        let b = AccountId::new();
        let postings = vec![
            Posting::debit(a, AssetCode::usd(), AtomicAmount::from_raw(amount)).unwrap(),
            Posting::credit(b, AssetCode::usd(), AtomicAmount::from_raw(amount + 1)).unwrap(),
        ];
        let result = JournalEntry::new(
            TransactionId::new(),
            IdempotencyKey::new("prop-key").unwrap(),
            "prop",
            postings,
            CorrelationId::new(),
            CausationId::new(),
        );
        prop_assert!(result.is_err());
    }
}
