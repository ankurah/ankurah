//! What a query gets back when one column carries more than one term.
//!
//! An index key holds one position per column, so a query naming the same
//! column twice gives the planner more terms than the key range can carry.
//! Choosing an index is an optimization; the answer is only correct because
//! every term is re-checked against the rows the scan returns — index bounds
//! are a prefilter over keys built by casting stored values to literal-derived
//! types, not an enforcement of any term. These run that over a real sled
//! node, where a term the planner forgot shows up as a row the query excluded.
//!
//! The planner-level counterpart lives in `storage/common/src/planner.rs`
//! (`repeated_column_tests`), which pins the plan; these pin the rows.

use ankurah::property::Ref;
use ankurah::{policy::DEFAULT_CONTEXT, Model, Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// Unique names to avoid collision with the other models in this test binary.
#[derive(Model, Debug, Serialize, Deserialize, Clone)]
pub struct DupUser {
    pub name: String,
}

#[derive(Model, Debug, Serialize, Deserialize, Clone)]
pub struct DupNote {
    #[active_type(LWW)]
    pub title: String,
    pub rank: i32,
    pub score: f64,
    pub owner: Ref<DupUser>,
    pub reviewer: Ref<DupUser>,
}

async fn setup_context() -> Result<ankurah::Context> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    node.system.create().await?;
    Ok(node.context_async(DEFAULT_CONTEXT).await)
}

fn titles(notes: &[DupNoteView]) -> Vec<String> {
    let mut titles: Vec<String> = notes.iter().map(|n| n.title().unwrap()).collect();
    titles.sort();
    titles
}

/// Two owners, and one note apiece, so that a term the scan drops is visible as
/// the other owner's note coming back.
async fn two_owners_two_notes(ctx: &ankurah::Context) -> Result<(ankurah::EntityId, ankurah::EntityId)> {
    let (alice, bob) = {
        let trx = ctx.begin();
        let alice = trx.create(&DupUser { name: "Alice".into() }).await?;
        let bob = trx.create(&DupUser { name: "Bob".into() }).await?;
        let ids = (alice.id(), bob.id());
        trx.commit().await?;
        ids
    };
    {
        let trx = ctx.begin();
        trx.create(&DupNote { title: "alice note".into(), rank: 1, score: 5.9, owner: Ref::new(alice), reviewer: Ref::new(bob) }).await?;
        trx.create(&DupNote { title: "bob note".into(), rank: 2, score: 2.0, owner: Ref::new(bob), reviewer: Ref::new(alice) }).await?;
        trx.commit().await?;
    }
    Ok((alice, bob))
}

/// An integer literal against an f64 column. The planner types the index
/// keypart from the literal (there is no schema to consult), the key encoding
/// casts each stored value to that type, and F64 -> I32 truncates — so 5.9
/// keys as 5 and sits inside the point range for `= 5`. The range therefore
/// enforces "truncates to 5", not "= 5", and only the retained residual term,
/// evaluated against the stored f64, keeps the row out. Repeating the term
/// must not change that: an identical duplicate is exactly the shape a policy
/// scope composes when the caller already asked for its own rows.
#[tokio::test]
async fn f64_column_with_integer_literal_matches_exactly() -> Result<()> {
    let ctx = setup_context().await?;
    two_owners_two_notes(&ctx).await?;

    assert_eq!(titles(&ctx.fetch::<DupNoteView>("score = 5").await?), Vec::<String>::new(), "5.9 is not 5, whatever its index key says");
    assert_eq!(
        titles(&ctx.fetch::<DupNoteView>("score = 5 AND score = 5").await?),
        Vec::<String>::new(),
        "repeating the term must not widen it"
    );
    assert_eq!(titles(&ctx.fetch::<DupNoteView>("score = 5 AND score = 6").await?), Vec::<String>::new());

    // Controls: an integer literal that genuinely equals a stored float
    // (2 vs 2.0) keeps its row, as does a same-typed duplicate on the i32
    // column — the residual re-check filters truncation artifacts without
    // over-filtering honest matches. (An exact f64 literal is not a control
    // available here: the query grammar has no decimal literals.)
    assert_eq!(titles(&ctx.fetch::<DupNoteView>("score = 2").await?), vec!["bob note"]);
    assert_eq!(titles(&ctx.fetch::<DupNoteView>("score = 2 AND score = 2").await?), vec!["bob note"]);
    assert_eq!(titles(&ctx.fetch::<DupNoteView>("rank = 1 AND rank = 1").await?), vec!["alice note"]);

    Ok(())
}

/// Two equalities on one String column contradict, so the query names no row.
#[tokio::test]
async fn two_equalities_on_one_string_column_match_nothing() -> Result<()> {
    let ctx = setup_context().await?;
    two_owners_two_notes(&ctx).await?;

    let rows = ctx.fetch::<DupNoteView>("title = 'alice note' AND title = 'bob note'").await?;
    assert_eq!(titles(&rows), Vec::<String>::new(), "no note carries both titles");

    // The control: one of those terms on its own does name a row, so the empty
    // answer above is the second term doing its job rather than a broken query.
    let rows = ctx.fetch::<DupNoteView>("title = 'alice note'").await?;
    assert_eq!(titles(&rows), vec!["alice note"]);

    Ok(())
}

/// The same term written twice still names its row. The planner is free to
/// notice that the repeat constrains nothing further, but it must not turn the
/// query into one that matches less.
#[tokio::test]
async fn repeated_identical_equality_still_matches() -> Result<()> {
    let ctx = setup_context().await?;
    two_owners_two_notes(&ctx).await?;

    let rows = ctx.fetch::<DupNoteView>("title = 'alice note' AND title = 'alice note'").await?;
    assert_eq!(titles(&rows), vec!["alice note"]);

    Ok(())
}

/// An equality and a range on one column, contradicting: the range endpoint is
/// the equality's point bound, so the inequality is the term that has to be
/// re-checked.
#[tokio::test]
async fn equality_and_range_on_one_column_match_nothing() -> Result<()> {
    let ctx = setup_context().await?;
    two_owners_two_notes(&ctx).await?;

    assert_eq!(titles(&ctx.fetch::<DupNoteView>("rank = 1 AND rank > 5").await?), Vec::<String>::new());
    assert_eq!(titles(&ctx.fetch::<DupNoteView>("rank > 5 AND rank = 1").await?), Vec::<String>::new());
    assert_eq!(titles(&ctx.fetch::<DupNoteView>("rank = 1 AND rank < 0").await?), Vec::<String>::new());

    // Consistent versions of the same shape keep their row.
    assert_eq!(titles(&ctx.fetch::<DupNoteView>("rank = 1 AND rank > 0").await?), vec!["alice note"]);
    assert_eq!(titles(&ctx.fetch::<DupNoteView>("rank = 2 AND rank <= 2").await?), vec!["bob note"]);

    Ok(())
}

/// Two ranges on one column fold into that column's low and high endpoints.
/// This shape was already sound; it is here so that folding cannot quietly stop
/// encoding one side without a test noticing.
#[tokio::test]
async fn two_ranges_on_one_column_bracket_correctly() -> Result<()> {
    let ctx = setup_context().await?;
    two_owners_two_notes(&ctx).await?;

    assert_eq!(titles(&ctx.fetch::<DupNoteView>("rank > 0 AND rank < 2").await?), vec!["alice note"]);
    assert_eq!(titles(&ctx.fetch::<DupNoteView>("rank >= 1 AND rank <= 2").await?), vec!["alice note", "bob note"]);
    assert_eq!(titles(&ctx.fetch::<DupNoteView>("rank > 5 AND rank < 1").await?), Vec::<String>::new());

    Ok(())
}

/// The repeated column sits between terms on other columns, so the key still
/// has room for it — the leftover term is not at the end of the key and cannot
/// be dropped by truncating it.
#[tokio::test]
async fn repeated_column_alongside_other_columns() -> Result<()> {
    let ctx = setup_context().await?;
    let (alice, bob) = two_owners_two_notes(&ctx).await?;

    let query = format!("owner = '{}' AND rank = 1 AND owner = '{}'", alice.to_base64(), bob.to_base64());
    assert_eq!(titles(&ctx.fetch::<DupNoteView>(query.as_str()).await?), Vec::<String>::new(), "no note has two owners");

    let query = format!("owner = '{}' AND rank = 1 AND owner = '{}'", alice.to_base64(), alice.to_base64());
    assert_eq!(titles(&ctx.fetch::<DupNoteView>(query.as_str()).await?), vec!["alice note"]);

    Ok(())
}

/// The same shape over `Ref` columns, which collate as raw EntityId bytes
/// rather than text. This is the column type a row-local read scope names when
/// the scoped field is a `Ref<User>`.
#[tokio::test]
async fn two_equalities_on_one_ref_column_match_nothing() -> Result<()> {
    let ctx = setup_context().await?;
    let (alice, bob) = two_owners_two_notes(&ctx).await?;

    let query = format!("owner = '{}' AND owner = '{}'", bob.to_base64(), alice.to_base64());
    assert_eq!(titles(&ctx.fetch::<DupNoteView>(query.as_str()).await?), Vec::<String>::new(), "no note is owned by two users");

    // Each half on its own names exactly one note, in both orders, so neither
    // id is simply unmatchable.
    let query = format!("owner = '{}'", bob.to_base64());
    assert_eq!(titles(&ctx.fetch::<DupNoteView>(query.as_str()).await?), vec!["bob note"]);
    let query = format!("owner = '{}'", alice.to_base64());
    assert_eq!(titles(&ctx.fetch::<DupNoteView>(query.as_str()).await?), vec!["alice note"]);

    Ok(())
}

/// A disjunction over two columns, ANDed with a term on one of them — the shape
/// a two-armed read scope (`owner = me OR reviewer = me`) takes once a caller's
/// own filter on `owner` is composed onto it. A disjunction is never encoded
/// into a key range, so both halves have to be re-checked, and the answer is the
/// rows that satisfy both.
#[tokio::test]
async fn or_over_two_columns_and_a_repeated_column() -> Result<()> {
    let ctx = setup_context().await?;
    let (alice, bob) = two_owners_two_notes(&ctx).await?;

    // A third note Alice has nothing to do with, so that the disjunction has a
    // row to exclude on its own.
    {
        let trx = ctx.begin();
        trx.create(&DupNote { title: "bob solo".into(), rank: 3, score: 1.0, owner: Ref::new(bob), reviewer: Ref::new(bob) }).await?;
        trx.commit().await?;
    }

    let alice_touches = format!("(owner = '{}' OR reviewer = '{}')", alice.to_base64(), alice.to_base64());

    // Alice reviews bob's note, so the disjunction admits it and the term on
    // `owner` keeps it; "bob solo" fails the disjunction and "alice note"
    // fails the term on `owner`.
    let query = format!("{alice_touches} AND owner = '{}'", bob.to_base64());
    assert_eq!(titles(&ctx.fetch::<DupNoteView>(query.as_str()).await?), vec!["bob note"]);

    // The same disjunction with the other principal on `owner` keeps only the
    // note Alice owns.
    let query = format!("{alice_touches} AND owner = '{}'", alice.to_base64());
    assert_eq!(titles(&ctx.fetch::<DupNoteView>(query.as_str()).await?), vec!["alice note"]);

    // And the term on `owner` repeated with two different principals leaves
    // nothing, disjunction or no disjunction.
    let query = format!("{alice_touches} AND owner = '{}' AND owner = '{}'", alice.to_base64(), bob.to_base64());
    assert_eq!(titles(&ctx.fetch::<DupNoteView>(query.as_str()).await?), Vec::<String>::new(), "no note has two owners");

    Ok(())
}

/// ORDER BY naming a column the predicate already pins. The sort column and the
/// repeated term compete for the same key position, and the answer must still
/// be the rows both terms admit.
#[tokio::test]
async fn repeated_column_under_order_by() -> Result<()> {
    let ctx = setup_context().await?;
    let (alice, bob) = two_owners_two_notes(&ctx).await?;

    let query = format!("owner = '{}' AND owner = '{}' ORDER BY owner, rank", alice.to_base64(), bob.to_base64());
    assert_eq!(titles(&ctx.fetch::<DupNoteView>(query.as_str()).await?), Vec::<String>::new());

    let query = format!("owner = '{}' AND owner = '{}' ORDER BY owner, rank", alice.to_base64(), alice.to_base64());
    assert_eq!(titles(&ctx.fetch::<DupNoteView>(query.as_str()).await?), vec!["alice note"]);

    Ok(())
}
