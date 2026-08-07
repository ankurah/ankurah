//! What a read scope does when the caller's own filter names the same column.
//!
//! A read scope is composed onto the caller's predicate as another conjunct, so
//! `{ "filter": "owner = $jwt.sub" }` and a caller asking for `owner = <someone
//! else>` produce one predicate with two terms on `owner`. Nothing about that
//! is special to policy — it is the ordinary case of a column carrying more
//! terms than an index key has positions for it, pinned in the storage planner
//! by `repeated_column_tests` in `storage/common/src/planner.rs` and over sled
//! rows by `tests/tests/sled/repeated_column.rs`.
//!
//! It gets its own coverage here because this is the composition consumers
//! actually run, and because it is the case where a term the planner forgets is
//! visible as a row: an index scan that answers a two-term query using only the
//! first term returns rows the second term excludes. The checks run against a
//! sled-backed durable node over both paths a reader takes — a one-shot `fetch`
//! and a LiveQuery.

mod common;

use ankurah::{Model, Node, Ref};
use ankurah_jwt_auth::{JwtAgent, JwtContext, JwtKeys, PolicyConfig, SigningKeys};
use ankurah_storage_sled::SledStorageEngine;
use std::sync::Arc;

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct OverlapUser {
    pub name: String,
}

/// A scoped column that is a `Ref`, which collates as raw EntityId bytes.
#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct OverlapDoc {
    pub owner: Ref<OverlapUser>,
    pub label: String,
}

/// The same scope over a String column, which collates as text and admits range
/// comparisons a `Ref` column does not usefully take.
#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct OverlapNote {
    pub owner: String,
    pub label: String,
}

/// Two scoped columns under one disjunctive rule — the shape a direct-message
/// thread takes, where either participant may read.
#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct OverlapThread {
    pub first: Ref<OverlapUser>,
    pub second: Ref<OverlapUser>,
    pub label: String,
}

const CONFIG_JSON: &str = r#"{
    "roles": { "Member": ["doc:read", "user:read"] },
    "collections": {
        "overlapdoc":    { "read": "doc:read", "scope": [{ "filter": "owner = $jwt.sub" }] },
        "overlapnote":   { "read": "doc:read", "scope": [{ "filter": "owner = $jwt.sub" }] },
        "overlapthread": { "read": "doc:read", "scope": [{ "filter": "first = $jwt.sub OR second = $jwt.sub" }] },
        "overlapuser":   { "read": "user:read" }
    }
}"#;

fn member_context(keys: &SigningKeys, subject: &str) -> JwtContext {
    let claims = common::make_claims(subject, &["Member"], "member@example.com");
    let token = common::sign_token(keys, &claims);
    JwtContext::from_claims(claims, token)
}

/// Alice, Bob and Carol; one document, note and thread per pairing, on a node
/// whose policy scopes each collection to the reader.
struct Fixture {
    node: Node<SledStorageEngine, JwtAgent>,
    keys: SigningKeys,
    alice: ankurah::EntityId,
    bob: ankurah::EntityId,
    carol: ankurah::EntityId,
}

impl Fixture {
    async fn build() -> anyhow::Result<Self> {
        let keys = common::test_keys();
        let agent = JwtAgent::new_ephemeral();
        agent.update_config(serde_json::from_str::<PolicyConfig>(CONFIG_JSON)?);
        agent.set_keys(JwtKeys::Signing(keys.clone()));

        let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent);
        node.system.create().await?;

        let root = node.context(JwtContext::system())?;
        let (alice, bob, carol) = {
            let trx = root.begin();
            let alice = trx.create(&OverlapUser { name: "Alice".into() }).await?;
            let bob = trx.create(&OverlapUser { name: "Bob".into() }).await?;
            let carol = trx.create(&OverlapUser { name: "Carol".into() }).await?;
            let ids = (alice.id(), bob.id(), carol.id());
            trx.commit().await?;
            ids
        };
        {
            let trx = root.begin();
            trx.create(&OverlapDoc { owner: alice.into(), label: "shared label".into() }).await?;
            trx.create(&OverlapDoc { owner: bob.into(), label: "shared label".into() }).await?;
            trx.create(&OverlapNote { owner: alice.to_base64(), label: "shared label".into() }).await?;
            trx.create(&OverlapNote { owner: bob.to_base64(), label: "shared label".into() }).await?;
            trx.create(&OverlapThread { first: alice.into(), second: carol.into(), label: "alice-carol".into() }).await?;
            trx.create(&OverlapThread { first: bob.into(), second: carol.into(), label: "bob-carol".into() }).await?;
            trx.commit().await?;
        }

        Ok(Self { node, keys, alice, bob, carol })
    }

    fn as_alice(&self) -> anyhow::Result<ankurah::Context> { Ok(self.node.context(member_context(&self.keys, &self.alice.to_base64()))?) }
}

/// The caller filters on the scoped column and names the other member. Both
/// terms are on `owner`, and the answer is the rows that satisfy both — none.
/// The control in the same test is the caller naming its own id on that column,
/// which does return its row, so the empty answer is the scope's term applying
/// rather than the query matching nothing.
#[tokio::test]
async fn caller_equality_on_the_scoped_column_does_not_replace_the_scope() -> anyhow::Result<()> {
    let f = Fixture::build().await?;
    let alice = f.as_alice()?;

    // Ref-typed scoped column.
    let own = format!("owner = '{}'", f.alice.to_base64());
    let rows = alice.fetch::<OverlapDocView>(own.as_str()).await?;
    assert_eq!(rows.len(), 1, "the caller's own row must still be reachable");
    assert_eq!(rows[0].owner()?.id(), f.alice, "and it must be the caller's own row");

    let other = format!("owner = '{}'", f.bob.to_base64());
    let rows = alice.fetch::<OverlapDocView>(other.as_str()).await?;
    assert!(rows.is_empty(), "the scope's term on `owner` must still apply, but the fetch returned {} row(s)", rows.len());

    // String-typed scoped column, same shape.
    let own = format!("owner = '{}'", f.alice.to_base64());
    assert_eq!(alice.fetch::<OverlapNoteView>(own.as_str()).await?.len(), 1);
    let other = format!("owner = '{}'", f.bob.to_base64());
    let rows = alice.fetch::<OverlapNoteView>(other.as_str()).await?;
    assert!(rows.is_empty(), "a String scoped column must hold its term too, but the fetch returned {} row(s)", rows.len());

    Ok(())
}

/// The caller's term on the scoped column is a range rather than an equality.
/// The scope's equality is the one the key range can hold, so the caller's
/// range is the leftover term — the same loss with the halves swapped, and the
/// caller gets rows outside the range it asked for rather than rows outside its
/// scope.
#[tokio::test]
async fn caller_range_on_the_scoped_column_still_applies() -> anyhow::Result<()> {
    let f = Fixture::build().await?;
    let alice = f.as_alice()?;
    let alice_b64 = f.alice.to_base64();

    // A range that excludes the caller's own id leaves nothing, whichever side
    // of the caller's id the bound falls on.
    let above = format!("owner > '{alice_b64}'");
    let rows = alice.fetch::<OverlapNoteView>(above.as_str()).await?;
    assert!(rows.is_empty(), "the caller's own row is not above its own id, yet the fetch returned {} row(s)", rows.len());

    let below = format!("owner < '{alice_b64}'");
    let rows = alice.fetch::<OverlapNoteView>(below.as_str()).await?;
    assert!(rows.is_empty(), "nor below it, yet the fetch returned {} row(s)", rows.len());

    // A range that includes it returns exactly it, and never the other
    // member's row, whichever way the two ids happen to sort.
    let inclusive = format!("owner >= '{alice_b64}' AND owner <= '{alice_b64}'");
    let rows = alice.fetch::<OverlapNoteView>(inclusive.as_str()).await?;
    assert_eq!(rows.len(), 1, "a range that brackets the caller's own id must return that row");
    assert_eq!(rows[0].owner()?, alice_b64);

    Ok(())
}

/// A disjunctive scope rule over two columns, with the caller naming one of
/// them. A disjunction never becomes a key range, so it stays in the residual
/// predicate and is re-checked per row. This shape was already sound; the test
/// is here so that a later change to which terms the planner encodes cannot
/// quietly take the disjunction with it.
#[tokio::test]
async fn disjunctive_scope_holds_when_the_caller_names_a_scoped_column() -> anyhow::Result<()> {
    let f = Fixture::build().await?;
    let alice = f.as_alice()?;

    let own = format!("first = '{}'", f.alice.to_base64());
    assert_eq!(alice.fetch::<OverlapThreadView>(own.as_str()).await?.len(), 1, "the caller's own thread must still be reachable");

    let other = format!("first = '{}'", f.bob.to_base64());
    let rows = alice.fetch::<OverlapThreadView>(other.as_str()).await?;
    assert!(rows.is_empty(), "neither arm of the scope admits Bob's thread, yet the fetch returned {} row(s)", rows.len());

    // The caller's own filter is itself a disjunction naming both scoped
    // columns, so neither side of the composed predicate is a plain equality.
    let both = format!("first = '{}' OR second = '{}'", f.bob.to_base64(), f.carol.to_base64());
    let rows = alice.fetch::<OverlapThreadView>(both.as_str()).await?;
    assert_eq!(rows.len(), 1, "Alice's own thread has Carol as its second participant, so her scope admits exactly it");
    assert_eq!(rows[0].label()?, "alice-carol");

    Ok(())
}

/// The same two terms on the scoped column, read through a LiveQuery rather
/// than a one-shot fetch. A LiveQuery's initial results come from the same
/// storage query, and its later results come from the reactor re-checking each
/// commit; both have to hold.
#[tokio::test]
async fn caller_equality_on_the_scoped_column_holds_for_a_live_query() -> anyhow::Result<()> {
    let f = Fixture::build().await?;
    let alice = f.as_alice()?;

    let own = format!("owner = '{}'", f.alice.to_base64());
    let own_lq = alice.query::<OverlapDocView>(own.as_str())?;
    own_lq.wait_initialized().await;
    assert_eq!(own_lq.ids().len(), 1, "the caller's own row must still reach its LiveQuery");

    let other = format!("owner = '{}'", f.bob.to_base64());
    let other_lq = alice.query::<OverlapDocView>(other.as_str())?;
    other_lq.wait_initialized().await;
    assert_eq!(other_lq.ids().len(), 0, "the scope's term must apply to the LiveQuery's initial results");

    // A row committed after the subscription reaches the LiveQuery through the
    // reactor rather than the planner, so check that arrival path too.
    {
        let root = f.node.context(JwtContext::system())?;
        let trx = root.begin();
        trx.create(&OverlapDoc { owner: f.bob.into(), label: "committed later".into() }).await?;
        trx.commit().await?;
    }
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
    assert_eq!(other_lq.ids().len(), 0, "a row committed later must not reach the LiveQuery either");

    Ok(())
}

/// The caller filters on a different column than the scope. Nothing here is at
/// risk from a repeated column, and that is the point: it is the control that
/// says the fixture and the scope both work, so the empty answers above are the
/// second term applying.
#[tokio::test]
async fn caller_filter_on_another_column_leaves_the_scope_intact() -> anyhow::Result<()> {
    let f = Fixture::build().await?;
    let alice = f.as_alice()?;

    let rows = alice.fetch::<OverlapDocView>("label = 'shared label'").await?;
    assert_eq!(rows.len(), 1, "both members' rows carry that label; the scope keeps one");
    assert_eq!(rows[0].owner()?.id(), f.alice);

    let _ = f.carol; // named in the fixture for the thread pairings

    Ok(())
}
