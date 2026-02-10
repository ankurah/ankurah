mod common;
use ankurah::{policy::DEFAULT_CONTEXT, Model, Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use common::TestDag;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A model with YrsString for testing text CRDT behavior
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Document {
    #[active_type(YrsString)]
    pub content: String,
}

async fn setup() -> Result<ankurah::Context> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    node.system.create().await?;
    Ok(node.context_async(DEFAULT_CONTEXT).await)
}

/// Test 2.1: Concurrent Text Inserts - Same Position
/// Both insertions should be present in the final text
#[tokio::test]
async fn test_concurrent_inserts_same_position() -> Result<()> {
    let ctx = setup().await?;
    let mut dag = TestDag::new();

    // Create document with initial content
    let doc_id = {
        let trx = ctx.begin();
        let doc = trx.create(&Document { content: "hello".to_owned() }).await?;
        let id = doc.id();
        dag.enumerate(trx.commit_and_return_events().await?); // A
        id
    };

    let doc = ctx.get::<DocumentView>(doc_id).await?;

    // Two concurrent transactions inserting at the same position
    let trx1 = ctx.begin();
    let trx2 = ctx.begin();

    doc.edit(&trx1)?.content().insert(5, " world")?; // Insert at end
    doc.edit(&trx2)?.content().insert(5, " there")?; // Also insert at end (position 5)

    dag.enumerate(trx1.commit_and_return_events().await?); // B
    dag.enumerate(trx2.commit_and_return_events().await?); // C

    // Both insertions should be present
    let final_doc = ctx.get::<DocumentView>(doc_id).await?;
    let final_content = final_doc.content().unwrap();

    // Yrs will deterministically order the concurrent inserts
    // Both " world" and " there" should be present
    assert!(
        final_content.contains("world") && final_content.contains("there"),
        "Both insertions should be present, got: {}",
        final_content
    );

    // Order is deterministic (though we don't know which comes first without knowing Yrs internals)
    let possible1 = "hello world there";
    let possible2 = "hello there world";
    assert!(
        final_content == possible1 || final_content == possible2,
        "Expected '{}' or '{}', got '{}'",
        possible1,
        possible2,
        final_content
    );

    Ok(())
}

/// Test 2.2: Concurrent Text Inserts - Different Positions
/// Non-conflicting positions should merge cleanly
#[tokio::test]
async fn test_concurrent_inserts_different_positions() -> Result<()> {
    let ctx = setup().await?;
    let mut dag = TestDag::new();

    // Create document
    let doc_id = {
        let trx = ctx.begin();
        let doc = trx.create(&Document { content: "hello world".to_owned() }).await?;
        let id = doc.id();
        dag.enumerate(trx.commit_and_return_events().await?);
        id
    };

    let doc = ctx.get::<DocumentView>(doc_id).await?;

    // Concurrent inserts at different positions
    let trx1 = ctx.begin();
    let trx2 = ctx.begin();

    doc.edit(&trx1)?.content().insert(0, "X")?; // Insert at start
    doc.edit(&trx2)?.content().insert(11, "Y")?; // Insert at end

    dag.enumerate(trx1.commit_and_return_events().await?);
    dag.enumerate(trx2.commit_and_return_events().await?);

    let final_doc = ctx.get::<DocumentView>(doc_id).await?;
    let final_content = final_doc.content().unwrap();

    // Since positions don't conflict, result should be deterministic
    assert_eq!(final_content, "Xhello worldY", "Got: {}", final_content);

    Ok(())
}

/// Test 2.3: Concurrent Text Deletes
/// Both deletions should be applied
#[tokio::test]
async fn test_concurrent_deletes() -> Result<()> {
    let ctx = setup().await?;
    let mut dag = TestDag::new();

    // Create document
    let doc_id = {
        let trx = ctx.begin();
        let doc = trx.create(&Document { content: "hello world".to_owned() }).await?;
        let id = doc.id();
        dag.enumerate(trx.commit_and_return_events().await?);
        id
    };

    let doc = ctx.get::<DocumentView>(doc_id).await?;

    // Concurrent deletes of different parts
    let trx1 = ctx.begin();
    let trx2 = ctx.begin();

    doc.edit(&trx1)?.content().delete(0, 6)?; // Delete "hello "
    doc.edit(&trx2)?.content().delete(6, 5)?; // Delete "world"

    dag.enumerate(trx1.commit_and_return_events().await?);
    dag.enumerate(trx2.commit_and_return_events().await?);

    let final_doc = ctx.get::<DocumentView>(doc_id).await?;
    let final_content = final_doc.content().unwrap();

    // Both deletions applied - only the space might remain (depends on how ranges overlap)
    // "hello world" - delete 0..6 ("hello ") - delete 6..11 ("world") = ""
    assert!(final_content.is_empty() || final_content == " ", "Expected empty or ' ', got: '{}'", final_content);

    Ok(())
}

/// Test 2.4: Concurrent Insert and Delete at Same Position
#[tokio::test]
async fn test_concurrent_insert_and_delete() -> Result<()> {
    let ctx = setup().await?;
    let mut dag = TestDag::new();

    // Create document
    let doc_id = {
        let trx = ctx.begin();
        let doc = trx.create(&Document { content: "hello world".to_owned() }).await?;
        let id = doc.id();
        dag.enumerate(trx.commit_and_return_events().await?);
        id
    };

    let doc = ctx.get::<DocumentView>(doc_id).await?;

    // One inserts, one deletes
    let trx1 = ctx.begin();
    let trx2 = ctx.begin();

    doc.edit(&trx1)?.content().insert(5, "X")?; // Insert X after "hello"
    doc.edit(&trx2)?.content().delete(5, 6)?; // Delete " world"

    dag.enumerate(trx1.commit_and_return_events().await?);
    dag.enumerate(trx2.commit_and_return_events().await?);

    let final_doc = ctx.get::<DocumentView>(doc_id).await?;
    let final_content = final_doc.content().unwrap();

    // X should be inserted, " world" should be deleted
    assert_eq!(final_content, "helloX", "Got: {}", final_content);

    Ok(())
}

/// Test 2.5: Text Replace (Delete + Insert)
#[tokio::test]
async fn test_text_replace() -> Result<()> {
    let ctx = setup().await?;
    let mut dag = TestDag::new();

    // Create document
    let doc_id = {
        let trx = ctx.begin();
        let doc = trx.create(&Document { content: "hello".to_owned() }).await?;
        let id = doc.id();
        dag.enumerate(trx.commit_and_return_events().await?);
        id
    };

    let doc = ctx.get::<DocumentView>(doc_id).await?;

    // Replace content
    let trx = ctx.begin();
    doc.edit(&trx)?.content().replace("goodbye")?;
    dag.enumerate(trx.commit_and_return_events().await?);

    let final_doc = ctx.get::<DocumentView>(doc_id).await?;
    assert_eq!(final_doc.content().unwrap(), "goodbye");

    Ok(())
}

/// Test 2.6: Yrs Convergence - Multiple Concurrent Operations
/// Verify that applying the same operations in any order yields the same result
#[tokio::test]
async fn test_yrs_convergence() -> Result<()> {
    let ctx = setup().await?;
    let mut dag = TestDag::new();

    // Create document
    let doc_id = {
        let trx = ctx.begin();
        let doc = trx.create(&Document { content: "abc".to_owned() }).await?;
        let id = doc.id();
        dag.enumerate(trx.commit_and_return_events().await?);
        id
    };

    let doc = ctx.get::<DocumentView>(doc_id).await?;

    // Three concurrent modifications
    let trx1 = ctx.begin();
    let trx2 = ctx.begin();
    let trx3 = ctx.begin();

    doc.edit(&trx1)?.content().insert(0, "1")?; // Insert at start
    doc.edit(&trx2)?.content().insert(1, "2")?; // Insert after 'a'
    doc.edit(&trx3)?.content().insert(3, "3")?; // Insert after 'c'

    dag.enumerate(trx1.commit_and_return_events().await?);
    dag.enumerate(trx2.commit_and_return_events().await?);
    dag.enumerate(trx3.commit_and_return_events().await?);

    let final_doc = ctx.get::<DocumentView>(doc_id).await?;
    let final_content = final_doc.content().unwrap();

    // All three insertions should be present
    assert!(final_content.contains('1'), "Should contain 1");
    assert!(final_content.contains('2'), "Should contain 2");
    assert!(final_content.contains('3'), "Should contain 3");
    assert!(final_content.contains('a'), "Should contain a");
    assert!(final_content.contains('b'), "Should contain b");
    assert!(final_content.contains('c'), "Should contain c");

    // Result is deterministic (same every time)
    let len = final_content.len();
    assert_eq!(len, 6, "Should have 6 characters: got '{}'", final_content);

    Ok(())
}

/// Test: Sequential text operations maintain order
// TODO(#175): Blocked on PR #236 — Yrs does not differentiate between empty string
// and null value. Creating an entity with content: "" produces no CRDT operations,
// so no creation event is generated and the entity is never persisted to storage.
#[tokio::test]
#[ignore = "blocked on #236: Yrs empty-string treated as null"]
async fn test_sequential_text_operations() -> Result<()> {
    let ctx = setup().await?;
    let mut dag = TestDag::new();

    // Create document
    let doc_id = {
        let trx = ctx.begin();
        let doc = trx.create(&Document { content: "".to_owned() }).await?;
        let id = doc.id();
        dag.enumerate(trx.commit_and_return_events().await?);
        id
    };

    // Sequential inserts
    for word in ["Hello", " ", "World", "!"] {
        let doc = ctx.get::<DocumentView>(doc_id).await?;
        let trx = ctx.begin();
        let content = doc.edit(&trx)?.content();
        let len = content.value().map(|s| s.len()).unwrap_or(0);
        content.insert(len as u32, word)?;
        dag.enumerate(trx.commit_and_return_events().await?);
    }

    let final_doc = ctx.get::<DocumentView>(doc_id).await?;
    assert_eq!(final_doc.content().unwrap(), "Hello World!");

    Ok(())
}
