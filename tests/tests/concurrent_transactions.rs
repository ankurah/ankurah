mod common;
use anyhow::Result;
use common::*;
use std::sync::Arc;

/// Test two concurrent transactions modifying the same entity
/// This reproduces the scenario where:
/// 1. Two transactions fork the same entity at the same head
/// 2. Both create events with the same parent
/// 3. First transaction commits successfully
/// 4. Second transaction should handle the concurrent update correctly
#[tokio::test]
async fn test_concurrent_transactions_same_entity() -> Result<()> {
    let context = durable_sled_setup().await?.context_async(DEFAULT_CONTEXT).await;

    // Create initial entity
    let album_id = {
        let trx = context.begin();
        let album = trx.create(&Album { name: "Initial Name".to_owned(), year: "2024".to_owned() }).await?;
        let id = album.id();
        trx.commit().await?;
        id
    };

    // Get the entity so both transactions will fork from the same head
    let album = context.get::<AlbumView>(album_id).await?;

    // Start two concurrent transactions
    let trx1 = context.begin();
    let trx2 = context.begin();

    // Both transactions edit the same entity (both fork from same head)
    let album_mut1 = album.edit(&trx1)?;
    let album_mut2 = album.edit(&trx2)?;

    // Make different changes
    album_mut1.name().replace("Updated by Trx1")?;
    album_mut2.year().replace("2025")?;

    // Commit first transaction - this should succeed
    trx1.commit().await?;

    // Commit second transaction - this should handle the concurrent update
    // The second transaction's event has parent that equals the head before trx1 committed,
    // but now the head has been updated by trx1. This should be detected as NotDescends
    // and handled appropriately.
    let result = trx2.commit().await;

    // For now, we expect this to either:
    // 1. Succeed (if concurrent updates are handled correctly)
    // 2. Return a clear error (not BudgetExceeded with wrong frontiers)
    match result {
        Ok(_) => {
            // If it succeeds, verify both changes were applied
            let final_album = context.get::<AlbumView>(album_id).await?;
            println!("Final album name: {:?}", final_album.name());
            println!("Final album year: {:?}", final_album.year());

            // With proper concurrent update handling, both changes should be visible
            assert_eq!(final_album.name().unwrap(), "Updated by Trx1");
            assert_eq!(final_album.year().unwrap(), "2025");
        }
        Err(e) => {
            // If it fails, it should be a clear error, not BudgetExceeded
            println!("Transaction 2 failed with: {:?}", e);

            // Check that it's not a BudgetExceeded error with wrong frontiers
            let error_str = format!("{:?}", e);
            if error_str.contains("BudgetExceeded") {
                panic!("Got BudgetExceeded error, which suggests lineage comparison is failing: {}", error_str);
            }
        }
    }

    Ok(())
}

/// Test rapid concurrent transactions to stress test the system
#[tokio::test]
async fn test_many_concurrent_transactions() -> Result<()> {
    let context = durable_sled_setup().await?.context_async(DEFAULT_CONTEXT).await;

    // Create initial entity
    let album_id = {
        let trx = context.begin();
        let album = trx.create(&Album { name: "Counter".to_owned(), year: "0".to_owned() }).await?;
        let id = album.id();
        trx.commit().await?;
        id
    };

    let album = context.get::<AlbumView>(album_id).await?;

    // Create 5 concurrent transactions
    let mut handles = vec![];
    for i in 0..5 {
        let album = album.clone();
        let ctx = context.clone();

        let handle = tokio::spawn(async move {
            let trx = ctx.begin();
            let album_mut = album.edit(&trx)?;
            // Each transaction updates the year field to a different value
            album_mut.year().replace(&format!("{}", i))?;
            trx.commit().await
        });
        handles.push(handle);
    }

    // Wait for all transactions and collect results
    let mut successes = 0;
    let mut failures = 0;
    for handle in handles {
        match handle.await? {
            Ok(_) => successes += 1,
            Err(e) => {
                failures += 1;
                let error_str = format!("{:?}", e);
                if error_str.contains("BudgetExceeded") {
                    panic!("Got BudgetExceeded error in concurrent transactions: {}", error_str);
                }
                println!("Transaction failed (expected): {:?}", e);
            }
        }
    }

    println!("Results: {} successes, {} failures", successes, failures);

    // At least the first transaction should succeed
    assert!(successes >= 1, "At least one transaction should succeed");

    Ok(())
}

/// Test concurrent transactions with a long lineage before the fork
/// This should reproduce the BudgetExceeded issue
#[tokio::test]
async fn test_concurrent_transactions_long_lineage() -> Result<()> {
    let context = durable_sled_setup().await?.context_async(DEFAULT_CONTEXT).await;

    // Create initial entity and build up a long lineage
    let album_id = {
        let trx = context.begin();
        let album = trx.create(&Album { name: "Initial".to_owned(), year: "0".to_owned() }).await?;
        let id = album.id();
        trx.commit().await?;
        id
    };

    // Make 20 sequential updates to build lineage
    for i in 1..=20 {
        let album = context.get::<AlbumView>(album_id).await?;
        let trx = context.begin();
        let album_mut = album.edit(&trx)?;
        album_mut.year().replace(&format!("{}", i))?;
        trx.commit().await?;
    }

    // Now create concurrent transactions that both fork from the same (latest) head
    let album = context.get::<AlbumView>(album_id).await?;

    let trx1 = context.begin();
    let trx2 = context.begin();

    let album_mut1 = album.edit(&trx1)?;
    let album_mut2 = album.edit(&trx2)?;

    album_mut1.name().replace("Updated by Trx1")?;
    album_mut2.name().replace("Updated by Trx2")?;

    // Commit first transaction
    trx1.commit().await?;

    // Commit second transaction - this should handle concurrency correctly
    // With the bug, this will try to traverse all the way back to root and hit BudgetExceeded
    let result = trx2.commit().await;

    match result {
        Ok(_) => {
            println!("Transaction 2 succeeded");
        }
        Err(e) => {
            let error_str = format!("{:?}", e);
            println!("Transaction 2 failed: {}", error_str);

            // This is the bug we're looking for
            if error_str.contains("BudgetExceeded") {
                panic!("Hit BudgetExceeded due to traversing too far back! Error: {}", error_str);
            }
        }
    }

    Ok(())
}
