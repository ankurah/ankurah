#[cfg(test)]
mod tests {
    use crate::changes::{ChangeSet, RecordChange, RecordChangeKind};
    use crate::derive_deps::wasm_bindgen::prelude::wasm_bindgen;
    use crate::model::{Model, Record, RecordInner, ScopedRecord};
    use crate::node::Node;
    use crate::property::{Backends, YrsString};
    use crate::storage::SledStorageEngine;
    use ankurah_derive::Model;
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Clone, Model)]
    pub struct Pet {
        #[active_value(YrsString)]
        pub name: String,
        #[active_value(YrsString)]
        pub age: String,
    }

    #[tokio::test]
    async fn test_subscription_and_notification() {
        // Create a new node
        let node = Node::new(Arc::new(SledStorageEngine::new_test().unwrap())).await;

        // Set up a subscription to watch for records with name = 'Rex' OR (age > 2 and age < 5)
        let predicate =
            ankql::parser::parse_selection("name = 'Rex' OR (age > 2 and age < 5)").unwrap();

        // Track received changesets
        let received_changesets = Arc::new(Mutex::new(Vec::new()));
        let received_changesets_clone = received_changesets.clone();

        // Helper function to check received changesets
        let check_received = move || {
            let mut changesets = received_changesets.lock().unwrap();
            let result: Vec<(Vec<ankurah_proto::RecordEvent>, RecordChangeKind)> = (*changesets)
                .iter()
                .map(|c: &ChangeSet| (c.changes[0].updates.clone(), c.changes[0].kind.clone()))
                .collect();
            changesets.clear();
            result
        };

        // Subscribe to changes
        let _handle = node
            .subscribe("pets", &predicate, move |changeset| {
                let mut received = received_changesets_clone.lock().unwrap();
                received.push(changeset);
            })
            .await
            .unwrap();

        // Create some test records
        let trx = node.begin();
        let rex = trx
            .create(&Pet {
                name: "Rex".to_string(),
                age: "1".to_string(),
            })
            .await;

        let snuffy = trx
            .create(&Pet {
                name: "Snuffy".to_string(),
                age: "2".to_string(),
            })
            .await;

        let jasper = trx
            .create(&Pet {
                name: "Jasper".to_string(),
                age: "6".to_string(),
            })
            .await;

        trx.commit().await.unwrap();

        // Verify initial state
        let received = check_received();
        assert_eq!(received.len(), 1); // Should have received one changeset for Rex (matches by name)

        // Update Rex's age to 7
        let trx = node.begin();
        let mut rex_edit = trx.edit(&rex).await.unwrap();
        rex_edit.set_age("7".to_string());
        trx.commit().await.unwrap();

        // Verify Rex's update was received
        let received = check_received();
        assert_eq!(received.len(), 1); // Should have received one changeset for Rex's age change

        // Update Snuffy's age to 3
        let trx = node.begin();
        let mut snuffy_edit = trx.edit(&snuffy).await.unwrap();
        snuffy_edit.set_age("3".to_string());
        trx.commit().await.unwrap();

        // Verify Snuffy's update was received (now matches age > 2 and age < 5)
        let received = check_received();
        assert_eq!(received.len(), 1);

        // Update Jasper's age to 4
        let trx = node.begin();
        let mut jasper_edit = trx.edit(&jasper).await.unwrap();
        jasper_edit.set_age("4".to_string());
        trx.commit().await.unwrap();

        // Verify Jasper's update was received (now matches age > 2 and age < 5)
        let received = check_received();
        assert_eq!(received.len(), 1);

        // Update Snuffy and Jasper to ages outside the range
        let trx = node.begin();
        let mut snuffy_edit = trx.edit(&snuffy).await.unwrap();
        snuffy_edit.set_age("5".to_string());
        let mut jasper_edit = trx.edit(&jasper).await.unwrap();
        jasper_edit.set_age("6".to_string());
        trx.commit().await.unwrap();

        // Verify both updates were received as removals
        let received = check_received();
        assert_eq!(received.len(), 2); // Should have received two changesets for removals

        // Update Rex to no longer match the query (instead of deleting)
        // This should still trigger a RecordChangeKind::Remove since it no longer matches
        let trx = node.begin();
        let mut rex_edit = trx.edit(&rex).await.unwrap();
        rex_edit.set_name("NotRex".to_string()); // No longer matches name = 'Rex'
        trx.commit().await.unwrap();

        // Verify Rex's "removal" was received
        let received = check_received();
        assert_eq!(received.len(), 1); // Should have received one changeset for Rex no longer matching
        assert_eq!(received[0].1, RecordChangeKind::Remove); // Should be a removal (from the result set)
    }
}
