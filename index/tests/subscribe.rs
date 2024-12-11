// use std::sync::Arc;
// use std::sync::atomic::{AtomicUsize, Ordering};

// #[test]
// fn test_rental_age_subscription() {
//     println!("🔄 Starting pubsub example...\n");

//     // Create a server with one initial record
//     let mut server = TestServer::new(vec![TestRecord {
//         name: "Alice".to_string(),
//         age: "36".to_string(),
//     }]);

//     // Subscribe to records for people who can rent a car (25-90)
//     println!("👀 Creating subscription for rental age (25-90)");
//     let subscription = server.subscribe("age >= 25 AND age <= 90");
//     let expected = Arc::new(AtomicUsize::new(1));
//     let expected_for_closure = expected.clone();

//     let _subscription = subscription.subscribe(move |op, state| {
//         match &op {
//             Op::Add {
//                 record, updates, ..
//             } => {
//                 println!("   ➕ Added: {record:?}");
//                 if !updates.is_empty() {
//                     println!(
//                         "      Due to: {}",
//                         updates
//                             .iter()
//                             .map(|Update(f, v)| format!("{f}={v}"))
//                             .collect::<Vec<_>>()
//                             .join(", ")
//                     );
//                 }
//             }
//             Op::Remove {
//                 old_record,
//                 updates,
//                 ..
//             } => {
//                 println!("   ➖ Removed: {old_record:?}");
//                 println!(
//                     "      Due to: {}",
//                     updates
//                         .iter()
//                         .map(|Update(f, v)| format!("{f}={v}"))
//                         .collect::<Vec<_>>()
//                         .join(", ")
//                 );
//             }
//             Op::Edit {
//                 old_record,
//                 new_record,
//                 updates,
//                 ..
//             } => {
//                 println!("   ✏️  Updated:");
//                 println!("      From: {old_record:?}");
//                 println!("      To:   {new_record:?}");
//                 println!(
//                     "      Changes: {}",
//                     updates
//                         .iter()
//                         .map(|Update(f, v)| format!("{f}={v}"))
//                         .collect::<Vec<_>>()
//                         .join(", ")
//                 );
//             }
//         }
//         println!("   Current matches: {state:?}\n");
//         assert_eq!(
//             state.len(),
//             expected_for_closure.load(std::sync::atomic::Ordering::Relaxed)
//         );
//     });

//     println!("\n📝 Running test sequence...\n");

//     // Add Bob (too young to rent)
//     let id = server.insert(TestRecord {
//         name: "Bob".to_string(),
//         age: "22".to_string(),
//     });

//     // Change Bob's name while too young - should see no updates
//     server.edit(id, vec![Update("name".to_string(), "Robert".to_string())]);

//     // Bob/Robert turns 25 (can now rent)
//     expected.store(2, std::sync::atomic::Ordering::Relaxed);
//     server.edit(id, vec![Update("age".to_string(), "25".to_string())]);

//     // Robert changes name back to Bob while age matches - should see an edit
//     server.edit(id, vec![Update("name".to_string(), "Bob".to_string())]);

//     // Bob turns 26 (still can rent)
//     server.edit(id, vec![Update("age".to_string(), "26".to_string())]);

//     // Bob changes name to Robert while age matches - should see an edit
//     server.edit(id, vec![Update("name".to_string(), "Robert".to_string())]);

//     // Robert turns 91 (too old to rent)
//     expected.store(1, std::sync::atomic::Ordering::Relaxed);
//     server.edit(id, vec![Update("age".to_string(), "91".to_string())]);

//     // Changes name back to Bob while too old - should see no updates
//     server.edit(id, vec![Update("name".to_string(), "Bob".to_string())]);

//     println!("✅ Test sequence complete!");
// }
