/// Yrs V2 binary fixture tests for cross-platform validation.
///
/// Each test function creates a Yrs Doc with deterministic client_id,
/// performs operations, and encodes the state as a V2 update.
///
/// - If `OVERWRITE_FIXTURES` env var is set: write the binary to the fixture file.
/// - If NOT set: read the fixture file and assert the bytes match exactly.
///
/// Run with `OVERWRITE_FIXTURES=1 cargo test -p ankurah-proto --test yrs_v2_fixtures` to regenerate.
use std::fs;
use std::path::PathBuf;

use yrs::{GetString, ReadTxn, Text, Transact, updates::decoder::Decode, Update};

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("test_fixtures")
        .join("yrs_v2")
        .join(name)
}

fn check_or_write_fixture(name: &str, data: &[u8]) {
    let path = fixture_path(name);
    let overwrite = std::env::var("OVERWRITE_FIXTURES").is_ok();

    if overwrite {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, data).unwrap();
        eprintln!("Wrote fixture: {} ({} bytes)", path.display(), data.len());
    } else if !path.exists() {
        // Auto-generate on first run if fixture doesn't exist yet
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, data).unwrap();
        eprintln!(
            "Generated missing fixture: {} ({} bytes)",
            path.display(),
            data.len()
        );
    } else {
        let expected = fs::read(&path).unwrap_or_else(|e| {
            panic!(
                "Failed to read fixture {}: {}. Run with OVERWRITE_FIXTURES=1 to generate.",
                path.display(),
                e
            )
        });
        assert_eq!(
            data,
            &expected[..],
            "Fixture mismatch for {} ({} bytes actual vs {} bytes expected)",
            name,
            data.len(),
            expected.len()
        );
    }
}

fn make_doc(client_id: u64) -> yrs::Doc {
    let options = yrs::Options {
        client_id,
        ..Default::default()
    };
    yrs::Doc::with_options(options)
}

fn encode_full_state(doc: &yrs::Doc) -> Vec<u8> {
    let txn = doc.transact();
    txn.encode_state_as_update_v2(&yrs::StateVector::default())
}

// ---- Fixture Tests ----

#[test]
fn test_empty_doc() {
    let doc = make_doc(1);
    let state = encode_full_state(&doc);
    check_or_write_fixture("empty_doc.bin", &state);
}

#[test]
fn test_simple_text() {
    let doc = make_doc(1);
    let text = doc.get_or_insert_text("content");
    {
        let mut txn = doc.transact_mut();
        text.insert(&mut txn, 0, "Hello, World!");
        txn.commit();
    }
    let state = encode_full_state(&doc);
    check_or_write_fixture("simple_text.bin", &state);
}

#[test]
fn test_multifield() {
    let doc = make_doc(2);
    let title = doc.get_or_insert_text("title");
    let description = doc.get_or_insert_text("description");
    {
        let mut txn = doc.transact_mut();
        title.insert(&mut txn, 0, "Cat video #2918");
        description.insert(&mut txn, 0, "Very cute cats playing");
        txn.commit();
    }
    let state = encode_full_state(&doc);
    check_or_write_fixture("multifield.bin", &state);
}

#[test]
fn test_text_with_edits() {
    let doc = make_doc(3);
    let text = doc.get_or_insert_text("content");
    {
        // Insert "Hello World" at 0
        let mut txn = doc.transact_mut();
        text.insert(&mut txn, 0, "Hello World");
        txn.commit();
    }
    {
        // Remove space at position 5, insert ", " -> "Hello, World"
        let mut txn = doc.transact_mut();
        text.remove_range(&mut txn, 5, 1);
        text.insert(&mut txn, 5, ", ");
        txn.commit();
    }
    {
        // Append "!" -> "Hello, World!"
        let mut txn = doc.transact_mut();
        let len = text.get_string(&txn).len() as u32;
        text.insert(&mut txn, len, "!");
        txn.commit();
    }

    // Verify the final text
    {
        let txn = doc.transact();
        let final_text = text.get_string(&txn);
        assert_eq!(final_text, "Hello, World!");
    }

    let state = encode_full_state(&doc);
    check_or_write_fixture("text_with_edits.bin", &state);
}

#[test]
fn test_incremental_base() {
    let doc = make_doc(4);
    let text = doc.get_or_insert_text("content");
    {
        let mut txn = doc.transact_mut();
        text.insert(&mut txn, 0, "Hello");
        txn.commit();
    }
    let state = encode_full_state(&doc);
    check_or_write_fixture("incremental_base.bin", &state);
}

#[test]
fn test_incremental_diff() {
    let doc = make_doc(4);
    let text = doc.get_or_insert_text("content");

    // Base state: insert "Hello"
    {
        let mut txn = doc.transact_mut();
        text.insert(&mut txn, 0, "Hello");
        txn.commit();
    }

    // Capture the base state vector
    let base_sv = doc.transact().state_vector();

    // Additional operation: append ", World!"
    {
        let mut txn = doc.transact_mut();
        let len = text.get_string(&txn).len() as u32;
        text.insert(&mut txn, len, ", World!");
        txn.commit();
    }

    // Verify the final text
    {
        let txn = doc.transact();
        let final_text = text.get_string(&txn);
        assert_eq!(final_text, "Hello, World!");
    }

    // Encode only the diff since the base state
    let diff = {
        let txn = doc.transact();
        txn.encode_state_as_update_v2(&base_sv)
    };

    check_or_write_fixture("incremental_diff.bin", &diff);
}

#[test]
fn test_concurrent_merge() {
    // Two docs with different client_ids make concurrent edits, then merge
    let doc_a = make_doc(10);
    let doc_b = make_doc(20);

    let text_a = doc_a.get_or_insert_text("content");
    let text_b = doc_b.get_or_insert_text("content");

    // Doc A inserts "Hello"
    {
        let mut txn = doc_a.transact_mut();
        text_a.insert(&mut txn, 0, "Hello");
        txn.commit();
    }

    // Doc B inserts "World" (concurrently, without seeing A's edit)
    {
        let mut txn = doc_b.transact_mut();
        text_b.insert(&mut txn, 0, "World");
        txn.commit();
    }

    // Merge: apply A's state into B, and B's state into A
    let state_a = encode_full_state(&doc_a);
    let state_b = encode_full_state(&doc_b);

    {
        let mut txn = doc_a.transact_mut();
        let update = Update::decode_v2(&state_b).unwrap();
        txn.apply_update(update).unwrap();
        txn.commit();
    }
    {
        let mut txn = doc_b.transact_mut();
        let update = Update::decode_v2(&state_a).unwrap();
        txn.apply_update(update).unwrap();
        txn.commit();
    }

    // Both docs should now have the same merged content
    let merged_a = {
        let txn = doc_a.transact();
        text_a.get_string(&txn)
    };
    let merged_b = {
        let txn = doc_b.transact();
        text_b.get_string(&txn)
    };
    assert_eq!(merged_a, merged_b, "Merged docs should be identical");

    // Save the merged state from doc_a (both should be equivalent)
    let merged_state = encode_full_state(&doc_a);
    check_or_write_fixture("concurrent_merge.bin", &merged_state);
}
