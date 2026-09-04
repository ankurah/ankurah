//! Yrs V2 binary fixture tests for cross-platform validation.
//!
//! Each test builds a Yrs Doc with a fixed client_id, performs a deterministic edit
//! sequence, and encodes it as a lib0-v2 update. Alongside every `.bin` a `.json`
//! sidecar records what a correct decoder must produce from those bytes: the text of
//! every root field, the resulting state vector, and the client ids involved. Byte
//! equality alone cannot tell a working decoder from one that never looks inside.
//!
//! These bytes are the same bytes `ankurah_core::property::backend::YrsBackend`
//! produces. Its `to_state_buffer` is `txn.encode_state_as_update_v2(&StateVector::default())`
//! and its `to_operations` is `txn.encode_diff_v2(&previous_state)`, which are the two
//! calls used here. The fixtures drive a raw `yrs::Doc` rather than a `YrsBackend`
//! because `YrsBackend::new` goes through `yrs::Doc::new`, which draws a random
//! client_id - no constructor on `YrsBackend` accepts a fixed one, so a fixture built
//! through it would not be reproducible.
//!
//! - If `OVERWRITE_FIXTURES` env var is set: write both the `.bin` and the `.json`.
//! - If NOT set: read both and assert they match exactly.
//!
//! Run with `OVERWRITE_FIXTURES=1 cargo test -p ankurah-proto --test yrs_v2_fixtures` to regenerate.
//!
//! See `proto/test_fixtures/README.md` for the fixture inventory.
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use yrs::{updates::decoder::Decode, GetString, ReadTxn, Text, Transact, Update};

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("test_fixtures")
        .join("yrs_v2")
        .join(name)
}

/// Yrs text indices are UTF-8 *byte* offsets under the default `OffsetKind::Bytes`,
/// which is what `yrs::Options::default()` sets and what these fixtures use. A JS
/// string indexes by UTF-16 code unit, so for any text containing non-ASCII the two
/// index spaces disagree - see the `unicode_text` fixture.
const OFFSET_KIND: &str =
    "Bytes (yrs Options default): Text insert/remove indices are UTF-8 byte offsets, \
     not UTF-16 code units and not Unicode scalar counts";

const ENCODING: &str = "yrs 0.24.0 update, lib0 v2 encoding (encode_state_as_update_v2 / encode_diff_v2)";

fn check_or_write_bytes(name: &str, data: &[u8]) {
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

/// Every root text field of `doc`, by name, with its rendered string.
fn text_fields(doc: &yrs::Doc) -> BTreeMap<String, String> {
    let txn = doc.transact();
    let names: Vec<String> = txn.root_refs().map(|(name, _)| name.to_owned()).collect();
    let mut fields = BTreeMap::new();
    for name in names {
        if let Some(text) = txn.get_text(name.as_str()) {
            fields.insert(name, text.get_string(&txn));
        }
    }
    fields
}

/// The doc's state vector as `{ client_id: clock }`, JSON-object-key friendly.
fn state_vector(doc: &yrs::Doc) -> BTreeMap<String, u32> {
    doc.transact()
        .state_vector()
        .iter()
        .map(|(client, clock)| (client.to_string(), *clock))
        .collect()
}

/// Write or verify a `.bin` plus the `.json` sidecar describing what it decodes to.
///
/// `applies_on_top_of` names the fixture this one is a diff against, or None when the
/// bytes are a complete document state.
fn check_or_write_fixture(
    name: &'static str,
    data: &[u8],
    doc: &yrs::Doc,
    applies_on_top_of: Option<&str>,
    note: &str,
) {
    let mut root = serde_json::Map::new();
    root.insert("fixture".into(), name.into());
    root.insert("encoding".into(), ENCODING.into());
    root.insert("offset_kind".into(), OFFSET_KIND.into());
    root.insert("total_len".into(), data.len().into());
    root.insert("note".into(), note.into());
    root.insert(
        "applies_on_top_of".into(),
        match applies_on_top_of {
            Some(base) => base.into(),
            None => serde_json::Value::Null,
        },
    );
    root.insert(
        "text_fields".into(),
        serde_json::to_value(text_fields(doc)).unwrap(),
    );
    root.insert(
        "state_vector".into(),
        serde_json::to_value(state_vector(doc)).unwrap(),
    );
    write_sidecar(name, root, data);
}

fn write_sidecar(name: &'static str, root: serde_json::Map<String, serde_json::Value>, data: &[u8]) {
    let sidecar_name = name
        .strip_suffix(".bin")
        .map(|stem| format!("{stem}.json"))
        .unwrap_or_else(|| panic!("fixture name {} must end in .bin", name));
    let mut text = serde_json::to_string_pretty(&serde_json::Value::Object(root)).unwrap();
    text.push('\n');

    check_or_write_bytes(name, data);
    check_or_write_bytes(&sidecar_name, text.as_bytes());
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
    check_or_write_fixture(
        "empty_doc.bin",
        &state,
        &doc,
        None,
        "A doc with no roots and no edits. Full-state encoding of nothing.",
    );
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
    check_or_write_fixture(
        "simple_text.bin",
        &state,
        &doc,
        None,
        "One root text, one insert at index 0, all ASCII.",
    );
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
    check_or_write_fixture(
        "multifield.bin",
        &state,
        &doc,
        None,
        "Two root texts written in one transaction. Root order in the encoding is \
         insertion order, not alphabetical.",
    );
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
    check_or_write_fixture(
        "text_with_edits.bin",
        &state,
        &doc,
        None,
        "Three transactions including a delete, so the encoding carries a delete set \
         and split blocks - the rendered string is shorter than the block content.",
    );
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
    check_or_write_fixture(
        "incremental_base.bin",
        &state,
        &doc,
        None,
        "Base state for incremental_diff.bin.",
    );
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

    check_or_write_fixture(
        "incremental_diff.bin",
        &diff,
        &doc,
        Some("incremental_base.bin"),
        "Diff only: the bytes carry the appended \", World!\" and nothing else. \
         text_fields and state_vector describe the doc AFTER applying this on top of \
         incremental_base.bin. This is the shape YrsBackend::to_operations emits.",
    );
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
    check_or_write_fixture(
        "concurrent_merge.bin",
        &merged_state,
        &doc_a,
        None,
        "Clients 10 and 20 both insert at index 0 without seeing each other. The \
         merged order is decided by client id, so the resulting string pins the \
         conflict-resolution rule, not just the byte layout.",
    );
}

// ---- Gap fixtures ------------------------------------------------------------

/// `Update::EMPTY_V2` is the exact 13-byte sequence `YrsBackend::to_operations`
/// compares its diff against to decide there is nothing to commit. A port that
/// produces any other encoding of "no change" emits spurious operations forever.
#[test]
fn test_empty_update() {
    // Round-trip proof that this constant really is what an unchanged doc encodes to.
    let doc = make_doc(5);
    let sv = doc.transact().state_vector();
    let diff = doc.transact().encode_diff_v2(&sv);
    assert_eq!(
        &diff[..],
        Update::EMPTY_V2,
        "an unchanged doc must encode to Update::EMPTY_V2"
    );

    let mut root = serde_json::Map::new();
    root.insert("fixture".into(), "empty_update.bin".into());
    root.insert("encoding".into(), ENCODING.into());
    root.insert("offset_kind".into(), OFFSET_KIND.into());
    root.insert("total_len".into(), diff.len().into());
    root.insert(
        "note".into(),
        "yrs::Update::EMPTY_V2 - the sentinel YrsBackend::to_operations compares \
         against to decide a backend produced no operations. Applying it is a no-op."
            .into(),
    );
    root.insert("applies_on_top_of".into(), serde_json::Value::Null);
    root.insert(
        "bytes".into(),
        serde_json::to_value(&diff[..]).unwrap(),
    );
    write_sidecar("empty_update.bin", root, &diff);
}

/// Text indices in yrs are UTF-8 byte offsets, not UTF-16 code units. Every insert
/// below is positioned by byte, and the resulting string is the assertion: a port
/// that indexes by JS `string.length` lands in the middle of a multi-byte sequence
/// and produces different text from identical-looking code.
#[test]
fn test_unicode_text() {
    let doc = make_doc(6);
    let text = doc.get_or_insert_text("content");
    {
        let mut txn = doc.transact_mut();
        // "café" is 5 bytes (é is 2), 4 chars, 4 UTF-16 code units.
        text.insert(&mut txn, 0, "café");
        txn.commit();
    }
    {
        let mut txn = doc.transact_mut();
        // Byte 5 is the end of "café". In UTF-16 units that position is 4.
        text.insert(&mut txn, 5, " 日本語");
        txn.commit();
    }
    {
        let mut txn = doc.transact_mut();
        // "café 日本語" is 5 + 1 + 9 = 15 bytes; 9 chars; 9 UTF-16 units.
        text.insert(&mut txn, 15, " 🚀");
        txn.commit();
    }
    {
        // Delete the 3-byte "語" that sits at bytes 12..15.
        let mut txn = doc.transact_mut();
        text.remove_range(&mut txn, 12, 3);
        txn.commit();
    }

    {
        let txn = doc.transact();
        assert_eq!(text.get_string(&txn), "café 日本 🚀");
    }

    let state = encode_full_state(&doc);
    check_or_write_fixture(
        "unicode_text.bin",
        &state,
        &doc,
        None,
        "Inserts and a delete positioned by UTF-8 byte offset across 2-, 3- and \
         4-byte sequences. The final string is 'café 日本 🚀'; a UTF-16-indexed port \
         computes different positions and produces different text.",
    );
}

/// A second field added to an existing doc in a later transaction, encoded as a diff.
/// This is the `YrsBackend::to_operations` shape for "a new property appeared", which
/// none of the existing diff fixtures covers - `incremental_diff` only extends a field
/// that already exists.
#[test]
fn test_new_field_diff() {
    let doc = make_doc(7);
    let title = doc.get_or_insert_text("title");
    {
        let mut txn = doc.transact_mut();
        title.insert(&mut txn, 0, "Original");
        txn.commit();
    }
    let base_sv = doc.transact().state_vector();

    let body = doc.get_or_insert_text("body");
    {
        let mut txn = doc.transact_mut();
        body.insert(&mut txn, 0, "Added later");
        txn.commit();
    }

    let diff = {
        let txn = doc.transact();
        txn.encode_diff_v2(&base_sv)
    };

    check_or_write_fixture(
        "new_field_diff.bin",
        &diff,
        &doc,
        Some("new_field_base.bin"),
        "Diff that introduces a root field absent from the base. text_fields and \
         state_vector describe the doc after applying this on top of new_field_base.bin.",
    );
}

/// The base that `new_field_diff.bin` applies on top of.
#[test]
fn test_new_field_base() {
    let doc = make_doc(7);
    let title = doc.get_or_insert_text("title");
    {
        let mut txn = doc.transact_mut();
        title.insert(&mut txn, 0, "Original");
        txn.commit();
    }
    let state = encode_full_state(&doc);
    check_or_write_fixture(
        "new_field_base.bin",
        &state,
        &doc,
        None,
        "Base state for new_field_diff.bin.",
    );
}

/// A document whose entire content has been deleted. The rendered text is empty but
/// the encoding is not: the tombstones stay, so an empty string here is a much
/// stronger check than the empty doc.
#[test]
fn test_fully_deleted_text() {
    let doc = make_doc(8);
    let text = doc.get_or_insert_text("content");
    {
        let mut txn = doc.transact_mut();
        text.insert(&mut txn, 0, "delete me");
        txn.commit();
    }
    {
        let mut txn = doc.transact_mut();
        text.remove_range(&mut txn, 0, 9);
        txn.commit();
    }
    {
        let txn = doc.transact();
        assert_eq!(text.get_string(&txn), "");
    }

    let state = encode_full_state(&doc);
    check_or_write_fixture(
        "fully_deleted_text.bin",
        &state,
        &doc,
        None,
        "All content deleted. The root field still exists and the delete set is \
         non-empty, so the bytes are far from the empty-doc encoding even though \
         the rendered string is ''.",
    );
}
