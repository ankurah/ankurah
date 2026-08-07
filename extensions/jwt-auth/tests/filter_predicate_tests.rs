mod common;

use ankql::ast::Predicate;
use ankurah_core::policy::ReadKind;
use ankurah_core::{
    policy::{AccessDenied, PolicyAgent},
    property::backend::{LWWBackend, PropertyBackend},
    selection::filter::{evaluate_predicate, Filterable},
    util::Iterable,
    value::Value,
};
use ankurah_jwt_auth::{JwtAgent, JwtClaims, JwtContext, JwtKeys, PolicyConfig, SigningKeys};
use ankurah_proto::{self as proto, CollectionId};
use common::{blog_config_path, make_predicate};
use jwt_simple::prelude::Duration;
use std::collections::BTreeMap;

/// Root context returns predicate unchanged (bypasses all filtering)
#[test]
fn test_filter_predicate_privileged_bypasses() {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys, blog_config_path()).unwrap();

    let ctx = JwtContext::Root;
    let predicate = make_predicate("title = 'hello'");
    let collection = CollectionId::from("post");

    let result = agent.filter_predicate(&ctx, &collection, predicate.clone()).unwrap();
    assert_eq!(result, predicate, "Root context should return predicate unchanged");
}

/// Collection without scope rules returns predicate unchanged
#[test]
fn test_filter_predicate_no_scope_rules() {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path()).unwrap();

    let claims = JwtClaims {
        sub: "user-1".into(),
        roles: vec!["Editor".into()],
        email: "editor@blog.com".into(),
        name: None,
        custom: serde_json::Map::new(),
    };
    let token = keys.sign(&claims, Duration::from_hours(1)).unwrap();
    let ctx = JwtContext::from_claims(claims, token);
    let predicate = make_predicate("body = 'test'");
    let collection = CollectionId::from("comment");

    let result = agent.filter_predicate(&ctx, &collection, predicate.clone()).unwrap();
    assert_eq!(result, predicate, "No scope rules should return predicate unchanged");
}

/// Author role (lacks manage_posts) gets scope filter AND-ed in
#[test]
fn test_filter_predicate_applies_scope_rule() {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path()).unwrap();

    let claims = JwtClaims {
        sub: "author-42".into(),
        roles: vec!["Author".into()],
        email: "author@blog.com".into(),
        name: None,
        custom: serde_json::Map::new(),
    };
    let token = keys.sign(&claims, Duration::from_hours(1)).unwrap();
    let ctx = JwtContext::from_claims(claims, token);
    let predicate = make_predicate("title = 'hello'");
    let collection = CollectionId::from("post");

    let result = agent.filter_predicate(&ctx, &collection, predicate).unwrap();

    match &result {
        Predicate::And(left, right) => {
            let display = format!("{}", right);
            assert!(display.contains("author-42"), "got: {}", display);
            let left_display = format!("{}", left);
            assert!(left_display.contains("hello"), "got: {}", left_display);
        }
        other => panic!("Expected AND predicate, got: {:?}", other),
    }
}

/// Editor role (has manage_posts) sees no scope filter (unless_privilege bypasses)
#[test]
fn test_filter_predicate_unless_privilege_bypasses() {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path()).unwrap();

    let claims = JwtClaims {
        sub: "editor-1".into(),
        roles: vec!["Editor".into()],
        email: "editor@blog.com".into(),
        name: None,
        custom: serde_json::Map::new(),
    };
    let token = keys.sign(&claims, Duration::from_hours(1)).unwrap();
    let ctx = JwtContext::from_claims(claims, token);
    let predicate = make_predicate("title = 'hello'");
    let collection = CollectionId::from("post");

    let result = agent.filter_predicate(&ctx, &collection, predicate.clone()).unwrap();
    assert_eq!(result, predicate, "Editor should bypass the scope filter");
}

/// NoUser context with scope rules should return AccessDenied
#[test]
fn test_filter_predicate_nouser_denied() {
    let config_json = r#"{
        "roles": { "Worker": ["view_records"] },
        "collections": {
            "record": {
                "read": "view_records",
                "write": "manage_records",
                "scope": [{ "filter": "owner = $jwt.sub" }]
            }
        }
    }"#;
    let config: PolicyConfig = serde_json::from_str(config_json).unwrap();
    let agent = JwtAgent::new_ephemeral();
    agent.update_config(config);

    let ctx = JwtContext::NoUser;
    let predicate = make_predicate("status = 'active'");
    let collection = CollectionId::from("record");

    let result = agent.filter_predicate(&ctx, &collection, predicate);
    assert!(result.is_err(), "NoUser context with scope rules should be denied");
}

/// Scope rule with no unless_privilege applies unconditionally
#[test]
fn test_filter_predicate_unconditional_scope_rule() {
    let config_json = r#"{
        "roles": { "Admin": ["*"], "Worker": ["view_records"] },
        "collections": {
            "record": {
                "read": "view_records",
                "write": "manage_records",
                "scope": [{ "filter": "owner = $jwt.sub" }]
            }
        }
    }"#;
    let config: PolicyConfig = serde_json::from_str(config_json).unwrap();

    let keys = common::test_keys();
    let agent = JwtAgent::new_ephemeral();
    agent.update_config(config);
    agent.set_keys(JwtKeys::Signing(keys.clone()));

    let claims = JwtClaims {
        sub: "worker-1".into(),
        roles: vec!["Worker".into()],
        email: "w@co.com".into(),
        name: None,
        custom: serde_json::Map::new(),
    };
    let token = keys.sign(&claims, Duration::from_hours(1)).unwrap();
    let ctx = JwtContext::from_claims(claims, token);
    let predicate = make_predicate("status = 'active'");
    let collection = CollectionId::from("record");

    let result = agent.filter_predicate(&ctx, &collection, predicate).unwrap();
    let display = format!("{}", result);
    assert!(display.contains("worker-1"), "got: {}", display);
}

/// Injection attempt with quotes fails-closed
#[test]
fn test_filter_predicate_injection_payload_is_inert() {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path()).unwrap();

    let payload = "'; DROP TABLE posts; --";
    let claims = JwtClaims {
        sub: payload.into(),
        roles: vec!["Author".into()],
        email: "ob@blog.com".into(),
        name: None,
        custom: serde_json::Map::new(),
    };
    let token = keys.sign(&claims, Duration::from_hours(1)).unwrap();
    let ctx = JwtContext::from_claims(claims, token);
    let predicate = make_predicate("title = 'hello'");
    let collection = CollectionId::from("post");

    // Claim values are populated into the parsed AST, never spliced into the
    // filter text — the payload lands as an ordinary string literal and the
    // scope clause structure is preserved intact.
    let result = agent.filter_predicate(&ctx, &collection, predicate).expect("injection payload must be inert, not an error");
    match &result {
        Predicate::And(left, right) => {
            let scope_display = format!("{}", right);
            assert!(scope_display.contains(payload), "payload should appear as an inert literal in: {}", scope_display);
            assert!(format!("{}", left).contains("hello"), "original predicate must be preserved");
        }
        other => panic!("Expected AND predicate, got: {:?}", other),
    }
}

// ---- multi-credential filtering ------------------------------------------
//
// A context set may carry several credentials, and check_read admits a row
// that ANY authorized credential may read. These cases hold the query-time
// narrowing to that same standard: the rows the filtered predicate admits
// must be the rows check_read would admit, no fewer and no more.

/// A post to ask the filtered predicate about. Which rows survive the filter
/// is the contract, so these tests evaluate the result; they assert on its
/// shape only where the claim itself is about shape — that a lone credential
/// is not Or-wrapped, and that an unrestricted credential collapses the union
/// back to the caller's own predicate.
#[derive(Clone, Copy)]
struct Post {
    author: &'static str,
    title: &'static str,
}

impl Filterable for Post {
    fn collection(&self) -> &str { "post" }

    fn value(&self, name: &str) -> Option<Value> {
        match name {
            "author" => Some(Value::String(self.author.to_string())),
            "title" => Some(Value::String(self.title.to_string())),
            _ => None,
        }
    }
}

fn admits(predicate: &Predicate, post: Post) -> bool {
    evaluate_predicate(&post, predicate).expect("filtered predicate must be evaluable against a post")
}

/// A blog credential holding a single role, signed by `keys`.
fn blog_context(keys: &SigningKeys, sub: &str, role: &str) -> JwtContext {
    let claims = common::make_claims(sub, &[role], &format!("{sub}@blog.com"));
    let token = common::sign_token(keys, &claims);
    JwtContext::from_claims(claims, token)
}

/// The same post as a stored row. check_read reads an entity's fields out of
/// a serialized state, so the differential test below hands both sides one
/// row rather than two hand-written copies of it.
fn post_state(post: Post) -> proto::State {
    let backend = LWWBackend::new();
    backend.set("author".into(), Some(Value::String(post.author.to_string())));
    backend.set("title".into(), Some(Value::String(post.title.to_string())));
    let operations = backend.to_operations().expect("LWW diff must serialize").expect("both properties were just set");
    // A state buffer carries only values an event committed, so the writes are
    // handed the identity of a synthetic one.
    backend.apply_operations_with_event(&operations, proto::EventId::from_bytes([1u8; 32])).expect("LWW diff must apply");
    let buffer = backend.to_state_buffer().expect("committed LWW values must serialize");
    proto::State { state_buffers: proto::StateBuffers(BTreeMap::from([("lww".to_string(), buffer)])), ..Default::default() }
}

/// check_read is the row-by-row authority the filtered predicate stands in for
/// on the durable fetch and livequery paths, which never call it. For each
/// row: the filter admits it exactly when the caller asked for it and
/// check_read would hand it over.
fn assert_agrees_with_check_read<C: Iterable<JwtContext>>(agent: &JwtAgent, contexts: &C, base: &Predicate, rows: &[Post]) {
    let collection = CollectionId::from("post");
    let filtered = agent.filter_predicate(contexts, &collection, base.clone()).expect("every scenario here yields a filter");

    for row in rows {
        let readable = agent.check_read(contexts, &proto::EntityId::new(), &collection, &post_state(*row), ReadKind::Scan).is_ok();
        let selected = admits(base, *row);
        assert_eq!(
            admits(&filtered, *row),
            readable && selected,
            "post {}/{}: check_read says readable={}, the caller's predicate says selected={}, filter is {}",
            row.author,
            row.title,
            readable,
            selected,
            filtered
        );
    }
}

/// One credential yields exactly its own scope AND-ed onto the caller's
/// predicate — the union must leave a single-credential query untouched.
#[test]
fn test_filter_predicate_single_context_parity() {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path()).unwrap();

    let ctx = blog_context(&keys, "author-42", "Author");
    let collection = CollectionId::from("post");

    let result = agent.filter_predicate(&ctx, &collection, make_predicate("title = 'hello'")).unwrap();

    assert_eq!(result, make_predicate("title = 'hello' AND author = 'author-42'"), "a lone credential's narrowing must be unwrapped");
}

/// Two credentials see the union of their slices: each author's own posts
/// pass, and the caller's predicate still binds both branches.
#[test]
fn test_filter_predicate_unions_across_contexts() {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path()).unwrap();

    let contexts = vec![blog_context(&keys, "author-1", "Author"), blog_context(&keys, "author-2", "Author")];
    let collection = CollectionId::from("post");

    let result = agent.filter_predicate(&contexts, &collection, make_predicate("title = 'hello'")).unwrap();

    assert!(admits(&result, Post { author: "author-1", title: "hello" }), "the first credential's posts are readable");
    assert!(admits(&result, Post { author: "author-2", title: "hello" }), "the second credential's posts are readable too");
    assert!(!admits(&result, Post { author: "author-3", title: "hello" }), "a third author's posts belong to neither credential");
    assert!(!admits(&result, Post { author: "author-1", title: "other" }), "the caller's own predicate still binds every branch");
}

/// A credential the scope rule does not constrain (Editor holds manage_posts)
/// may read every post the caller's predicate selects, so the union collapses
/// to that predicate: no wider than what the editor could read alone, and no
/// narrower than the author's branch it swallows.
#[test]
fn test_filter_predicate_unrestricted_context_collapses_union() {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path()).unwrap();

    let contexts = vec![blog_context(&keys, "author-42", "Author"), blog_context(&keys, "editor-1", "Editor")];
    let collection = CollectionId::from("post");
    let predicate = make_predicate("title = 'hello'");

    let result = agent.filter_predicate(&contexts, &collection, predicate.clone()).unwrap();

    assert_eq!(result, predicate, "an unrestricted credential leaves nothing to union");
    assert!(admits(&result, Post { author: "someone-else", title: "hello" }), "the editor may read posts it did not author");
    assert!(admits(&result, Post { author: "author-42", title: "hello" }), "the author's own posts stay readable");
    assert!(!admits(&result, Post { author: "someone-else", title: "other" }), "the caller's own predicate still binds");
}

/// Three credentials union three ways, and every branch stays under the
/// caller's predicate.
#[test]
fn test_filter_predicate_unions_three_contexts() {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path()).unwrap();

    let contexts = vec![
        blog_context(&keys, "author-1", "Author"),
        blog_context(&keys, "author-2", "Author"),
        blog_context(&keys, "author-3", "Author"),
    ];
    let collection = CollectionId::from("post");

    let result = agent.filter_predicate(&contexts, &collection, make_predicate("title = 'hello'")).unwrap();

    assert!(admits(&result, Post { author: "author-1", title: "hello" }), "the first credential's posts are readable");
    assert!(admits(&result, Post { author: "author-2", title: "hello" }), "the second credential's posts are readable");
    assert!(admits(&result, Post { author: "author-3", title: "hello" }), "the third credential's posts are readable");
    assert!(!admits(&result, Post { author: "author-4", title: "hello" }), "a fourth author's posts belong to no credential in the set");
    assert!(!admits(&result, Post { author: "author-3", title: "other" }), "the caller's own predicate still binds the last branch");
}

/// Credentials of equal value produce equal slices, and the union keeps one:
/// the predicate a single copy would have produced.
#[test]
fn test_filter_predicate_deduplicates_equal_slices() {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path()).unwrap();
    let collection = CollectionId::from("post");

    let lone = blog_context(&keys, "author-42", "Author");
    let single = agent.filter_predicate(&lone, &collection, make_predicate("title = 'hello'")).unwrap();

    let duplicated = vec![blog_context(&keys, "author-42", "Author"), blog_context(&keys, "author-42", "Author")];
    let result = agent.filter_predicate(&duplicated, &collection, make_predicate("title = 'hello'")).unwrap();

    assert_eq!(result, single, "a repeated credential must not repeat its branch");
}

/// An empty credential set is refused on a scoped collection — there is no
/// slice to union — but the no-scope early return runs before any credential
/// is read, so an unscoped collection still returns the caller's predicate
/// untouched. The refusal is narrower than "a caller with no authorized
/// context is always denied".
#[test]
fn test_filter_predicate_empty_context_set() {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys, blog_config_path()).unwrap();

    let contexts: Vec<JwtContext> = Vec::new();
    let predicate = make_predicate("title = 'hello'");

    let scoped = agent.filter_predicate(&contexts, &CollectionId::from("post"), predicate.clone());
    assert!(scoped.is_err(), "post is scoped, so an empty set has no authorized slice, got: {:?}", scoped);

    let unscoped = agent.filter_predicate(&contexts, &CollectionId::from("comment"), predicate.clone()).unwrap();
    assert_eq!(unscoped, predicate, "comment carries no scope rules, so the query is returned before any credential is read");
}

/// An authenticated credential that cannot reach the collection contributes
/// no branch — its scope must not stand in for an authorized credential's,
/// whichever order the set iterates in. Reader lacks view_posts, and listing
/// it first is the case the old first-match narrowing got wrong outright.
#[test]
fn test_filter_predicate_ignores_unauthorized_context() {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path()).unwrap();

    let contexts = vec![blog_context(&keys, "reader-1", "Reader"), blog_context(&keys, "author-42", "Author")];
    let collection = CollectionId::from("post");

    let result = agent.filter_predicate(&contexts, &collection, make_predicate("title = 'hello'")).unwrap();

    assert!(admits(&result, Post { author: "author-42", title: "hello" }), "the authorized credential's posts are readable");
    assert!(!admits(&result, Post { author: "reader-1", title: "hello" }), "the unauthorized credential opens no window of its own");
}

/// A set with no authorized credential is refused outright, the same
/// fail-closed answer the unauthenticated case gets.
#[test]
fn test_filter_predicate_no_authorized_context_denied() {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path()).unwrap();

    let ctx = blog_context(&keys, "reader-1", "Reader");
    let collection = CollectionId::from("post");

    let result = agent.filter_predicate(&ctx, &collection, make_predicate("title = 'hello'"));
    assert!(result.is_err(), "Reader cannot read the post collection at all, got: {:?}", result);
}

/// A privileged credential anywhere in the set bypasses filtering entirely,
/// ahead of any union.
#[test]
fn test_filter_predicate_privileged_precedes_union() {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path()).unwrap();

    let contexts = vec![blog_context(&keys, "author-42", "Author"), JwtContext::Root];
    let predicate = make_predicate("title = 'hello'");
    let collection = CollectionId::from("post");

    let result = agent.filter_predicate(&contexts, &collection, predicate.clone()).unwrap();
    assert_eq!(result, predicate, "Root alongside a scoped credential still returns the predicate unchanged");
}

// ---- credentials whose scope cannot be constructed ------------------------
//
// A scope filter names claims, and a token need not carry them. Such a
// credential is skipped by both halves with a warning: it admits no row and
// denies none either, so a caller loses nothing by presenting it alongside a
// credential that works. A caller holding nothing but these is refused.

/// A blog whose post scope names a claim a token need not carry: the author a
/// credential is confined to lives in a custom claim, so a token minted
/// without it has a read scope that cannot be constructed at all. The shipped
/// fixture cannot express that — its scope reads $jwt.sub, which every token
/// carries — and these cases build the policy inline the way the neighbours
/// above do.
fn custom_author_agent(keys: &SigningKeys) -> JwtAgent {
    let config_json = r#"{
        "roles": {
            "Author": ["view_posts", "create_posts"],
            "Editor": ["view_posts", "manage_posts"]
        },
        "collections": {
            "post": {
                "read": "view_posts",
                "write": "manage_posts",
                "scope": [
                    {
                        "filter": "author = $jwt.custom.author_id",
                        "unless_privilege": "manage_posts"
                    }
                ]
            }
        }
    }"#;
    let config: PolicyConfig = serde_json::from_str(config_json).unwrap();

    let agent = JwtAgent::new_ephemeral();
    agent.update_config(config);
    agent.set_keys(JwtKeys::Signing(keys.clone()));
    agent
}

/// An Author credential carrying the author_id claim its read scope names.
fn resolvable_author(keys: &SigningKeys, author_id: &str) -> JwtContext {
    let mut claims = common::make_claims(author_id, &["Author"], &format!("{author_id}@blog.com"));
    claims.custom.insert("author_id".to_string(), serde_json::Value::String(author_id.to_string()));
    let token = common::sign_token(keys, &claims);
    JwtContext::from_claims(claims, token)
}

/// An Author credential minted without that claim: authorized for the
/// collection, but its read scope cannot be constructed.
fn unresolvable_author(keys: &SigningKeys, sub: &str) -> JwtContext {
    let claims = common::make_claims(sub, &["Author"], &format!("{sub}@blog.com"));
    let token = common::sign_token(keys, &claims);
    JwtContext::from_claims(claims, token)
}

/// A credential whose scope cannot be constructed contributes nothing, and the
/// query still runs: it is narrowed to what the working credential may read,
/// exactly as if the broken credential had never been presented.
#[test]
fn test_filter_predicate_skips_unresolvable_credential() {
    let keys = common::test_keys();
    let agent = custom_author_agent(&keys);
    let collection = CollectionId::from("post");
    let predicate = make_predicate("title = 'hello'");

    let contexts = vec![unresolvable_author(&keys, "no-claim-1"), resolvable_author(&keys, "author-42")];

    let result =
        agent.filter_predicate(&contexts, &collection, predicate.clone()).expect("one unresolvable credential must not refuse the query");

    assert!(admits(&result, Post { author: "author-42", title: "hello" }), "the working credential's posts stay readable");
    assert!(!admits(&result, Post { author: "no-claim-1", title: "hello" }), "the skipped credential opens no window of its own");
    assert!(!admits(&result, Post { author: "author-42", title: "other" }), "the caller's own predicate still binds");
    assert_eq!(
        result,
        agent.filter_predicate(&resolvable_author(&keys, "author-42"), &collection, predicate).unwrap(),
        "the broken credential must cost the caller nothing at all"
    );
}

/// A caller whose every credential has an unresolvable scope has no slice left
/// to union, and is refused by the query-time half and denied by the row-time
/// half alike — the same fail-closed answer a caller holding nothing
/// authorized gets.
#[test]
fn test_filter_predicate_all_unresolvable_refused() {
    let keys = common::test_keys();
    let agent = custom_author_agent(&keys);
    let collection = CollectionId::from("post");
    let predicate = make_predicate("title = 'hello'");
    let row = post_state(Post { author: "no-claim-1", title: "hello" });

    // Naming the refusals pins where they come from: the skip must walk off the
    // end of the loop into the union's own denial, never propagate the
    // resolution error it swallowed.
    let lone = unresolvable_author(&keys, "no-claim-1");
    let filtered = agent.filter_predicate(&lone, &collection, predicate.clone());
    assert!(
        matches!(filtered, Err(AccessDenied::ByPolicy("No authorized context for row filtering"))),
        "a lone unresolvable credential leaves nothing to read by, got: {:?}",
        filtered
    );
    let read = agent.check_read(&lone, &proto::EntityId::new(), &collection, &row, ReadKind::Scan);
    assert!(
        matches!(read, Err(AccessDenied::ByPolicy("Read outside permitted scope"))),
        "the row-time half must deny the caller the query-time half refused, got: {:?}",
        read
    );

    let several = vec![unresolvable_author(&keys, "no-claim-1"), unresolvable_author(&keys, "no-claim-2")];
    let filtered = agent.filter_predicate(&several, &collection, predicate);
    assert!(
        matches!(filtered, Err(AccessDenied::ByPolicy("No authorized context for row filtering"))),
        "several unresolvable credentials are no better than one, got: {:?}",
        filtered
    );
    let read = agent.check_read(&several, &proto::EntityId::new(), &collection, &row, ReadKind::Scan);
    assert!(
        matches!(read, Err(AccessDenied::ByPolicy("Read outside permitted scope"))),
        "the row-time half must deny that caller too, got: {:?}",
        read
    );
}

/// Both halves skip the same credential, so the answer cannot depend on the
/// order the caller presents its credentials in — the property the skip exists
/// for.
#[test]
fn test_filter_predicate_unresolvable_order_independent() {
    let keys = common::test_keys();
    let agent = custom_author_agent(&keys);
    let collection = CollectionId::from("post");
    let predicate = make_predicate("title = 'hello'");

    let broken_first = vec![unresolvable_author(&keys, "no-claim-1"), resolvable_author(&keys, "author-42")];
    let broken_last = vec![resolvable_author(&keys, "author-42"), unresolvable_author(&keys, "no-claim-1")];

    let first = agent.filter_predicate(&broken_first, &collection, predicate.clone()).unwrap();
    let last = agent.filter_predicate(&broken_last, &collection, predicate).unwrap();
    assert_eq!(first, last, "the filtered query must not depend on credential order");

    for row in [Post { author: "author-42", title: "hello" }, Post { author: "no-claim-1", title: "hello" }] {
        let state = post_state(row);
        assert_eq!(
            agent.check_read(&broken_first, &proto::EntityId::new(), &collection, &state, ReadKind::Scan).is_ok(),
            agent.check_read(&broken_last, &proto::EntityId::new(), &collection, &state, ReadKind::Scan).is_ok(),
            "post {}/{}: the row-time half must not depend on credential order either",
            row.author,
            row.title
        );
    }
}

/// The row-level contract this section claims, executed: over every credential
/// set the cases above assert shape or row survival for, the filtered
/// predicate and check_read admit the same posts.
#[test]
fn test_filter_predicate_agrees_with_check_read() {
    let keys = common::test_keys();
    let agent = JwtAgent::new_durable(keys.clone(), blog_config_path()).unwrap();
    let base = make_predicate("title = 'hello'");
    let rows = [
        Post { author: "author-1", title: "hello" },
        Post { author: "author-2", title: "hello" },
        Post { author: "author-42", title: "hello" },
        Post { author: "reader-1", title: "hello" },
        Post { author: "someone-else", title: "hello" },
        Post { author: "author-1", title: "other" },
        Post { author: "someone-else", title: "other" },
        Post { author: "no-claim-1", title: "hello" },
        Post { author: "no-claim-1", title: "other" },
    ];

    let lone_author = blog_context(&keys, "author-42", "Author");
    let two_authors = vec![blog_context(&keys, "author-1", "Author"), blog_context(&keys, "author-2", "Author")];
    let author_and_editor = vec![blog_context(&keys, "author-42", "Author"), blog_context(&keys, "editor-1", "Editor")];
    let reader_and_author = vec![blog_context(&keys, "reader-1", "Reader"), blog_context(&keys, "author-42", "Author")];
    let author_and_root = vec![blog_context(&keys, "author-42", "Author"), JwtContext::Root];

    assert_agrees_with_check_read(&agent, &lone_author, &base, &rows);
    assert_agrees_with_check_read(&agent, &two_authors, &base, &rows);
    assert_agrees_with_check_read(&agent, &author_and_editor, &base, &rows);
    assert_agrees_with_check_read(&agent, &reader_and_author, &base, &rows);
    assert_agrees_with_check_read(&agent, &author_and_root, &base, &rows);

    // The broken-plus-working pair needs a scope naming a claim a token can
    // lack, which the blog fixture's $jwt.sub never is, so the pair rides its
    // own policy through the same oracle. A credential skipped by one half but
    // not the other surfaces here as a row the two disagree about, in either
    // order the caller might present them in.
    let custom = custom_author_agent(&keys);
    let broken_and_working = vec![unresolvable_author(&keys, "no-claim-1"), resolvable_author(&keys, "author-42")];
    let working_and_broken = vec![resolvable_author(&keys, "author-42"), unresolvable_author(&keys, "no-claim-1")];

    assert_agrees_with_check_read(&custom, &broken_and_working, &base, &rows);
    assert_agrees_with_check_read(&custom, &working_and_broken, &base, &rows);
}
