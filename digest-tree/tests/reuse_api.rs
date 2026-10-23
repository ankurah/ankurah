//! Reuse examples checked against independently specified membership.
mod common;
use ankurah_digest_tree::{Digest, Leaf, TreeMatch, TreeRequest};
use common::{populated_tree, types};

#[tokio::test]
async fn one_tree_serves_its_own_selection_whole_and_a_narrower_one_as_a_span() {
    let above_7 = selection!("project = 'acme' AND foo > 7 ORDER BY foo");
    let tree = populated_tree(&above_7).await;
    let mut tx = tree.begin();
    for (selection, kind, ids) in [
        (above_7, TreeMatch::Whole, vec![0, 1, 2, 3]),
        (selection!("project = 'acme' AND foo > 7"), TreeMatch::Whole, vec![0, 1, 2, 3]),
        (selection!("project = 'acme' AND foo > 7 ORDER BY bar"), TreeMatch::Whole, vec![0, 1, 2, 3]),
        (selection!("project = 'acme' AND foo > 10 ORDER BY foo"), TreeMatch::Span, vec![2, 3]),
        (selection!("project = 'acme' AND foo > 10"), TreeMatch::Span, vec![2, 3]),
        (selection!("project = 'acme' AND foo > 7 AND foo <= 12"), TreeMatch::Span, vec![0, 1, 2]),
        (selection!("project = 'acme' AND foo = 10"), TreeMatch::Span, vec![1]),
    ] {
        let request = TreeRequest::for_selection(&selection, &types()).unwrap();
        assert_eq!(tree.spec().serves(&request), Some(kind));
        let scope = tx.scope(&request).await.unwrap();
        assert_eq!(tx.count(&scope).await.unwrap(), ids.len() as u64);
        let page = tx.page(&scope, None, 20).await.unwrap();
        assert_eq!(
            page.leaves.iter().map(|row| row.entity_id).collect::<Vec<_>>(),
            ids.iter().copied().map(common::entity).collect::<Vec<_>>()
        );
        let expected: Digest = page.leaves.iter().map(|row| Digest::from(Leaf::new(&row.key, row.entity_id, row.head).element())).sum();
        assert_eq!(tx.digest(&scope).await.unwrap(), expected);
    }
}

#[tokio::test]
async fn limits_and_reverse_order_use_scopes_of_the_same_tree() {
    let tree = populated_tree(&selection!("project = 'acme' AND foo > 7")).await;
    let mut tx = tree.begin();
    for (selection, ids) in [
        (selection!("project = 'acme' AND foo > 7 ORDER BY foo DESC"), vec![0, 1, 2, 3]),
        (selection!("project = 'acme' AND foo > 7 ORDER BY foo LIMIT 2"), vec![0, 1]),
        (selection!("project = 'acme' AND foo > 7 ORDER BY foo DESC LIMIT 2"), vec![3, 2]),
        (selection!("project = 'acme' AND foo > 10 ORDER BY foo DESC LIMIT 1"), vec![3]),
        (selection!("project = 'acme' AND foo > 7 ORDER BY foo LIMIT 0"), vec![]),
    ] {
        let request = TreeRequest::for_selection(&selection, &types()).unwrap();
        let scope = tx.scope(&request).await.unwrap();
        assert_eq!(tx.count(&scope).await.unwrap(), ids.len() as u64);
        let mut found = Vec::new();
        let mut cursor = None;
        loop {
            let page = tx.page(&scope, cursor.as_ref(), 1).await.unwrap();
            found.extend(page.leaves.iter().map(|row| row.entity_id));
            cursor = page.next;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(found, ids.into_iter().map(common::entity).collect::<Vec<_>>());
    }
}

#[tokio::test]
async fn missing_members_and_conditions_outside_the_key_prevent_reuse() {
    let tree = populated_tree(&selection!("project = 'acme' AND foo > 7")).await;
    for selection in [
        selection!("project = 'acme' AND foo > 3 ORDER BY foo"),
        selection!("project = 'acme'"),
        selection!("project = 'zed' AND foo > 10"),
        selection!("foo > 10"),
        selection!("project = 'acme' AND foo > 7 AND bar > 0"),
        selection!("project = 'acme' AND foo > 7 ORDER BY bar LIMIT 2"),
    ] {
        let request = TreeRequest::for_selection(&selection, &types()).unwrap();
        assert_eq!(tree.spec().serves(&request), None, "{selection}");
        assert!(tree.begin().scope(&request).await.is_err());
    }
}
