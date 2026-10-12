//! The reuse API by example, written before the API exists.
//! Reuse means exact digest membership; sorting unlimited results is the caller's job.
mod common;
use ankurah_digest_tree::TreeRequest;
use common::{codec, populated_tree, types};

#[tokio::test]
async fn one_tree_serves_its_own_selection_whole_and_a_narrower_one_as_a_span() {
    let above_7 = selection!("project = 'acme' AND foo > 7 ORDER BY foo");
    let tree = populated_tree(&above_7).await;
    let mut tx = tree.begin();

    for selection in [
        above_7,                                                                 // Whole, count 4
        selection!("project = 'acme' AND foo > 7"),                                // Whole, count 4
        selection!("project = 'acme' AND foo > 7 ORDER BY bar"),                   // Whole, count 4
        selection!("project = 'acme' AND foo > 10 ORDER BY foo"),                  // Span, count 2
        selection!("project = 'acme' AND foo > 10"),                               // Span, count 2
        selection!("project = 'acme' AND foo > 7 AND foo <= 12"),                  // Span, count 3
        selection!("project = 'acme' AND foo = 10"),                               // Span, count 1
    ] {
        let request = TreeRequest::for_selection(&selection, &types()).unwrap();
        let _match = tree.spec().serves(&request);
        let scope = tx.scope(&request, &codec()).await.unwrap();
        let _heads = tx.heads(&scope).await.unwrap(); // [root] for Whole; bounded heads for Span
        let _count = tx.count(&scope).await.unwrap();
    }
}

#[tokio::test]
async fn limits_and_reverse_order_use_scopes_of_the_same_tree() {
    let above_7 = selection!("project = 'acme' AND foo > 7");
    let tree = populated_tree(&above_7).await;
    let mut tx = tree.begin();

    for selection in [
        selection!("project = 'acme' AND foo > 7 ORDER BY foo DESC"),              // Whole, count 4
        selection!("project = 'acme' AND foo > 7 ORDER BY foo LIMIT 2"),           // Span, foo 9 and 10
        selection!("project = 'acme' AND foo > 7 ORDER BY foo DESC LIMIT 2"),      // Span, foo 15 and 12
        selection!("project = 'acme' AND foo > 10 ORDER BY foo DESC LIMIT 1"),     // Span, foo 15
    ] {
        let request = TreeRequest::for_selection(&selection, &types()).unwrap();
        let _match = tree.spec().serves(&request);
        let scope = tx.scope(&request, &codec()).await.unwrap();
        let _heads = tx.heads(&scope).await.unwrap();
        let _count = tx.count(&scope).await.unwrap();
    }
}

#[tokio::test]
async fn missing_members_and_conditions_outside_the_key_prevent_reuse() {
    let above_7 = selection!("project = 'acme' AND foo > 7");
    let tree = populated_tree(&above_7).await;

    for selection in [
        selection!("project = 'acme' AND foo > 3 ORDER BY foo"),                   // Admission excludes 3 < foo <= 7
        selection!("project = 'acme'"),                                           // Admission excludes foo <= 7
        selection!("project = 'zed' AND foo > 10"),                                // Admission excludes zed
        selection!("foo > 10"),                                                    // Admission excludes other projects
        selection!("project = 'acme' AND foo > 7 AND bar > 0"),                    // Cannot bound bar with this key
        selection!("project = 'acme' AND foo > 7 ORDER BY bar LIMIT 2"),           // Cannot find the cut without bar
    ] {
        let request = TreeRequest::for_selection(&selection, &types()).unwrap();
        let _match = tree.spec().serves(&request); // None: no scope from this tree
    }
}
