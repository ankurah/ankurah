use ankurah_digest_tree::{Page, Span};

use super::*;

#[tokio::test]
async fn comparison_pages_expose_only_the_authorized_tree_structure() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(0)?;
    let versions: Vec<_> = (1u32..=600)
        .map(|number| {
            let mut bytes = [0; 32];
            bytes[..4].copy_from_slice(&number.to_be_bytes());
            Version {
                id: EntityId::from_bytes(bytes),
                score: if number <= 150 { i64::from(number) } else { 10_000 + i64::from(number) },
                active: Some(true),
                head: Clock::genesis(EventId::from_bytes(bytes)),
            }
        })
        .collect();
    fixture.seed(&versions)?;
    let expected = Fixture::<Ristretto255>::new(0)?;
    expected.seed(&versions[..150])?;
    let selection = selection(false, None, false);
    let expected = expected.manager.open(selection.clone()).await?;
    let (owner, capture) = fixture
        .manager
        .capture_for_comparison(selection, Some(&mut |id, _| Ok(u32::from_be_bytes(id.to_bytes()[..4].try_into().unwrap()) <= 150)))
        .await?;

    assert_eq!(capture.key_spec(), expected.key_spec());
    assert_eq!(capture.audit().await?.members, 150);
    assert_eq!(owner.view().count(..).await?, versions.len() as u64);
    assert_eq!(fixture.manager.trees().len(), 1, "the owner keeps the original tree managed without idle retention");

    // Check every returned bound, digest and leaf, not just root equality.
    // A span-filtered view of the original trie can leak denied prefixes.
    let mut spans = vec![Span::all()];
    while !spans.is_empty() {
        let pages = capture.view().pages(spans.clone()).await?;
        assert_eq!(pages, expected.view().pages(spans).await?);
        spans = pages
            .into_iter()
            .flat_map(|page| match page {
                Page::Digests(digests) => {
                    digests.into_iter().filter(|digest| digest.digest.count() > 0).map(|digest| digest.bounds).collect()
                }
                Page::Members(_) => Vec::new(),
            })
            .collect();
    }

    fixture.manager.flush().await?;
    assert_eq!(owner.view().count(..).await?, versions.len() as u64, "authorization edits never enter the managed or durable tree");
    assert_eq!(capture.view().count(..).await?, 150);
    let owner_weak = owner.downgrade();
    drop(owner);
    assert!(owner_weak.upgrade().is_none(), "the private copy is not a second registered owner");
    assert!(fixture.manager.trees().is_empty());
    assert_eq!(capture.view().count(..).await?, 150);
    Ok(())
}

#[tokio::test]
async fn capture_filters_a_limited_selection_without_refilling_it() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(0)?;
    let versions = [version(1, 10, true), version(2, 20, true), version(3, 30, true)];
    fixture.seed(&versions)?;
    let (owner, capture) =
        fixture.manager.capture_for_comparison(selection(false, Some(2), false), Some(&mut |id, _| Ok(id != versions[0].id))).await?;
    assert_eq!(ids(&owner.view()).await?, vec![versions[0].id, versions[1].id]);
    assert_eq!(ids(&capture.view()).await?, vec![versions[1].id]);
    assert_eq!(capture.view().range(..)?.try_next().await?.unwrap().payload.0, versions[1].head);
    Ok(())
}

#[tokio::test]
async fn capture_preserves_the_authorized_heads_after_managed_updates() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(0)?;
    let before = version(1, 10, true);
    fixture.seed(std::slice::from_ref(&before))?;
    let mut checked = Vec::new();
    let (owner, capture) = fixture
        .manager
        .capture_for_comparison(
            selection(false, None, false),
            Some(&mut |id, state| {
                checked.push((id, state.head.clone()));
                Ok(true)
            }),
        )
        .await?;
    assert_eq!(checked, vec![(before.id, before.head.clone())]);
    let captured_digest = capture.view().span_digest(..).await?;
    let after = Version { score: 20, head: Clock::genesis(EventId::from_bytes([44; 32])), ..before.clone() };
    fixture.commit(&[(Some(before.clone()), Some(after.clone()))]).await?;
    fixture.manager.flush().await?;
    let old = capture.view().range(..)?.try_next().await?.unwrap();
    let current = owner.view().range(..)?.try_next().await?.unwrap();
    assert_eq!(old.payload.0, before.head);
    assert_eq!(current.payload.0, after.head);
    assert_ne!(old.key, current.key);
    assert_eq!(capture.view().span_digest(..).await?, captured_digest);
    Ok(())
}

#[tokio::test]
async fn caller_proof_captures_every_selected_member_without_authorization_filtering() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(0)?;
    let versions = [version(1, 10, true), version(2, 20, true)];
    fixture.seed(&versions)?;
    let (owner, capture) = fixture.manager.capture_for_comparison(selection(false, None, false), None).await?;
    assert_eq!(ids(&capture.view()).await?, versions.iter().map(|version| version.id).collect::<Vec<_>>());
    assert_eq!(capture.view().span_digest(..).await?, owner.view().span_digest(..).await?);
    Ok(())
}

#[tokio::test]
async fn failed_source_validation_never_publishes_private_authorization_edits() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(0)?;
    fixture.seed(&[version(1, 10, true), version(2, 20, true)])?;
    let selection = selection(false, None, false);
    let owner = fixture.manager.open(selection.clone()).await?;
    let before = owner.view().span_digest(..).await?;
    let result = fixture
        .manager
        .capture_for_comparison(
            selection,
            Some(&mut |_, _| {
                fixture.storage.reject_state_finish.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(false)
            }),
        )
        .await;
    assert!(result.is_err());
    fixture.storage.reject_state_finish.store(false, std::sync::atomic::Ordering::SeqCst);
    fixture.manager.maintain().await?;
    assert_eq!(owner.view().span_digest(..).await?, before);
    assert_eq!(owner.view().count(..).await?, 2);
    Ok(())
}
