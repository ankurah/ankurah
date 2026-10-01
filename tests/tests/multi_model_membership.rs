use ankurah::core::test_helpers::commit_transaction;
mod common;

use ankurah::{core::error::RetrievalError, model::Mutable};
use common::*;

/// A second membership is an event, not a second canonical entity.
#[tokio::test]
async fn local_and_remote_commits_preserve_plural_membership() -> anyhow::Result<()> {
    let storage = std::sync::Arc::new(SledStorageEngine::new_test()?);
    let node = Node::new_durable(storage.clone(), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await?;
    node.wait_ready().await?;
    let epoch = node.system.system_epoch().unwrap();
    assert!(matches!(Pet::descriptor().bind_local(&node.catalog, epoch), Err(RetrievalError::UnboundDeclaration { .. })));
    let album_model = ctx.resolve_model_id::<Album>().await?;
    let pet_model = ctx.resolve_model_id::<Pet>().await?;
    assert_eq!(Pet::descriptor().bind_local(&node.catalog, epoch)?, pet_model);
    let albums = ctx.query_wait::<AlbumView>("name = 'Shared entity'").await?;
    let pets = ctx.query_wait::<PetView>("true").await?;

    let trx = ctx.begin();
    let entity_id = trx.create(&Album { name: "Shared entity".into(), year: "2026".into() }).await?.id();
    trx.commit().await?;
    assert_eq!(albums.peek().iter().map(View::id).collect::<Vec<_>>(), vec![entity_id]);
    assert!(pets.peek().is_empty());

    let readonly = ctx.get::<AlbumView>(entity_id).await?;
    let trx = ctx.begin();
    for result in [
        ctx.get::<PetView>(entity_id).await.map(|_| ()),
        ctx.get_cached::<PetView>(entity_id).await.map(|_| ()),
        trx.get::<Pet>(&entity_id).await.map(|_| ()),
        trx.edit::<Pet>(readonly.entity()).map(|_| ()),
    ] {
        assert!(matches!(
            result,
            Err(RetrievalError::MissingComponent { entity_id: id, model_id }) if id == entity_id && model_id == pet_model
        ));
    }
    let entity = trx.get::<Album>(&entity_id).await?;
    entity.entity().add_membership(pet_model)?;
    let snapshot = entity.entity().clone();
    trx.commit().await?;
    assert!(matches!(snapshot.add_membership(pet_model), Err(ankurah::core::property::PropertyError::TransactionClosed)));
    let membership_events = storage.dump_entity_events(entity_id).await?;
    assert!(membership_events.iter().any(|event| event
        .payload
        .operations()
        .memberships()
        .any(|operation| matches!(operation, proto::Membership::Add(model) if *model == pet_model))));
    assert_eq!(pets.peek().iter().map(View::id).collect::<Vec<_>>(), vec![entity_id]);

    let trx = ctx.begin();
    let pet = trx.get::<Pet>(&entity_id).await?;
    pet.name()?.insert(0, "Nori")?;
    pet.age()?.insert(0, "3")?;
    trx.commit().await?;
    pets.update_selection_wait("name = 'Nori' AND age = '3'").await?;

    let plural = storage.get_state(entity_id).await?;
    assert_eq!(plural.payload.state.memberships, [album_model, pet_model].into());
    let update = proto::Event::update(entity_id, plural.payload.state.head, proto::AuthorId::Unknown, proto::OperationSet::default());
    let events = vec![proto::Attested::opt(update, None)];
    commit_transaction(&node, &DEFAULT_CONTEXT, proto::TransactionId::new(), events)
        .await?;

    let canonical = storage.get_state(entity_id).await?;
    assert_eq!(canonical.payload.state.memberships, [album_model, pet_model].into());
    // An identity lookup checks the complete membership set, not one model's projection.
    use ankql::ast::Predicate;
    use ankurah::core::storage::GetStateResult;
    let both = Predicate::And(Box::new(Predicate::MemberOf(album_model)), Box::new(Predicate::MemberOf(pet_model)));
    assert!(matches!(storage.get_states(vec![entity_id], &both).await?.as_slice(), [GetStateResult::Found(state)] if *state == canonical));
    assert!(matches!(storage.get_states(vec![entity_id], &Predicate::Not(Box::new(both))).await?.as_slice(),
        [GetStateResult::PredicateMismatch(id)] if *id == entity_id));
    let missing = proto::EntityId::random();
    assert!(matches!(storage.get_states(vec![entity_id, missing, entity_id], &Predicate::False).await?.as_slice(),
        [GetStateResult::PredicateMismatch(first), GetStateResult::NotFound(absent), GetStateResult::PredicateMismatch(last)]
        if *first == entity_id && *absent == missing && *last == entity_id));
    let selection = ankql::ast::Selection::from(ankql::ast::Predicate::<ankql::ast::Resolved>::True);
    for model in [album_model, pet_model] {
        let projected = storage.fetch_states(&selection.clone().and_member_of(model)).await?;
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0], canonical);
    }
    assert_eq!(albums.peek().iter().map(View::id).collect::<Vec<_>>(), vec![entity_id]);
    assert_eq!(pets.peek().iter().map(View::id).collect::<Vec<_>>(), vec![entity_id]);
    let client = ephemeral_sled_setup().await?;
    let _connection = LocalProcessConnection::new(&node, &client).await?;
    let client_ctx = client.context_async(DEFAULT_CONTEXT).await?;
    let client_albums = client_ctx.query_wait::<AlbumView>(nocache("name = 'Shared entity'")?).await?;
    let client_pets = client_ctx.query_wait::<PetView>(nocache("name = 'Nori' AND age = '3'")?).await?;
    assert_eq!(client_albums.peek().iter().map(View::id).collect::<Vec<_>>(), vec![entity_id]);
    assert_eq!(client_pets.peek().iter().map(View::id).collect::<Vec<_>>(), vec![entity_id]);

    let trx = ctx.begin();
    trx.get::<Album>(&entity_id).await?.year()?.overwrite(0, 4, "2027")?;
    trx.commit().await?;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if client_albums.peek().first().is_some_and(|album| album.year().ok().as_deref() == Some("2027")) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let album = client_albums.peek()[0].clone();
    let pet = client_pets.peek()[0].clone();
    assert_eq!(album.entity(), pet.entity(), "both views share one resident entity");
    assert_eq!(pet.entity().memberships(), [album_model, pet_model].into());
    let album = album.to_model()?;
    let pet = pet.to_model()?;
    assert_eq!((album.name.as_str(), album.year.as_str()), ("Shared entity", "2027"));
    assert_eq!((pet.name.as_str(), pet.age.as_str()), ("Nori", "3"));
    Ok(())
}
