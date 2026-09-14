use ankql::ast::{EntityId, ModelId, Predicate, Resolved, Selection, SystemModel};

#[test]
fn membership_identities_round_trip() {
    for model in [
        ModelId::EntityId(EntityId::from_bytes([0xff; 32])),
        ModelId::System(SystemModel::System),
        ModelId::System(SystemModel::Model),
        ModelId::System(SystemModel::Property),
        ModelId::System(SystemModel::ModelProperty),
    ] {
        assert_eq!(model.to_string().parse::<ModelId>().unwrap(), model);
        let selection: Selection<Resolved> = Predicate::MemberOf(model).into();
        let parsed = ankql::parser::parse_selection(&selection.to_string()).unwrap();
        assert_eq!(parsed.predicate, Predicate::MemberOf(model.into()));
        let bytes = bincode::serialize(&selection).unwrap();
        assert_eq!(bincode::deserialize::<Selection<Resolved>>(&bytes).unwrap(), selection);
    }
    assert_eq!(ModelId::System(SystemModel::Model).to_string(), "system:model");
    assert!("system:unknown".parse::<ModelId>().is_err());
    for label in ["album", "artist's works"] {
        let selection = Selection::from(Predicate::<ankql::ast::Parsed>::MemberOf(label.into()));
        assert_eq!(ankql::parser::parse_selection(&selection.to_string()).unwrap(), selection);
    }
}

#[test]
fn membership_references_include_disjunction_and_negation() {
    let a = ModelId::EntityId(EntityId::from_bytes([1; 32]));
    let b = ModelId::EntityId(EntityId::from_bytes([2; 32]));
    let member = |model| Predicate::<Resolved>::MemberOf(model);
    let both = Predicate::And(Box::new(member(a)), Box::new(member(b)));
    assert_eq!(both.referenced_models(), [a, b].into());
    assert_eq!(Predicate::Or(Box::new(both), Box::new(member(a))).referenced_models(), [a, b].into());
    assert_eq!(Predicate::Not(Box::new(member(a))).referenced_models(), [a].into());
}
