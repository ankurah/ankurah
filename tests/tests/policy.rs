mod common;
use common::*;

use ankurah::{
    changes::ChangeSet,
    model::{Entity, Model, View},
    policy::AccessResult,
    proto::{CollectionId, NodeId, ID},
    traits::{Context, PolicyAgent},
    Node,
};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use std::{collections::HashSet, sync::Arc};

#[derive(Debug, Clone, Copy)]
pub enum Role {
    Admin,
    User,
    Service,
}

// Example implementation for a simple role-based policy
pub struct RoleBasedPolicyAgent;

impl PolicyAgent for RoleBasedPolicyAgent {
    type Context = UserContext;

    fn can_access_collection(&self, context: &Self::Context, collection: &CollectionId) -> AccessResult {
        match context.role {
            Role::Admin => AccessResult::Allow,
            Role::User => AccessResult::Allow, // For testing, all collections are public
            Role::Service => {
                if context.allowed_collections.contains(collection) {
                    AccessResult::Allow
                } else {
                    AccessResult::Deny
                }
            }
        }
    }

    fn can_read_entity(&self, context: &Self::Context, collection: &CollectionId, _id: &ID) -> AccessResult {
        self.can_access_collection(context, collection)
    }

    fn can_modify_entity(&self, context: &Self::Context, collection: &CollectionId, _id: &ID) -> AccessResult {
        match context.role {
            Role::Admin => AccessResult::Allow,
            _ => AccessResult::Deny,
        }
    }

    fn can_create_in_collection(&self, context: &Self::Context, collection: &CollectionId) -> AccessResult {
        self.can_modify_entity(context, collection, &ID::new())
    }

    fn can_subscribe(&self, context: &Self::Context, collection: &CollectionId, _predicate: &ankql::ast::Predicate) -> AccessResult {
        self.can_access_collection(context, collection)
    }

    fn can_communicate_with_node(&self, context: &Self::Context, _node_id: &NodeId) -> AccessResult {
        match context.role {
            Role::Admin | Role::Service => AccessResult::Allow,
            Role::User => AccessResult::Deny,
        }
    }
}

// Example context implementation
pub struct UserContext {
    pub user_id: String,
    pub role: Role,
    pub allowed_collections: HashSet<CollectionId>,
    pub namespace: Option<String>,
}

impl Namespace for UserContext {
    fn namespace(&self) -> Option<&str> { self.namespace.as_deref() }
}

#[tokio::test]
async fn test_policy_controlled_subscription() -> Result<()> {
    // Create a policy agent
    let pa = RoleBasedPolicyAgent;

    // Create a node with the policy
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), pa);

    // Create an admin context that should have full access
    let context = UserContext { user_id: "admin1".into(), role: Role::Admin, allowed_collections: HashSet::new(), namespace: None };

    let album = node.create(context, &Album { name: "I would do anything for love".into(), year: "1990".into() }).await?;
    let rs: Resultset<AlbumView> = node.fetch(context, "year > 1980").await?;

    Ok(())
}
