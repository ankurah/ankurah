pub mod attestation;
pub mod changes;
pub mod collation;
pub mod connector;
pub mod context;
pub mod entity;
pub mod error;
#[cfg(not(feature = "bench-internals"))]
pub(crate) mod event_dag;
// The benchmark harness (workstream E) compiles in a separate unit and needs
// to reach the DAG engine. Under `bench-internals` the module is raised to
// `pub` so `bench_support` can wrap it; the default build keeps it crate-local.
#[cfg(feature = "bench-internals")]
pub mod event_dag;

/// Narrow, feature-gated entry points into the event DAG engine for the
/// benchmark harness. Never part of the default public surface.
#[cfg(feature = "bench-internals")]
pub mod bench_support;
pub mod indexing;
pub mod livequery;
pub mod model;
pub mod node;
pub mod node_applier;
pub mod node_key;
pub mod peer_subscription;
pub mod policy;
pub mod property;
pub mod query_value;
pub mod reactor;
pub mod resultset;
pub mod retrieval;
pub mod selection;
pub mod storage;
pub mod system;
pub mod task;
pub mod transaction;
pub mod type_resolver;
pub mod util;
pub mod value;

pub mod collectionset;
pub mod schema;

#[cfg(feature = "uniffi")]
uniffi::setup_scaffolding!();
pub use model::Model;
pub use node::Node;
pub use property::Json;
pub use query_value::QueryValue;
pub use type_resolver::TypeResolver;

pub use ankurah_proto as proto;
pub use ankurah_proto::EntityId;
