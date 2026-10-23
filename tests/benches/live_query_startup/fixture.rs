//! One sample's nodes and data: the server's rows, the client's cache, the retained query's
//! starting point, and the result the measured query must hold.

use std::{collections::BTreeSet, sync::Arc};

use ankurah::{policy::DEFAULT_CONTEXT, proto, Context, LiveQuery, Model, Mutable, Node, PermissiveAgent};
use serde::{Deserialize, Serialize};

use crate::{
    link::Link,
    phase,
    scenario::{Cache, Case},
    storage::CountingStorage,
    BenchNode,
};

/// The model the server is populated with. Row `i` has `position` `i` and `body` `row-` followed by
/// `i` zero-padded to eight digits, so both fields order the rows the same way.
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct StartupRow {
    pub position: i64,
    pub body: String,
}

/// What the measured query must hold at its endpoint: these members and, for a limited selection,
/// this order.
pub struct Expected {
    pub members: BTreeSet<proto::EntityId>,
    pub order: Option<Vec<proto::EntityId>>,
}

/// One sample's server and client, the link between them, and the rows the server holds.
///
/// Fields drop in declaration order, which is the reverse of their creation: the client's side
/// is torn down before the server's. Tearing the server down first can deadlock a reactor while
/// it closes its subscriptions.
pub struct Fixture {
    /// The client's context, which runs the queries.
    pub reader: Context,
    pub link: Link,
    pub client: BenchNode,
    /// Every row the server was populated with, in position order.
    pub rows: Vec<StartupRowView>,
    /// The server's context, which writes the rows.
    writer: Context,
    pub server: BenchNode,
    pub expected: Expected,
}

impl Fixture {
    /// Creates the server with the case's rows, then the client, and connects them.
    pub async fn new(case: &Case<'_>) -> anyhow::Result<Self> {
        let result_count = case.result_count();
        phase("server-create");
        let server = Node::new_durable(Arc::new(CountingStorage::new()?), PermissiveAgent::new());
        server.system.create().await?;
        let writer = server.context_async(DEFAULT_CONTEXT).await?;
        let trx = writer.begin();
        let mut members = BTreeSet::new();
        let mut rows = Vec::with_capacity(case.entities);
        for position in 0..case.entities {
            let row = trx.create(&StartupRow { position: position as i64, body: format!("row-{position:08}") }).await?;
            if position < result_count {
                members.insert(row.id());
            }
            rows.push(row.read()?);
        }
        trx.commit().await?;
        let order = case.limit.map(|_| rows.iter().take(result_count).map(|row| row.id()).collect());
        phase("server-populated");
        // Register the model even when the server holds no rows, so the client can resolve it.
        writer.resolve_model_id::<StartupRow>().await?;
        let client = Node::new(Arc::new(CountingStorage::new()?), PermissiveAgent::new());
        let link = Link::new(case.rtt_ms, case.bytes_per_second);
        phase("connect-catalog");
        link.connect(&server, &client).await?;
        let reader = client.context_async(DEFAULT_CONTEXT).await?;
        reader.resolve_model_id::<StartupRow>().await?;
        Ok(Self { server, client, link, writer, reader, rows, expected: Expected { members, order } })
    }

    /// Fetches the client's starting cache, checks how many rows it holds, and settles the link.
    /// Returns the number of rows fetched.
    pub async fn prefetch(&self, case: &Case<'_>, selection: &str) -> anyhow::Result<usize> {
        phase("prefetch");
        let result_count = case.result_count();
        let limited = case.limit.is_some();
        let cached = match case.cache {
            Cache::Empty => 0,
            Cache::Half => {
                let half = if limited {
                    format!("body >= 'row-' AND body < 'row-{:08}'", result_count / 2)
                } else {
                    format!("position < {}", result_count / 2)
                };
                self.reader.fetch::<StartupRowView>(half).await?.len()
            }
            Cache::HalfBlocks => {
                let blocks = (0..result_count)
                    .step_by(512)
                    .map(|start| {
                        let end = (start + 256).min(result_count);
                        if limited {
                            format!("(body >= 'row-{start:08}' AND body < 'row-{end:08}')")
                        } else {
                            format!("(position >= {start} AND position < {end})")
                        }
                    })
                    .collect::<Vec<_>>();
                let wanted: BTreeSet<_> = self
                    .rows
                    .iter()
                    .take(result_count)
                    .enumerate()
                    .filter(|(position, _)| position / 256 % 2 == 0)
                    .map(|(_, row)| row.id())
                    .collect();
                let mut cached_ids = BTreeSet::new();
                for batch in blocks.chunks(16) {
                    cached_ids.extend(self.reader.fetch::<StartupRowView>(batch.join(" OR ")).await?.into_iter().map(|row| row.id()));
                }
                anyhow::ensure!(cached_ids == wanted, "gapped prefetch membership");
                cached_ids.len()
            }
            Cache::HalfInterleaved => {
                let positions = (0..result_count)
                    .step_by(2)
                    .map(|position| if limited { format!("body = 'row-{position:08}'") } else { format!("position = {position}") })
                    .collect::<Vec<_>>();
                let wanted: BTreeSet<_> = self
                    .rows
                    .iter()
                    .take(result_count)
                    .enumerate()
                    .filter(|(position, _)| position % 2 == 0)
                    .map(|(_, row)| row.id())
                    .collect();
                let mut cached_ids = BTreeSet::new();
                for batch in positions.chunks(32) {
                    cached_ids.extend(self.reader.fetch::<StartupRowView>(batch.join(" OR ")).await?.into_iter().map(|row| row.id()));
                }
                anyhow::ensure!(cached_ids == wanted, "gapped prefetch membership");
                cached_ids.len()
            }
            Cache::All => {
                let fetched = self.reader.fetch::<StartupRowView>(selection).await?;
                if let Some(order) = &self.expected.order {
                    anyhow::ensure!(
                        fetched.iter().map(|row| row.id()).collect::<Vec<_>>() == *order,
                        "prefetch has incorrect ordered membership"
                    );
                }
                fetched.len()
            }
            Cache::Full => self.reader.fetch::<StartupRowView>(case.predicate()).await?.len(),
        };
        let coverage = match case.cache {
            Cache::Empty => 0,
            Cache::Half => result_count / 2,
            Cache::HalfBlocks => (0..result_count).filter(|position| position / 256 % 2 == 0).count(),
            Cache::HalfInterleaved => result_count.div_ceil(2),
            Cache::All => result_count,
            Cache::Full => case.entities,
        };
        anyhow::ensure!(cached == coverage, "prefetch coverage: fetched {cached} rows, expected {coverage}");
        self.link.settle(0, None).await?;
        Ok(cached)
    }

    /// Opens the live query a warm scenario retains, waits for its durable answer, and settles the
    /// link.
    pub async fn open_query(&self, selection: &str) -> anyhow::Result<LiveQuery<StartupRowView>> {
        phase("initial-query");
        let query = self.reader.query_wait::<StartupRowView>(selection).await?;
        query.wait_durable_answered().await?;
        self.link.settle(1, None).await?;
        Ok(query)
    }

    /// Disconnects the client and waits for the messages still in flight to be dropped.
    pub async fn go_offline(&self) -> anyhow::Result<()> {
        self.link.disconnect(&self.server, &self.client);
        self.link.settle(0, None).await?;
        Ok(())
    }

    /// Edits the server while the client is offline, and updates what the retained query must hold.
    /// One selected row's payload changes; with `change_membership`, the first row also leaves the
    /// selection and a new row joins it.
    pub async fn edit_offline(&mut self, case: &Case<'_>, change_membership: bool) -> anyhow::Result<()> {
        let result_count = case.result_count();
        anyhow::ensure!(result_count > 0, "offline edits need at least one selected row");
        let limited = case.limit.is_some();
        let trx = self.writer.begin();
        let edited = &self.rows[result_count / 2];
        if limited {
            // A limited selection orders by `body`, so the edit leaves the sort key alone.
            edited.edit(&trx)?.position()?.set(&(case.entities as i64 + 1))?;
        } else {
            edited.edit(&trx)?.body()?.replace("offline-edit")?;
        }
        if change_membership {
            let first = &self.rows[0];
            if limited {
                first.edit(&trx)?.body()?.replace("excluded")?;
            } else {
                first.edit(&trx)?.position()?.set(&-1)?;
            }
            let entities = case.entities;
            let body = if limited { format!("row-{entities:08}") } else { "offline-added".to_owned() };
            let added = trx.create(&StartupRow { position: entities as i64, body }).await?;
            let added_id = added.id();
            match case.limit {
                Some(limit) => {
                    // The first row's exit pulls the next row into the window; the new row sorts
                    // last and joins only if the window reaches it.
                    let order: Vec<_> = self.rows.iter().skip(1).map(|row| row.id()).chain(std::iter::once(added_id)).take(limit).collect();
                    self.expected.members = order.iter().copied().collect();
                    self.expected.order = Some(order);
                }
                None => {
                    self.expected.members.remove(&first.id());
                    self.expected.members.insert(added_id);
                }
            }
        }
        trx.commit().await?;
        Ok(())
    }
}
