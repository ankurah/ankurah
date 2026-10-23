//! The scenarios a sample measures, the caches the client can start from, and one case of the
//! benchmark matrix.

use std::str::FromStr;

use ankurah::LiveQuery;
use serde::Serialize;

use crate::fixture::{Fixture, StartupRowView};

/// What a sample measures. Every scenario except `Cold` keeps a live query from before the
/// measurement: the retained query.
#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// A new live query, measured until it is durably answered.
    Cold,
    /// The retained query reissues its own selection over the open connection, measured until the
    /// new version is durably answered.
    WarmRefresh,
    /// The retained query reconnects to an unchanged server, measured until it installs a fresh
    /// result.
    WarmEqual,
    /// As `WarmEqual`, after the server edited one selected row while the client was offline.
    WarmEdit,
    /// As `WarmEdit`, after one row also left the selection and a new row joined it.
    WarmMembership,
}

impl FromStr for Mode {
    type Err = anyhow::Error;

    fn from_str(mode: &str) -> anyhow::Result<Self> {
        Ok(match mode {
            "cold" => Mode::Cold,
            "warm_refresh" => Mode::WarmRefresh,
            "warm_equal" => Mode::WarmEqual,
            "warm_edit" => Mode::WarmEdit,
            "warm_membership" => Mode::WarmMembership,
            _ => anyhow::bail!("unknown mode: {mode}"),
        })
    }
}

impl Mode {
    /// Whether the measured operation reconnects the client. A reconnect ends when the retained
    /// query installs a fresh result, not at a durable-answer wait.
    pub fn reconnects(self) -> bool { matches!(self, Mode::WarmEqual | Mode::WarmEdit | Mode::WarmMembership) }

    /// Brings the fixture to this scenario's starting point and returns the retained query.
    pub async fn prepare(
        self,
        fixture: &mut Fixture,
        case: &Case<'_>,
        selection: &str,
    ) -> anyhow::Result<Option<LiveQuery<StartupRowView>>> {
        if self == Mode::Cold {
            return Ok(None);
        }
        let query = fixture.open_query(selection).await?;
        if self.reconnects() {
            fixture.go_offline().await?;
        }
        match self {
            Mode::WarmEdit => fixture.edit_offline(case, false).await?,
            Mode::WarmMembership => fixture.edit_offline(case, true).await?,
            _ => {}
        }
        Ok(Some(query))
    }

    /// Runs the measured operation and returns the query it measures.
    pub async fn measure(
        self,
        fixture: &Fixture,
        selection: &str,
        retained: Option<LiveQuery<StartupRowView>>,
    ) -> anyhow::Result<LiveQuery<StartupRowView>> {
        match (self, retained) {
            (Mode::Cold, _) => {
                let query = fixture.reader.query_wait::<StartupRowView>(selection).await?;
                query.wait_durable_answered().await?;
                Ok(query)
            }
            (Mode::WarmRefresh, Some(query)) => {
                query.update_selection_wait(selection).await?;
                query.wait_durable_answered().await?;
                Ok(query)
            }
            (_, Some(query)) => {
                fixture.link.connect(&fixture.server, &fixture.client).await?;
                Ok(query)
            }
            (_, None) => anyhow::bail!("a warm scenario needs its retained query"),
        }
    }
}

/// What the client fetched before the measured operation.
#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Cache {
    /// Nothing.
    Empty,
    /// The first half of the result.
    Half,
    /// The whole result, fetched with the measured selection itself.
    All,
    /// Every row, fetched with the unlimited predicate, so a limited query can be answered from a
    /// broader tree.
    Full,
}

impl FromStr for Cache {
    type Err = anyhow::Error;

    fn from_str(cache: &str) -> anyhow::Result<Self> {
        Ok(match cache {
            "empty" => Cache::Empty,
            "half" => Cache::Half,
            "all" => Cache::All,
            "full" => Cache::Full,
            _ => anyhow::bail!("unknown cache mode: {cache}"),
        })
    }
}

/// One cell of the matrix: the server's rows, the selection's limit, the client's cache, the
/// scenario and the link.
pub struct Case<'a> {
    pub entities: usize,
    /// The measured selection's LIMIT; `None` is unlimited.
    pub limit: Option<usize>,
    pub cache: Cache,
    pub mode: Mode,
    pub rtt_ms: u64,
    /// Serialization rate in each direction; zero is unlimited.
    pub bytes_per_second: u64,
    pub repeat: usize,
    /// A name for the revision under test, copied into every record.
    pub label: &'a str,
}

impl Case<'_> {
    /// Whether the matrix runs this combination. An empty server runs only a cold start from an
    /// empty cache, and reconnect scenarios run only from the `all` cache.
    pub fn is_runnable(&self) -> bool {
        let empty_server = self.entities == 0 && (self.cache != Cache::Empty || self.mode != Mode::Cold);
        let reconnect_without_full_cache = self.mode.reconnects() && self.cache != Cache::All;
        !empty_server && !reconnect_without_full_cache
    }

    /// How many rows the measured query holds before any offline edit.
    pub fn result_count(&self) -> usize { self.limit.unwrap_or(self.entities).min(self.entities) }

    /// The predicate the populated rows match. Limited selections order by the zero-padded `body`,
    /// because ORDER BY sorts `i64` values as strings (GitHub #210).
    pub fn predicate(&self) -> &'static str {
        if self.limit.is_some() {
            "body >= 'row-'"
        } else {
            "position >= 0"
        }
    }

    /// The measured selection: the predicate, ordered and limited when the case has a limit.
    pub fn selection(&self) -> String {
        match self.limit {
            Some(limit) => format!("{} ORDER BY body ASC LIMIT {limit}", self.predicate()),
            None => self.predicate().to_owned(),
        }
    }
}
