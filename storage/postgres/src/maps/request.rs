//! One request on a map connection: statements sent together, in their
//! order, before any answer is awaited, so that the request costs one round
//! trip however many statements it holds.

use std::future::{poll_fn, Future};
use std::ops::Bound;
use std::pin::Pin;
use std::task::Poll;

use ankurah_core::storage::pending_writes::PendingWrites;
use ankurah_core::storage::{LockName, MapRead, MapRows};
use futures_util::TryStreamExt;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Row, Statement};

use super::connection::Connection;

/// A statement's parameter, which the statement owns until it is sent.
type Parameter = Box<dyn ToSql + Sync + Send>;

/// The most bytes of keys and values one statement puts or deletes, unless
/// one write alone holds more. A map's writes past it go in several
/// statements of the one request, at no cost in round trips. It keeps a
/// statement far below the 1 GiB PostgreSQL holds in one value, as each of a
/// statement's arrays is, and keeps the entries PostgreSQL gathers from the
/// arrays before it writes them within `work_mem`, 4 MB by default, past
/// which they would go to a temporary file.
const MOST_BYTES_WRITTEN: usize = 1 << 20;

/// One step of a request, which sends its messages the first time it is
/// polled, and gives the rows of its answer.
type Step<'c> = Pin<Box<dyn Future<Output = Result<Vec<Row>, tokio_postgres::Error>> + Send + 'c>>;

/// What a step's rows answer.
enum Answer {
    /// Nothing the caller asked for: a transaction's beginning or end, a
    /// lock, or a write.
    Nothing,

    /// A [`MapRead::Get`]: the value under each key, in the keys' order.
    Got,

    /// A [`MapRead::Range`]: each entry's key and value, in the read's order.
    Ranged,
}

/// The failure of a request's first step to fail.
#[derive(Debug)]
pub(super) enum Failure {
    /// The step failed before it was sent, as one whose parameter doesn't
    /// encode does, so no step behind it was sent.
    Unsent(tokio_postgres::Error),

    /// The step failed once sent, or once it may have been: the database
    /// refused it, the connection failed, or its answer didn't decode.
    Sent(tokio_postgres::Error),
}

impl Failure {
    /// The failure of a step that failed at its first poll, in which it hands
    /// its messages to the connection: sent if the database refused it, or if
    /// the connection is closed, since the connection's task may have sent it
    /// and then failed before the poll returned; else the client's own,
    /// before sending.
    fn at_first_poll(error: tokio_postgres::Error) -> Self {
        match error.as_db_error().is_some() || error.is_closed() {
            true => Self::Sent(error),
            false => Self::Unsent(error),
        }
    }
}

/// One request's steps, in the order the database runs them, each with what
/// its rows answer.
pub(super) struct Request<'c> {
    connection: &'c Connection,
    steps: Vec<(Step<'c>, Answer)>,
}

impl<'c> Request<'c> {
    pub(super) fn on(connection: &'c Connection) -> Self { Self { connection, steps: Vec::new() } }

    /// Queue `step`, which begins or ends the transaction and answers nothing,
    /// for [`send`](Self::send).
    pub(super) fn then(&mut self, step: impl Future<Output = Result<(), tokio_postgres::Error>> + Send + 'c) {
        self.steps.push((Box::pin(async move { step.await.map(|()| Vec::new()) }), Answer::Nothing));
    }

    /// Queue the writes `pending` holds, taking them: each map's clear, then
    /// its puts, then its deletes, batched by [`MOST_BYTES_WRITTEN`].
    pub(super) fn write(&mut self, pending: &mut PendingWrites) {
        let statements = &self.connection.statements;

        for (map, changes) in pending.take() {
            let map = map.as_bytes().to_vec();
            if changes.cleared {
                self.statement(&statements.clear, vec![Box::new(map.clone())], Answer::Nothing);
            }

            let (mut put, mut deleted) = (Vec::new(), Vec::new());
            for (key, value) in changes.keys {
                match value {
                    Some(value) => put.push((key, value)),

                    // A cleared map holds nothing for a delete to remove.
                    None if changes.cleared => {}
                    None => deleted.push(key),
                }
            }

            for entries in in_statements(put, |(key, value)| key.len() + value.len()) {
                let (keys, values): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
                self.statement(&statements.put, vec![Box::new(map.clone()), Box::new(keys), Box::new(values)], Answer::Nothing);
            }

            for keys in in_statements(deleted, Vec::len) {
                self.statement(&statements.delete, vec![Box::new(map.clone()), Box::new(keys)], Answer::Nothing);
            }
        }
    }

    /// Queue acquisitions of `names` in their order.
    pub(super) fn lock(&mut self, names: &[LockName]) {
        for name in names {
            self.statement(&self.connection.statements.lock, vec![Box::new(name.as_bytes().to_vec())], Answer::Nothing);
        }
    }

    /// Queue `reads` in their order.
    pub(super) fn read(&mut self, reads: Vec<MapRead>) {
        let statements = &self.connection.statements;

        for read in reads {
            match read {
                MapRead::Get { map, keys } => {
                    self.statement(&statements.get, vec![Box::new(map.as_bytes().to_vec()), Box::new(keys)], Answer::Got)
                }

                MapRead::Range { map, start, end, direction, limit } => {
                    let statement = statements.range(&start, &end, direction);
                    let mut parameters: Vec<Parameter> = vec![Box::new(map.as_bytes().to_vec())];
                    for bound in [start, end] {
                        if let Bound::Included(key) | Bound::Excluded(key) = bound {
                            parameters.push(Box::new(key));
                        }
                    }

                    // A limit past what PostgreSQL's bigint holds reads every
                    // entry, as such a limit asks.
                    parameters.push(Box::new(i64::try_from(limit).unwrap_or(i64::MAX)));
                    self.statement(statement, parameters, Answer::Ranged);
                }
            }
        }
    }

    /// Send every step, and wait for their answers: one [`MapRows`] for each
    /// read, in the reads' order. The first step that fails fails the
    /// request, and one that fails at its first poll keeps every step behind
    /// it from being sent.
    pub(super) async fn send(self) -> Result<Vec<MapRows>, Failure> {
        let mut steps: Vec<(Step<'c>, Answer, Option<Result<Vec<Row>, tokio_postgres::Error>>)> =
            self.steps.into_iter().map(|(step, answer)| (step, answer, None)).collect();

        // Each step sends its messages the first time it is polled, so they
        // are polled once each, in order, before any is awaited. One that
        // fails then, as when a parameter fails to encode, stops the request:
        // the steps behind it are never polled.
        poll_fn(|context| {
            for (step, _, answered) in &mut steps {
                if let Poll::Ready(answer) = step.as_mut().poll(context) {
                    let failed = answer.is_err();
                    *answered = Some(answer);

                    if failed {
                        break;
                    }
                }
            }

            Poll::Ready(())
        })
        .await;

        // The database answers them in the order it ran them.
        let mut answered = Vec::with_capacity(steps.len());
        for (step, answer, rows) in steps {
            let rows = match rows {
                Some(rows) => rows.map_err(Failure::at_first_poll)?,
                None => step.await.map_err(Failure::Sent)?,
            };
            answered.push((answer, rows));
        }

        map_rows(answered).map_err(Failure::Sent)
    }

    /// Send `statement` with `parameters` in its turn, its rows answering as
    /// `answer` says.
    fn statement(&mut self, statement: &'c Statement, parameters: Vec<Parameter>, answer: Answer) {
        let client = &self.connection.client;
        self.steps.push((Box::pin(async move { client.query_raw(statement, parameters).await?.try_collect().await }), answer));
    }
}

/// `writes` in their order, in the runs that each go in one statement: at
/// most [`MOST_BYTES_WRITTEN`] of them by `bytes`, unless one alone holds
/// more.
fn in_statements<W>(writes: Vec<W>, bytes: impl Fn(&W) -> usize) -> Vec<Vec<W>> {
    let (mut runs, mut last_bytes): (Vec<Vec<W>>, usize) = (Vec::new(), 0);
    for write in writes {
        let write_bytes = bytes(&write);
        match runs.last_mut() {
            Some(last) if last_bytes + write_bytes <= MOST_BYTES_WRITTEN => last.push(write),
            _ => {
                runs.push(vec![write]);
                last_bytes = 0;
            }
        }

        last_bytes += write_bytes;
    }

    runs
}

/// What the steps' rows answer, each as its [`Answer`] says: one [`MapRows`]
/// for each read, in the reads' order.
fn map_rows(answered: Vec<(Answer, Vec<Row>)>) -> Result<Vec<MapRows>, tokio_postgres::Error> {
    let mut rows = Vec::new();
    for (answer, answered) in answered {
        match answer {
            Answer::Nothing => {}
            Answer::Got => rows.push(MapRows::Got(answered.iter().map(|row| row.try_get(0)).collect::<Result<_, _>>()?)),
            Answer::Ranged => {
                rows.push(MapRows::Ranged(answered.iter().map(|row| Ok((row.try_get(0)?, row.try_get(1)?))).collect::<Result<_, _>>()?))
            }
        }
    }

    Ok(rows)
}

#[cfg(test)]
mod tests;
