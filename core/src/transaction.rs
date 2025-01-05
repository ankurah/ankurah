use ankurah_proto::ID;
use std::sync::Arc;

use crate::{
    error::RetrievalError,
    model::{RecordInner, ScopedRecord},
    Model, Node,
};
use tracing::debug;

use append_only_vec::AppendOnlyVec;

// Q. When do we want unified vs individual property storage for TypeEngine operations?
// A. When we start to care about differentiating possible recipients for different properties.

pub struct Transaction {
    pub(crate) node: Arc<Node>, // only here for committing records to storage engine

    records: AppendOnlyVec<RecordInner>,

    // markers
    implicit: bool,
    consumed: bool,
}

impl Transaction {
    pub fn new(node: Arc<Node>) -> Self {
        Self {
            node,
            records: AppendOnlyVec::new(),
            implicit: true,
            consumed: false,
        }
    }

    /// Fetch a record already in the transaction.
    pub fn fetch_record_from_transaction(
        &self,
        id: ID,
        bucket_name: &'static str,
    ) -> Option<&RecordInner> {
        self.records
            .iter()
            .find(|&record| record.id() == id && record.bucket_name() == bucket_name)
    }

    /// Fetch a record.
    pub async fn fetch_record(
        &self,
        id: ID,
        bucket_name: &'static str,
    ) -> Result<&RecordInner, RetrievalError> {
        if let Some(local) = self.fetch_record_from_transaction(id, bucket_name) {
            return Ok(local);
        }

        let record_inner = self.node.fetch_record_inner(id, bucket_name).await?;
        let record_ref = self.add_record(record_inner.snapshot());
        Ok(record_ref)
    }

    pub fn add_record(&self, record: RecordInner) -> &RecordInner {
        let index = self.records.push(record);
        &self.records[index]
    }

    pub async fn create<'rec, 'trx: 'rec, M: Model>(
        &'trx self,
        model: &M,
    ) -> M::ScopedRecord<'rec> {
        let id = self.node.next_record_id();
        tracing::info!("Creating record with id: {:?}", id);
        let record_inner = model.to_record_inner(id);
        let record_ref = self.add_record(record_inner);
        <M::ScopedRecord<'rec> as ScopedRecord<'rec>>::from_record_inner(record_ref)
    }

    pub async fn edit<'rec, 'trx: 'rec, M: Model>(
        &'trx self,
        id: impl Into<ID>,
    ) -> Result<M::ScopedRecord<'rec>, crate::error::RetrievalError> {
        let id = id.into();
        let record_ref = self.fetch_record(id, M::bucket_name()).await?;
        Ok(<M::ScopedRecord<'rec> as ScopedRecord<'rec>>::from_record_inner(record_ref))
    }

    #[must_use]
    pub async fn commit(mut self) -> anyhow::Result<()> {
        self.commit_mut_ref().await
    }

    #[must_use]
    // only because Drop is &mut self not mut self
    pub(crate) async fn commit_mut_ref(&mut self) -> anyhow::Result<()> {
        tracing::info!("trx.commit_mut_ref");
        self.consumed = true;
        // this should probably be done in parallel, but microoptimizations
        let mut record_events = Vec::new();
        for record in self.records.iter() {
            debug!("trx.commit_mut_ref: record: {:?}", record);
            if let Some(record_event) = record.get_record_event()? {
                debug!("trx.commit_mut_ref: record_event: {:?}", record_event);
                record_events.push(record_event);
            }
        }

        debug!(
            "trx.commit_mut_ref: record_events HERE: {:?}",
            record_events
        );

        self.node.commit_events(&record_events).await?;

        debug!("trx.commit_mut_ref: done");
        Ok(())
    }

    pub fn rollback(mut self) {
        tracing::info!("trx.rollback");
        self.consumed = true; // just do nothing on drop
    }

    // TODO: Implement delete functionality after core query/edit operations are stable
    // For now, "removal" from result sets is handled by edits that cause records to no longer match queries
    /*
    pub async fn delete<'rec, 'trx: 'rec, M: Model>(
        &'trx self,
        id: impl Into<ID>,
    ) -> Result<(), crate::error::RetrievalError> {
        let id = id.into();
        let record = self.fetch_record(id, M::bucket_name()).await?;
        let record = Arc::new(record.clone());
        self.node.delete_record(record).await?;
        Ok(())
    }
    */
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if self.implicit && !self.consumed {
            // Since we can't make drop async, we'll need to block on the commit
            // This is not ideal, but necessary for the implicit transaction case
            // TODO: Make this a rollback, which will also mean we don't need use block_on
            // #[cfg(not(target_arch = "wasm32"))]
            // tokio::spawn(async move {
            //     self.commit_mut_ref()
            //         .await
            //         .expect("Failed to commit implicit transaction");
            // });

            // #[cfg(target_arch = "wasm32")]
            // wasm_bindgen_futures::spawn_local(async move {
            //     if let Err(e) = fut.await {
            //         tracing::error!("Failed to commit implicit transaction: {}", e);
            //     }
            // });
            // todo!()
        }
    }
}
