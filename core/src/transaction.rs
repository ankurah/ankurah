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

    records: AppendOnlyVec<Arc<RecordInner>>,

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
    pub async fn get_record(
        &self,
        id: ID,
        bucket_name: &'static str,
    ) -> Result<&Arc<RecordInner>, RetrievalError> {
        if let Some(record) = self
            .records
            .iter()
            .find(|record| record.id() == id && record.bucket_name() == bucket_name)
        {
            return Ok(record);
        }

        let upstream = self.node.fetch_record_inner(id, bucket_name).await?;
        Ok(self.add_record(upstream.snapshot()))
    }

    fn add_record(&self, record: Arc<RecordInner>) -> &Arc<RecordInner> {
        let index = self.records.push(record);
        &self.records[index]
    }

    pub async fn create<'rec, 'trx: 'rec, M: Model>(
        &'trx self,
        model: &M,
    ) -> M::ScopedRecord<'rec> {
        let id = self.node.next_record_id();
        let new_record = Arc::new(model.create_record(id));
        let record_ref = self.add_record(new_record);
        <M::ScopedRecord<'rec> as ScopedRecord<'rec>>::new(record_ref)
    }

    pub async fn edit<'rec, 'trx: 'rec, M: Model>(
        &'trx self,
        id: impl Into<ID>,
    ) -> Result<M::ScopedRecord<'rec>, crate::error::RetrievalError> {
        let id = id.into();
        let record = self.get_record(id, M::bucket_name()).await?;

        Ok(<M::ScopedRecord<'rec> as ScopedRecord<'rec>>::new(record))
    }

    #[must_use]
    pub async fn commit(mut self) -> anyhow::Result<()> {
        self.commit_mut_ref().await
    }

    #[must_use]
    // only because Drop is &mut self not mut self
    pub(crate) async fn commit_mut_ref(&mut self) -> anyhow::Result<()> {
        tracing::debug!("trx.commit");
        self.consumed = true;
        // this should probably be done in parallel, but microoptimizations
        let mut record_events = Vec::new();
        for record in self.records.iter() {
            if let Some(record_event) = record.commit()? {
                if let Some(upstream) = &record.upstream {
                    upstream.apply_record_event(&record_event)?;
                } else {
                    self.node.insert_record(record.clone()).await?;
                }
                record_events.push(record_event);
            }
        }
        self.node.commit_events(&record_events).await?;

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
