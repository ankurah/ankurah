//! Reading a tree: views of the whole tree or of the part a plan selects,
//! the digests they sum over spans, the windows a plan's limit reads, and the
//! range reads that stream a view's members.

mod members;
mod span_digests;
mod view;
mod window;

pub use members::{Members, ScanRange};
pub use view::{DigestTreeView, Leaf};

use super::rows::StagedRows;
use super::{DigestTree, ScanDirection, ViewPlan};
use crate::store::LeafRow;
use crate::{DigestGroup, Error, MemberId, Payload, Row, RowKey, Span, Store};

impl<K, I, L, G, S> DigestTree<K, I, L, G, S>
where
    K: Clone + Send + Sync + 'static,
    I: MemberId,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    /// A view of the whole tree.
    pub fn view(&self) -> DigestTreeView<K, I, L, G, S> { DigestTreeView::new(self.clone(), vec![Span::all()]) }

    /// A view of the part `plan` selects. A plan with a limit reads the
    /// window when the view is made: the view covers the members the limit
    /// admits, or the plan's spans whole if fewer members lie within them,
    /// since a peer may hold the rest.
    pub async fn select(&self, plan: &ViewPlan) -> Result<DigestTreeView<K, I, L, G, S>, Error> {
        let spans = Span::normalize(plan.spans.clone());
        let spans = match plan.limit {
            None => spans,
            Some(limit) => {
                let _read = self.inner.lock.read().await;
                window::window(&StagedRows::new(&self.inner.store), spans, plan.direction, limit).await?
            }
        };

        Ok(DigestTreeView::new(self.clone(), spans))
    }
}

/// The first `limit` members within `spans`, read in `direction`: the spans
/// one after another in the order given, each in `direction`. A window
/// reads its members so, and so does each batch of a scan.
async fn first_members<'a, K, L, G, S>(
    rows: &StagedRows<'_, K, L, G, S>,
    spans: impl IntoIterator<Item = &'a Span>,
    direction: ScanDirection,
    limit: usize,
) -> Result<Vec<(Vec<u8>, LeafRow<L, G>)>, Error>
where
    K: Clone + Send + Sync + 'static,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    let mut members = Vec::new();
    for span in spans {
        if members.len() == limit {
            break;
        }

        members.extend(rows.leaves(span, direction, limit - members.len()).await?);
    }

    Ok(members)
}
