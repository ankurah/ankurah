//! Digests over spans: a subtree inside a span contributes its stored digest,
//! and the members of a bucket the span cuts are summed from their elements.

use crate::tree::trie::{Node, Trie};
use crate::{Digest, DigestGroup, Error, Payload, Row, RowKey, ScanDirection, Span, Store};

impl<K, L, G, S> Trie<'_, '_, K, L, G, S>
where
    K: Clone + Send + Sync + 'static,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    /// The digest of the members within `region` beneath the node `from`.
    pub(super) async fn digest_within(&self, from: Node<G>, region: &Span) -> Result<Digest<G>, Error> {
        let mut total = Digest::empty();
        let mut pending = vec![from];

        while let Some(node) = pending.pop() {
            let subtree = node.prefix.span();
            let Some(cut) = subtree.intersect(region) else { continue };

            if subtree.is_subset_of(region) {
                total.add(&node.digest)?;
                continue;
            }

            match self.children(&node.prefix).await? {
                Some(children) => pending.extend(children),
                None => {
                    for (_, leaf) in self.rows().leaves(&cut, ScanDirection::Forward, usize::MAX).await? {
                        total.add_element(&leaf.element)?;
                    }
                }
            }
        }

        Ok(total)
    }

    /// The smallest subtree that contains `span`: the deepest node whose
    /// subtree does.
    pub(super) async fn smallest_containing(&self, span: &Span) -> Result<Node<G>, Error> {
        let mut node = self.root().await?;
        while let Some([first, second]) = self.children(&node.prefix).await? {
            // A span that holds no address lies within both children; it
            // stays with their parent.
            node = match (span.is_subset_of(&first.prefix.span()), span.is_subset_of(&second.prefix.span())) {
                (true, false) => first,
                (false, true) => second,
                _ => break,
            };
        }

        Ok(node)
    }
}
