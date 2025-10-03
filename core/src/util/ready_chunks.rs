use core::pin::Pin;
use core::task::{Context, Poll};
use futures::future::Future;
use futures::stream::{FuturesUnordered, Stream, StreamExt};

/// Yields a Vec of all items that are ready at the time of a wake.
/// Internally uses FuturesUnordered for readiness and fairness.
pub struct ReadyChunks<F: Future> {
    inner: FuturesUnordered<Pin<Box<F>>>,
}

impl<F: Future> ReadyChunks<F> {
    pub fn new<I>(futures: I) -> Self
    where I: IntoIterator<Item = F> {
        let inner = futures.into_iter().map(|f| Box::pin(f)).collect::<FuturesUnordered<_>>();
        Self { inner }
    }

    pub fn is_empty(&self) -> bool { self.inner.is_empty() }
    pub fn len(&self) -> usize { self.inner.len() }
}

impl<F: Future> Stream for ReadyChunks<F> {
    type Item = Vec<F::Output>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut batch = Vec::new();

        // TODO - it's not obvious to me that FuturesUnordered is actually the optimal way to do this
        // wait for at least one item so we don't spin
        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(item)) => batch.push(item),
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => return Poll::Pending,
        }

        // drain all currently-ready items
        loop {
            match self.inner.poll_next_unpin(cx) {
                Poll::Ready(Some(item)) => batch.push(item),
                Poll::Ready(None) | Poll::Pending => break,
            }
        }

        Poll::Ready(Some(batch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::channel::oneshot;
    use futures::stream::StreamExt;

    #[tokio::test(flavor = "current_thread")]
    async fn drains_all_simultaneously_ready() {
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();
        let (tx3, rx3) = oneshot::channel();

        let mut stream = ReadyChunks::new(vec![rx1, rx2, rx3]);

        tx1.send(1).unwrap();
        tx2.send(2).unwrap();

        let chunk = stream.next().await.unwrap();
        let mut values: Vec<_> = chunk.into_iter().map(|r| r.unwrap()).collect();
        values.sort_unstable();
        assert_eq!(values, vec![1, 2]);

        tx3.send(3).unwrap();
        let chunk2 = stream.next().await.unwrap();
        let values2: Vec<_> = chunk2.into_iter().map(|r| r.unwrap()).collect();
        assert_eq!(values2, vec![3]);

        assert!(stream.next().await.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn yields_pending_until_first_ready_then_drains() {
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        let mut stream = ReadyChunks::new(vec![rx1, rx2]);

        // Make first ready earlier than the second
        tokio::spawn(async move {
            tokio::time::sleep(core::time::Duration::from_millis(10)).await;
            let _ = tx1.send(10);
        });
        tokio::spawn(async move {
            tokio::time::sleep(core::time::Duration::from_millis(30)).await;
            let _ = tx2.send(20);
        });

        let first = stream.next().await.unwrap();
        let values: Vec<_> = first.into_iter().map(|r| r.unwrap()).collect();
        assert_eq!(values, vec![10]);

        let second = stream.next().await.unwrap();
        let values2: Vec<_> = second.into_iter().map(|r| r.unwrap()).collect();
        assert_eq!(values2, vec![20]);

        assert!(stream.next().await.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn empty_stream_yields_none() {
        let futs: Vec<oneshot::Receiver<i32>> = Vec::new();
        let mut stream = ReadyChunks::new(futs);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn includes_cancellations_in_chunk() {
        let (tx, rx) = oneshot::channel::<i32>();
        drop(tx);

        let mut stream = ReadyChunks::new(vec![rx]);
        let chunk = stream.next().await.unwrap();
        assert_eq!(chunk.len(), 1);
        assert!(chunk[0].is_err());
        assert!(stream.next().await.is_none());
    }
}
