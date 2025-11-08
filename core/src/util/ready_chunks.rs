use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;
use futures::future::Future;
use futures::stream::{FuturesUnordered, Stream, StreamExt};
use futures_timer::Delay;

/// Yields a Vec of all items that are ready at the time of a wake.
/// After the first item is ready, waits up to `timeout` for additional items.
/// Internally uses FuturesUnordered for readiness and fairness.
pub struct ReadyChunks<F: Future> {
    inner: FuturesUnordered<Pin<Box<F>>>,
    timeout: Duration,
    timer: Option<Pin<Box<Delay>>>,
    batch: Vec<F::Output>,
}

impl<F: Future> ReadyChunks<F> {
    pub fn new<I>(futures: I, timeout: Duration) -> Self
    where I: IntoIterator<Item = F> {
        let inner = futures.into_iter().map(|f| Box::pin(f)).collect::<FuturesUnordered<_>>();
        Self { inner, timeout, timer: None, batch: Vec::new() }
    }

    pub fn is_empty(&self) -> bool { self.inner.is_empty() }
    pub fn len(&self) -> usize { self.inner.len() }
}

impl<F: Future> Stream for ReadyChunks<F>
where F::Output: Unpin
{
    type Item = Vec<F::Output>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // If we don't have a batch started, wait for first item
        if this.batch.is_empty() && this.timer.is_none() {
            match this.inner.poll_next_unpin(cx) {
                Poll::Ready(Some(item)) => {
                    this.batch.push(item);
                    // Start the timer after first item
                    this.timer = Some(Box::pin(Delay::new(this.timeout)));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }

        // We have a batch started, collect more items until timer expires
        loop {
            // Check if timer has expired
            if let Some(ref mut timer) = this.timer {
                match timer.as_mut().poll(cx) {
                    Poll::Ready(()) => {
                        // Timer expired, return what we have
                        this.timer = None;
                        let batch = core::mem::take(&mut this.batch);
                        return Poll::Ready(Some(batch));
                    }
                    Poll::Pending => {}
                }
            }

            // Try to get more ready items
            match this.inner.poll_next_unpin(cx) {
                Poll::Ready(Some(item)) => {
                    this.batch.push(item);
                    // Continue loop to check for more
                }
                Poll::Ready(None) => {
                    // All futures done, return batch
                    this.timer = None;
                    let batch = core::mem::take(&mut this.batch);
                    return if batch.is_empty() { Poll::Ready(None) } else { Poll::Ready(Some(batch)) };
                }
                Poll::Pending => {
                    // No more items ready right now
                    // If timer is running, wait for it or more items
                    if this.timer.is_some() {
                        return Poll::Pending;
                    } else {
                        // No timer means we already collected everything
                        let batch = core::mem::take(&mut this.batch);
                        return Poll::Ready(Some(batch));
                    }
                }
            }
        }
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

        let mut stream = ReadyChunks::new(vec![rx1, rx2, rx3], Duration::from_millis(10));

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

        let mut stream = ReadyChunks::new(vec![rx1, rx2], Duration::from_millis(10));

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
        let mut stream = ReadyChunks::new(futs, Duration::from_millis(10));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn includes_cancellations_in_chunk() {
        let (tx, rx) = oneshot::channel::<i32>();
        drop(tx);

        let mut stream = ReadyChunks::new(vec![rx], Duration::from_millis(10));
        let chunk = stream.next().await.unwrap();
        assert_eq!(chunk.len(), 1);
        assert!(chunk[0].is_err());
        assert!(stream.next().await.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn batches_items_within_timeout_window() {
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();
        let (tx3, rx3) = oneshot::channel();

        let mut stream = ReadyChunks::new(vec![rx1, rx2, rx3], Duration::from_millis(15));

        // First arrives immediately
        tx1.send(1).unwrap();

        // Second arrives within timeout
        tokio::spawn(async move {
            tokio::time::sleep(core::time::Duration::from_millis(5)).await;
            let _ = tx2.send(2);
        });

        // Third arrives after timeout
        tokio::spawn(async move {
            tokio::time::sleep(core::time::Duration::from_millis(30)).await;
            let _ = tx3.send(3);
        });

        let chunk = stream.next().await.unwrap();
        let mut values: Vec<_> = chunk.into_iter().map(|r| r.unwrap()).collect();
        values.sort_unstable();
        assert_eq!(values, vec![1, 2]); // Should batch first two

        let chunk2 = stream.next().await.unwrap();
        let values2: Vec<_> = chunk2.into_iter().map(|r| r.unwrap()).collect();
        assert_eq!(values2, vec![3]); // Third comes separately

        assert!(stream.next().await.is_none());
    }
}
