use std::{future::Future, pin::Pin, time::Duration};

pub(super) const UPDATE_TICK: Duration = Duration::from_millis(50);
pub(super) const MAX_BATCH_DRAIN: usize = 128;

pub(super) trait UpdateWorker {
    type Item;
    type TryRecvError;

    fn try_recv_update_item(&mut self) -> Result<Self::Item, Self::TryRecvError>;

    fn apply_update_item<'a>(
        &'a mut self,
        item: Self::Item,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>
    where
        Self::Item: 'a;

    fn update_ready(&mut self) -> bool;

    fn update(&mut self);
}

pub(super) async fn process_batch<W: UpdateWorker>(worker: &mut W, first: W::Item) {
    worker.apply_update_item(first).await;

    for _ in 0..MAX_BATCH_DRAIN {
        let Ok(item) = worker.try_recv_update_item() else {
            break;
        };
        worker.apply_update_item(item).await;
    }

    update_if_ready(worker);
}

pub(super) async fn drain_pending<W: UpdateWorker>(worker: &mut W) {
    while let Ok(item) = worker.try_recv_update_item() {
        worker.apply_update_item(item).await;
    }

    update_if_ready(worker);
}

pub(super) fn update_if_ready<W: UpdateWorker>(worker: &mut W) {
    if worker.update_ready() {
        worker.update();
    }
}
