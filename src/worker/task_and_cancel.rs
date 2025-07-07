use std::future::Future;

use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::utils::PanicHelper;

pub struct TaskAndCancel<R> {
    inner: Option<Inner<R>>,
}

impl<R: Send + 'static> TaskAndCancel<R> {
    pub fn spawn<O: Future<Output = R> + Send + 'static, F: FnOnce(CancellationToken) -> O>(fut_builder: F) -> Self {
        let cancel = CancellationToken::new();
        let handle = tokio::spawn(fut_builder(cancel.clone()));

        TaskAndCancel {
            inner: Some(Inner {
                handle,
                cancel,
            }),
        }
    }

    pub async fn cancel(mut self) -> Result<R, JoinError> {
        let inner = self.inner.take().panic("inner dropped");
        inner.cancel.cancel();
        inner.handle.await
    }
}

impl<R> Drop for TaskAndCancel<R> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.cancel.cancel();
        }
    }
}

struct Inner<R> {
    handle: JoinHandle<R>,
    cancel: CancellationToken,
}

