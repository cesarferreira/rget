//! Cooperative cancellation (PRD §26).
//!
//! Small enough not to justify a dependency: a flag plus a notifier. The
//! important property is that `cancel()` is observable both by a poll
//! (`is_cancelled`) on the hot write path and by an await
//! (`cancelled().await`) inside a `select!`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;

#[derive(Clone, Default)]
pub struct Cancel {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    flag: AtomicBool,
    notify: Notify,
}

impl Cancel {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.inner.flag.store(true, Ordering::SeqCst);
        self.inner.notify.notify_waiters();
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.flag.load(Ordering::SeqCst)
    }

    /// Resolves as soon as cancellation has been requested, including when it
    /// was requested before this call.
    pub async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        loop {
            let notified = self.inner.notify.notified();
            if self.is_cancelled() {
                return;
            }
            notified.await;
            if self.is_cancelled() {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resolves_when_cancelled_later() {
        let c = Cancel::new();
        let c2 = c.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            c2.cancel();
        });
        c.cancelled().await;
        assert!(c.is_cancelled());
    }

    #[tokio::test]
    async fn resolves_immediately_if_already_cancelled() {
        let c = Cancel::new();
        c.cancel();
        // Would hang if `cancelled()` only watched for future notifications.
        tokio::time::timeout(std::time::Duration::from_millis(50), c.cancelled())
            .await
            .expect("should resolve immediately");
    }

    #[tokio::test]
    async fn clones_share_state() {
        let a = Cancel::new();
        let b = a.clone();
        a.cancel();
        assert!(b.is_cancelled());
    }
}
