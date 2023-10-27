use std::{ops::Deref, sync::Arc};

use tokio::sync::{futures::Notified, Notify, Semaphore};
#[derive(Debug, Clone)]
pub(crate) struct WaitGroup(Arc<WaitGroupInner>);
#[derive(Debug)]
pub(crate) struct WaitGroupInner {
    sem: Semaphore,
    count: u32,
}
impl WaitGroupInner {
    pub(crate) fn done(&self) {
        self.sem.add_permits(1);
    }
    pub(crate) async fn wait(
        &self,
    ) -> Result<tokio::sync::SemaphorePermit<'_>, tokio::sync::AcquireError> {
        self.sem.acquire_many(self.count).await
    }
}
impl Deref for WaitGroup {
    type Target = WaitGroupInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl WaitGroup {
    pub(crate) fn new(count: u32) -> WaitGroup {
        let sem = Semaphore::new(0);
        Self(WaitGroupInner { sem, count }.into())
    }
}
#[derive(Debug, Clone)]
pub(crate) struct CloseNotify(Arc<Notify>);
impl CloseNotify {
    pub(crate) fn new() -> Self {
        let notify = Notify::new();
        Self(notify.into())
    }
    pub(crate) fn notify(&self) {
        self.0.notify_one();
    }
    pub(crate) fn notified(&self) -> Notified {
        self.0.notified()
    }
}
#[derive(Debug, Clone)]
pub(crate) struct Closer(Arc<CloserInner>);
#[derive(Debug)]
struct CloserInner {
    wait_group: WaitGroupInner,
    notify: Notify,
}
impl Closer {
    pub(crate) fn new(count: u32) -> Self {
        Self(
            CloserInner {
                wait_group: WaitGroupInner {
                    sem: Semaphore::new(0),
                    count,
                },
                notify: Notify::new(),
            }
            .into(),
        )
    }

    pub(crate) fn signal(&self) {
        self.0.notify.notify_one();
    }

    pub(crate) fn captured(&self) -> Notified<'_> {
        self.0.notify.notified()
    }
}
impl Deref for Closer {
    type Target = WaitGroupInner;

    fn deref(&self) -> &Self::Target {
        &self.0.wait_group
    }
}
#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use crate::util::closer::{CloseNotify, Closer, WaitGroup};

    #[tokio::test]
    async fn test_waitgroup() {
        let wait_group = WaitGroup::new(2);
        (0..3).for_each(|_| {
            let w = wait_group.clone();
            tokio::spawn(async move {
                w.done();
            });
        });
        let _ = wait_group.wait().await;
    }
    #[tokio::test]
    async fn test_notify() {
        let close_send = CloseNotify::new();
        let close_recv = close_send.clone();
        let sleep = 2;
        let start = SystemTime::now();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(sleep)).await;
            close_send.notify();
        });
        close_recv.notified().await;
        let p = SystemTime::now().duration_since(start).unwrap().as_secs();
        assert_eq!(sleep, p);
    }
    #[tokio::test]
    async fn test_closer() {
        let closer = Closer::new(1);
        let closer_c = closer.clone();
        let s = 1;
        let t = 2;
        let start = SystemTime::now();
        let handler = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            println!("signal");
            closer_c.signal();
            let start = SystemTime::now();
            let _ = closer_c.wait().await;
            let p = SystemTime::now().duration_since(start).unwrap().as_secs();
            assert_eq!(t, p);
            println!("recv sem");
        });
        closer.captured().await;
        println!("captured");
        let p = SystemTime::now().duration_since(start).unwrap().as_secs();
        assert_eq!(p, s);
        println!("send 'done' sem");
        tokio::time::sleep(Duration::from_secs(t)).await;
        closer.done();
        let _ = handler.await;
    }
}
