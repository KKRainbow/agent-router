use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

use tokio::sync::{Mutex, mpsc};

#[derive(Debug)]
pub(crate) struct SessionWorkQueue<M> {
    capacity: usize,
    idle_timeout: Duration,
    workers: Arc<Mutex<HashMap<String, mpsc::Sender<M>>>>,
}

impl<M> Clone for SessionWorkQueue<M> {
    fn clone(&self) -> Self {
        Self {
            capacity: self.capacity,
            idle_timeout: self.idle_timeout,
            workers: self.workers.clone(),
        }
    }
}

impl<M> SessionWorkQueue<M>
where
    M: Send + 'static,
{
    pub(crate) fn new(capacity: usize, idle_timeout: Duration) -> Self {
        Self {
            capacity,
            idle_timeout,
            workers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) async fn enqueue<H, Fut>(
        &self,
        session_key: String,
        mut message: M,
        handler: H,
    ) -> EnqueueResult
    where
        H: Fn(M) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut workers = self.workers.lock().await;
        loop {
            if let Some(sender) = workers.get(&session_key) {
                match sender.try_send(message) {
                    Ok(()) => return EnqueueResult::Queued,
                    Err(mpsc::error::TrySendError::Closed(returned)) => {
                        message = returned;
                        workers.remove(&session_key);
                        continue;
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        return EnqueueResult::Full {
                            capacity: self.capacity,
                        };
                    }
                }
            }

            let (sender, receiver) = mpsc::channel(self.capacity);
            workers.insert(session_key.clone(), sender);
            self.clone()
                .spawn_worker(session_key.clone(), receiver, handler.clone());
        }
    }

    fn spawn_worker<H, Fut>(self, session_key: String, mut receiver: mpsc::Receiver<M>, handler: H)
    where
        H: Fn(M) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(async move {
            loop {
                match tokio::time::timeout(self.idle_timeout, receiver.recv()).await {
                    Ok(Some(message)) => handler(message).await,
                    Ok(None) => break,
                    Err(_) => {
                        let mut workers = self.workers.lock().await;
                        match receiver.try_recv() {
                            Ok(message) => {
                                drop(workers);
                                handler(message).await;
                            }
                            Err(mpsc::error::TryRecvError::Empty) => {
                                workers.remove(&session_key);
                                break;
                            }
                            Err(mpsc::error::TryRecvError::Disconnected) => {
                                workers.remove(&session_key);
                                break;
                            }
                        }
                    }
                }
            }
        });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EnqueueResult {
    Queued,
    Full { capacity: usize },
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::{
        sync::Mutex,
        time::{Duration, sleep, timeout},
    };

    use super::*;

    #[tokio::test]
    async fn processes_one_session_in_order() {
        let queue = SessionWorkQueue::new(8, Duration::from_secs(1));
        let seen = Arc::new(Mutex::new(Vec::new()));

        for item in [1, 2, 3] {
            let seen = seen.clone();
            assert_eq!(
                queue
                    .enqueue("qq:group:g1".to_string(), item, move |message| {
                        let seen = seen.clone();
                        async move {
                            seen.lock().await.push(message);
                        }
                    })
                    .await,
                EnqueueResult::Queued
            );
        }

        wait_for_len(&seen, 3).await;
        assert_eq!(*seen.lock().await, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn idle_worker_can_be_recreated_without_losing_messages() {
        let queue = SessionWorkQueue::new(8, Duration::from_millis(20));
        let seen = Arc::new(Mutex::new(Vec::new()));

        enqueue_recording(&queue, "qq:c2c:u1", 1, seen.clone()).await;
        wait_for_len(&seen, 1).await;
        sleep(Duration::from_millis(60)).await;
        enqueue_recording(&queue, "qq:c2c:u1", 2, seen.clone()).await;
        wait_for_len(&seen, 2).await;

        assert_eq!(*seen.lock().await, vec![1, 2]);
    }

    async fn enqueue_recording(
        queue: &SessionWorkQueue<i32>,
        session_key: &str,
        item: i32,
        seen: Arc<Mutex<Vec<i32>>>,
    ) {
        assert_eq!(
            queue
                .enqueue(session_key.to_string(), item, move |message| {
                    let seen = seen.clone();
                    async move {
                        seen.lock().await.push(message);
                    }
                })
                .await,
            EnqueueResult::Queued
        );
    }

    async fn wait_for_len(seen: &Arc<Mutex<Vec<i32>>>, len: usize) {
        timeout(Duration::from_secs(1), async {
            loop {
                if seen.lock().await.len() >= len {
                    return;
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
}
