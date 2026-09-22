/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

//! `Send`-safe variant of the operation queue, gated behind the `send`
//! feature.

use std::{
    cell::RefCell,
    fmt::Debug,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use async_channel::{Receiver, Sender};

use crate::{error::Error, operation_queue::RunnerState};

/// `Send`-safe counterpart to [`QueuedOperation`], kept as a separate trait so
/// existing non-`Send` implementations still compile under the `send`
/// feature.
///
/// [`QueuedOperation`]: crate::QueuedOperation
pub trait SendQueuedOperation: Debug + Send {
    fn perform(&self) -> impl Future<Output = ()> + Send;
}

/// Dyn-compatible version of [`SendQueuedOperation`]. See
/// [`ErasedQueuedOperation`] for why this is necessary.
///
/// [`ErasedQueuedOperation`]: crate::ErasedQueuedOperation
pub trait ErasedSendQueuedOperation: Debug + Send {
    fn perform<'op>(&'op self) -> Pin<Box<dyn Future<Output = ()> + Send + 'op>>;
}

impl<T> ErasedSendQueuedOperation for T
where
    T: SendQueuedOperation,
{
    fn perform<'op>(&'op self) -> Pin<Box<dyn Future<Output = ()> + Send + 'op>> {
        Box::pin(SendQueuedOperation::perform(self))
    }
}

/// `Send`-safe counterpart to [`OperationQueue`]; its runners' futures are
/// `Send`, so they can be spawned with e.g. `tokio::spawn` on tokio's
/// multi-threaded runtime. Takes [`SendQueuedOperation`] instead of
/// [`QueuedOperation`].
///
/// [`OperationQueue`]: crate::OperationQueue
/// [`QueuedOperation`]: crate::QueuedOperation
pub struct SendOperationQueue {
    channel_sender: Sender<Box<dyn ErasedSendQueuedOperation>>,
    channel_receiver: Receiver<Box<dyn ErasedSendQueuedOperation>>,
    runners: RefCell<Vec<Arc<SendRunner>>>,
    spawn_task: fn(fut: Pin<Box<dyn Future<Output = ()> + Send>>),
}

impl SendOperationQueue {
    /// See [`OperationQueue::new`].
    ///
    /// [`OperationQueue::new`]: crate::OperationQueue::new
    pub fn new(
        spawn_task: fn(fut: Pin<Box<dyn Future<Output = ()> + Send>>),
    ) -> SendOperationQueue {
        let (snd, rcv) = async_channel::unbounded();

        SendOperationQueue {
            channel_sender: snd,
            channel_receiver: rcv,
            runners: RefCell::new(Vec::new()),
            spawn_task,
        }
    }

    /// See [`OperationQueue::start`].
    ///
    /// [`OperationQueue::start`]: crate::OperationQueue::start
    pub fn start(&self, runners: u32) -> Result<(), Error> {
        if self.channel_sender.is_closed() {
            return Err(Error::Stopped);
        }

        for i in 0..runners {
            let runner = SendRunner::new(i, self.channel_receiver.clone());
            (self.spawn_task)(Box::pin(runner.clone().run()));
            self.runners.borrow_mut().push(runner);
        }

        Ok(())
    }

    /// See [`OperationQueue::enqueue`].
    ///
    /// [`OperationQueue::enqueue`]: crate::OperationQueue::enqueue
    pub async fn enqueue(&self, op: Box<dyn ErasedSendQueuedOperation>) -> Result<(), Error> {
        self.channel_sender.send(op).await?;
        Ok(())
    }

    /// See [`OperationQueue::stop`].
    ///
    /// [`OperationQueue::stop`]: crate::OperationQueue::stop
    pub async fn stop(&self) {
        if !self.channel_sender.close() {
            log::warn!("request queue: attempted to close channel that's already closed");
        }

        self.runners.borrow_mut().clear();
    }

    /// See [`OperationQueue::running`].
    ///
    /// [`OperationQueue::running`]: crate::OperationQueue::running
    pub fn running(&self) -> bool {
        let active_runners =
            self.count_matching_runners(|runner| !matches!(runner.state(), RunnerState::Stopped));

        log::debug!("{active_runners} runner(s) currently active");

        active_runners > 0
    }

    /// See [`OperationQueue::idle`].
    ///
    /// [`OperationQueue::idle`]: crate::OperationQueue::idle
    pub fn idle(&self) -> bool {
        let idle_runners =
            self.count_matching_runners(|runner| matches!(runner.state(), RunnerState::Waiting));

        log::debug!("{idle_runners} runner(s) currently idle");

        idle_runners == self.runners.borrow().len()
    }

    fn count_matching_runners<PredicateT>(&self, predicate: PredicateT) -> usize
    where
        PredicateT: FnMut(&&Arc<SendRunner>) -> bool,
    {
        self.runners.borrow().iter().filter(predicate).count()
    }
}

/// See [`Runner`].
///
/// [`Runner`]: crate::operation_queue::Runner
struct SendRunner {
    receiver: Receiver<Box<dyn ErasedSendQueuedOperation>>,

    // `Mutex` rather than `Runner`'s `Cell`, so `SendRunner` is `Sync`:
    // `Arc<SendRunner>` is only `Send` if `SendRunner` is `Send` + `Sync`.
    state: Mutex<RunnerState>,

    id: u32,
}

impl SendRunner {
    fn new(id: u32, receiver: Receiver<Box<dyn ErasedSendQueuedOperation>>) -> Arc<SendRunner> {
        Arc::new(SendRunner {
            id,
            receiver,
            state: Mutex::new(RunnerState::Pending),
        })
    }

    async fn run(self: Arc<SendRunner>) {
        loop {
            *self.state.lock().expect("runner state lock poisoned") = RunnerState::Waiting;

            let op = match self.receiver.recv().await {
                Ok(op) => op,
                Err(_) => {
                    log::info!(
                        "request queue: channel has closed (likely due to client shutdown), exiting the loop"
                    );
                    *self.state.lock().expect("runner state lock poisoned") = RunnerState::Stopped;
                    return;
                }
            };

            *self.state.lock().expect("runner state lock poisoned") = RunnerState::Running;

            log::info!(
                "operation_queue::SendRunner: runner {} performing op: {op:?}",
                self.id
            );

            op.perform().await;
        }
    }

    fn state(&self) -> RunnerState {
        *self.state.lock().expect("runner state lock poisoned")
    }
}

#[cfg(test)]
#[allow(unexpected_cfgs)]
mod tests {
    use super::*;

    use async_channel::Sender;
    use tokio::time::Duration;

    fn assert_send<T: Send>() {}

    #[test]
    fn send_operation_queue_is_send() {
        assert_send::<SendOperationQueue>();
    }

    fn new_queue() -> SendOperationQueue {
        SendOperationQueue::new(|fut| {
            _ = tokio::spawn(fut);
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn start_queue() {
        let queue = new_queue();

        queue.start(5).unwrap();
        assert_eq!(queue.runners.borrow().len(), 5);

        tokio::time::sleep(Duration::from_millis(0)).await;
        assert!(queue.idle());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stop_queue() {
        let queue = new_queue();

        queue.start(5).unwrap();

        tokio::time::sleep(Duration::from_millis(0)).await;
        assert!(queue.idle());

        queue.stop().await;
        assert!(!queue.running());
        assert!(queue.channel_receiver.is_closed());

        match queue.start(1) {
            Ok(_) => panic!("we should not be able to start the queue after stopping it"),
            Err(Error::Stopped) => (),
            Err(_) => panic!("unexpected error"),
        }

        #[derive(Debug)]
        struct Operation {}
        impl SendQueuedOperation for Operation {
            async fn perform(&self) {}
        }

        let op = Box::new(Operation {});
        match queue.enqueue(op).await {
            Ok(_) => panic!("we should not be able to enqueue operations after stopping the queue"),
            Err(Error::Sender) => (),
            Err(_) => panic!("unexpected error"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn operation_order_across_threads() {
        #[derive(Debug)]
        struct Operation {
            id: u8,
            sender: Sender<u8>,
        }
        impl SendQueuedOperation for Operation {
            async fn perform(&self) {
                self.sender.send(self.id).await.unwrap();
            }
        }

        let queue = new_queue();

        let (sender, receiver) = async_channel::unbounded();

        queue
            .enqueue(Box::new(Operation {
                id: 1,
                sender: sender.clone(),
            }))
            .await
            .unwrap();

        queue
            .enqueue(Box::new(Operation {
                id: 2,
                sender: sender.clone(),
            }))
            .await
            .unwrap();

        queue.start(1).unwrap();

        let id = receiver.recv().await.unwrap();
        assert_eq!(id, 1);
        let id = receiver.recv().await.unwrap();
        assert_eq!(id, 2);

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(queue.idle());
    }
}
