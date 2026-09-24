/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

//! `Send`-safe variant of the operation queue, used when the `send` feature
//! is enabled. See the crate's top-level documentation.

use std::{
    cell::RefCell,
    fmt::Debug,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use async_channel::{Receiver, Sender};

use crate::{error::Error, runner_state::RunnerState};

/// An operation that can be added to an [`OperationQueue`]. `Send`-safe
/// counterpart to the non-`send`-feature `QueuedOperation`, for use with e.g.
/// `tokio::spawn` on a multi-threaded runtime.
pub trait QueuedOperation: Debug + Send {
    fn perform(&self) -> impl Future<Output = ()> + Send;
}

/// A dyn-compatible version of [`QueuedOperation`], implemented for all types
/// that implement it. See `local_thread::ErasedQueuedOperation` for why this
/// is necessary.
pub trait ErasedQueuedOperation: Debug + Send {
    fn perform<'op>(&'op self) -> Pin<Box<dyn Future<Output = ()> + Send + 'op>>;
}

impl<T> ErasedQueuedOperation for T
where
    T: QueuedOperation,
{
    fn perform<'op>(&'op self) -> Pin<Box<dyn Future<Output = ()> + Send + 'op>> {
        Box::pin(QueuedOperation::perform(self))
    }
}

/// A queue that performs asynchronous operations in order. `Send`-safe
/// counterpart to the non-`send`-feature `OperationQueue`.
pub struct OperationQueue {
    channel_sender: Sender<Box<dyn ErasedQueuedOperation>>,
    channel_receiver: Receiver<Box<dyn ErasedQueuedOperation>>,
    runners: RefCell<Vec<Arc<Runner>>>,
    spawn_task: fn(fut: Pin<Box<dyn Future<Output = ()> + Send>>),
}

impl OperationQueue {
    /// Creates a new operation queue. `spawn_task` spawns new runners, e.g.
    /// `tokio::spawn`, and must not be blocking.
    pub fn new(spawn_task: fn(fut: Pin<Box<dyn Future<Output = ()> + Send>>)) -> OperationQueue {
        let (snd, rcv) = async_channel::unbounded();

        OperationQueue {
            channel_sender: snd,
            channel_receiver: rcv,
            runners: RefCell::new(Vec::new()),
            spawn_task,
        }
    }

    /// Starts `runners` runners that consume items pushed to the queue.
    /// Errors if the queue has previously been stopped.
    pub fn start(&self, runners: u32) -> Result<(), Error> {
        if self.channel_sender.is_closed() {
            return Err(Error::Stopped);
        }

        for i in 0..runners {
            let runner = Runner::new(i, self.channel_receiver.clone());
            (self.spawn_task)(Box::pin(runner.clone().run()));
            self.runners.borrow_mut().push(runner);
        }

        Ok(())
    }

    /// Pushes an operation to the back of the queue. Errors if the queue has
    /// been stopped.
    pub async fn enqueue(&self, op: Box<dyn ErasedQueuedOperation>) -> Result<(), Error> {
        self.channel_sender.send(op).await?;
        Ok(())
    }

    /// Stops the queue. Already-queued operations still run, but subsequent
    /// [`start`](OperationQueue::start)/[`enqueue`](OperationQueue::enqueue)
    /// calls will fail.
    pub async fn stop(&self) {
        if !self.channel_sender.close() {
            log::warn!("request queue: attempted to close channel that's already closed");
        }

        self.runners.borrow_mut().clear();
    }

    /// Checks whether one or more runners are currently active (any state
    /// other than fully stopped, including pending).
    pub fn running(&self) -> bool {
        let active_runners =
            self.count_matching_runners(|runner| !matches!(runner.state(), RunnerState::Stopped));

        log::debug!("{active_runners} runner(s) currently active");

        active_runners > 0
    }

    /// Checks whether all runners are currently waiting for an operation.
    pub fn idle(&self) -> bool {
        let idle_runners =
            self.count_matching_runners(|runner| matches!(runner.state(), RunnerState::Waiting));

        log::debug!("{idle_runners} runner(s) currently idle");

        idle_runners == self.runners.borrow().len()
    }

    /// Counts runners matching `predicate`. Panics if `self.runners` is
    /// currently mutably borrowed.
    fn count_matching_runners<PredicateT>(&self, predicate: PredicateT) -> usize
    where
        PredicateT: FnMut(&&Arc<Runner>) -> bool,
    {
        self.runners.borrow().iter().filter(predicate).count()
    }
}

/// A runner created and run by the [`OperationQueue`]. Runs an infinite loop
/// via [`Runner::run`] until the queue's channel is closed and drained.
struct Runner {
    receiver: Receiver<Box<dyn ErasedQueuedOperation>>,

    // `Mutex` rather than a `Cell`: `Arc<Runner>` is only `Send` if `Runner`
    // is `Sync` too.
    state: Mutex<RunnerState>,

    // Used for debugging.
    id: u32,
}

impl Runner {
    /// Creates a new [`Runner`], wrapped in an [`Arc`] since [`Runner::run`]
    /// requires it.
    fn new(id: u32, receiver: Receiver<Box<dyn ErasedQueuedOperation>>) -> Arc<Runner> {
        Arc::new(Runner {
            id,
            receiver,
            state: Mutex::new(RunnerState::Pending),
        })
    }

    /// Waits for and performs operations as they come down the channel.
    async fn run(self: Arc<Runner>) {
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
                "operation_queue::Runner: runner {} performing op: {op:?}",
                self.id
            );

            op.perform().await;
        }
    }

    /// Gets the runner's current state.
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
    fn operation_queue_is_send() {
        assert_send::<OperationQueue>();
    }

    fn new_queue() -> OperationQueue {
        OperationQueue::new(|fut| {
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
        impl QueuedOperation for Operation {
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
        impl QueuedOperation for Operation {
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
