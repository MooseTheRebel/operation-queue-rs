/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

//! Shared between `local_thread` and `multi_thread`, since it carries no
//! `Send`-related semantics of its own.

/// The status of a runner.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum RunnerState {
    /// The runner has been created but isn't running yet.
    Pending,

    /// The runner is currently waiting for an operation to perform.
    Waiting,

    /// The runner is currently performing an operation.
    Running,

    /// The runner has finished performing its last operation and has exited its
    /// main loop.
    Stopped,
}
