use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
};
use std::collections::hash_map::Entry;
use std::sync::RwLock;
use revm::primitives::{AccessListItem, TxEnv};
use rustc_hash::{FxBuildHasher, FxHashMap};
use crate::{FinishExecFlags, IncarnationStatus, Task, TxIdx, TxStatus, TxVersion};
use crate::IncarnationStatus::{Executed, ReadyToExecute, Validated};
use crate::scheduler::Scheduler;

// This optimization is desired as we constantly index into many
// vectors of the block-size size. It can yield up to 5% improvement.
macro_rules! mut_vec_access {
    ($vec:expr, $index:expr) => {
        // SAFETY: A correct scheduler would not leak indexes larger
        // than the block size, which is the size of all vectors we
        // index via this macro. Otherwise, DO NOT USE!
        unsafe { $vec.get_unchecked_mut($index) }
    };
}

macro_rules! vec_access {
    ($vec:expr, $index:expr) => {
        // SAFETY: A correct scheduler would not leak indexes larger
        // than the block size, which is the size of all vectors we
        // index via this macro. Otherwise, DO NOT USE!
        unsafe { $vec.get_unchecked($index) }
    };
}


// The Pevm collaborative scheduler coordinates execution & validation
// tasks among work threads.
//
// To pick a task, threads increment the smaller of the (execution and
// validation) task counters until they find a task that is ready to be
// performed. To redo a task for a transaction, the thread updates the status
// and reduces the corresponding counter to the transaction index if it had a
// larger value.
//
// An incarnation may write to a memory location that was previously
// read by a higher transaction. Thus, when an incarnation finishes, new
// validation tasks are created for higher transactions.
//
// Validation tasks are scheduled optimistically and in parallel. Identifying
// validation failures and aborting incarnations as soon as possible is critical
// for performance, as any incarnation that reads values written by an
// incarnation that aborts also must abort.
// When an incarnation writes only to a subset of memory locations written
// by the previously completed incarnation of the same transaction, we schedule
// validation just for the incarnation. This is sufficient as the whole write
// set of the previous incarnation is marked as ESTIMATE during the abort.
// The abort leads to optimistically creating validation tasks for higher
// transactions. Threads that perform these tasks can already detect validation
// failure due to the ESTIMATE markers on memory locations, instead of waiting
// for a subsequent incarnation to finish.
#[derive(Debug)]
pub(crate) struct ChironScheduler {
    // The number of transactions in this block.
    block_size: usize,
    // The most up-to-date incarnation number (initially 0) and
    // the status of this incarnation.
    // TODO: Consider packing [TxStatus]s into atomics instead of
    // [Mutex] given how small they are.
    transactions_status: Vec<RwLock<TxStatus>>,
    // The list of dependent transactions to queue for execution when the
    // key transaction finished executing.
    transactions_dependents: Vec<Vec<usize>>,
    // The list of dependency transactions to wait for before
    // key transaction is executed.
    transactions_dependencies: Vec<Vec<usize>>,
    // The next transaction to try and validate.
    validation_idx: AtomicUsize,
    // We won't validate until we find the first non-lazy transaction that
    // needs to read explicit values. We also skip the first transaction.
    min_validation_idx: AtomicUsize,
    // The number of validated transactions
    num_validated: AtomicUsize,
    // True if the scheduler has been aborted, likely due to fatal execution
    // errors.
    aborted: AtomicBool,

    pub(crate) default_channel: (flume::Sender<TxIdx>, flume::Receiver<TxIdx>),

    pub(crate) priority_channel: (flume::Sender<TxIdx>, flume::Receiver<TxIdx>),
}

impl ChironScheduler {
    pub(crate) fn new(block_size: usize, txs: &Vec<TxEnv>) -> Self {
        let default_channel = flume::unbounded();
        let priority_channel = flume::unbounded();

        let mut res_map : FxHashMap<&AccessListItem, TxIdx> = FxHashMap::with_capacity_and_hasher(txs.len(), FxBuildHasher::default());
        let mut parents: Vec<Vec<usize>> = Vec::with_capacity(txs.len());
        let mut children: Vec<Vec<usize>> = Vec::with_capacity(txs.len());
        for _ in 0..block_size
        {
            parents.push(Vec::default());
            children.push(Vec::default());
        }

        for (idx, tx) in txs.iter().enumerate() {
            let parent_set = mut_vec_access!(parents, idx);
            for hint in &tx.access_list {
                match res_map.entry(hint) {
                    Entry::Occupied(mut entry) => {
                        let parent = *entry.get();
                        parent_set.push(parent);

                        mut_vec_access!(children, parent).push(idx);

                        entry.insert(idx);
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(idx);
                    }
                }
            }
            //if slowest_parent != TxIdx::MAX {
            //    mut_vec_access!(children, slowest_parent).push(idx);
            //}
        }
        
        for idx in 0..block_size {
            let local_parents = parents.get(idx).unwrap();
            let local_children = children.get(idx).unwrap();
            if local_parents.is_empty() {
                if local_children.is_empty() {
                    default_channel.0.send(idx).unwrap();
                } else {
                    priority_channel.0.send(idx).unwrap();
                }
            }
        }

        // 1ms for 10k, 10ms for 50k
        Self {
            block_size,
            transactions_status: (0..block_size)
                .map(|_| {
                    RwLock::new(TxStatus {
                        incarnation: 0,
                        status: IncarnationStatus::ReadyToExecute,
                    })
                })
                .collect(),
            transactions_dependents: children,
            transactions_dependencies: parents,
            // We won't validate until we find the first non-lazy transaction that
            // needs to read explicit values. We also skip the first transaction.
            validation_idx: AtomicUsize::new(block_size),
            min_validation_idx: AtomicUsize::new(block_size),
            num_validated: AtomicUsize::new(0),
            aborted: AtomicBool::new(false),
            default_channel,
            priority_channel
        }
    }
}

// TODO: Better error handling.
// Like returning errors instead of panicking on [unreachable]s.
impl Scheduler for ChironScheduler {
    fn abort(&self) {
        self.aborted.store(true, Ordering::Relaxed);
    }

    fn try_execute(&self, tx_idx: TxIdx) -> Option<TxVersion> {
        if tx_idx < self.block_size {
            let mut tx = write_index_mutex!(self.transactions_status, tx_idx);
            if tx.status == IncarnationStatus::ReadyToExecute {
                tx.status = IncarnationStatus::Executing;
                return Some(TxVersion {
                    tx_idx,
                    tx_incarnation: tx.incarnation,
                });
            }
        }
        None
    }

    fn next_task(&self) -> Option<Task> {
        while !self.aborted.load(Ordering::Relaxed) {

            // Check in priority channel for a task.
            if let Ok(tx_id) = self.priority_channel.1.try_recv() {
                // Prioritize execution task
                if let Some(tx_version) = self.try_execute(tx_id) {
                    return Some(Task::Execution(tx_version));
                }
            }

            // Check in default channel for a task.
            if let Ok(tx_id) = self.default_channel.1.try_recv() {
                // Prioritize execution task
                if let Some(tx_version) = self.try_execute(tx_id) {
                    return Some(Task::Execution(tx_version));
                }
            }

            // Check if we finished and can stop execution.
            let validation_idx = self.validation_idx.load(Ordering::Relaxed);
            if validation_idx >= self.block_size {
                if self.num_validated.load(Ordering::Relaxed) >= self.block_size - self.min_validation_idx.load(Ordering::Relaxed)
                {
                    break;
                }
                thread::yield_now();
                continue;
            }

            //todo do we need re-execute?

            // Check if we can do validation.
            if read_index_mutex!(self.transactions_status, validation_idx).status == Executed {
                if validation_idx < self.block_size {
                    let mut tx = write_index_mutex!(self.transactions_status, validation_idx);
                    // "Steal" execution job while holding the lock
                    if tx.status == IncarnationStatus::ReadyToExecute {
                        tx.status = IncarnationStatus::Executing;
                        return Some(Task::Execution(TxVersion {
                            tx_idx: validation_idx,
                            tx_incarnation: tx.incarnation,
                        }));
                    }
                    // Start a typical validation task
                    if matches!(tx.status,IncarnationStatus::Executed | IncarnationStatus::Validated) {
                        return Some(Task::Validation(TxVersion {
                            tx_idx: validation_idx,
                            tx_incarnation: tx.incarnation,
                        }));
                    }
                    // Validation index is still catching up so continue a
                    // new loop iteration to refetch the latest indices
                    // before deciding again.
                    if tx.status == IncarnationStatus::Aborting {
                        continue;
                    }
                    // Fall back to execution job as this executing tx will
                    // decide if validation is needed when it's done. If it
                    // does, all validation tasks here would be redone anyway.
                }
            }
        }
        None
    }

    // Add [tx_idx] as a dependent of [blocking_tx_idx] so [tx_idx] is
    // re-executed when the next [blocking_tx_idx] incarnation is executed.
    // Return [false] if we encounter a race condition when [blocking_tx_idx]
    // gets re-executed before the dependency can be added.
    fn add_dependency(&self, tx_idx: TxIdx, blocking_tx_idx: TxIdx) -> bool {
        // This is an important lock to prevent a race condition where the blocking
        // transaction completes re-execution before this dependency can be added.
        let blocking_tx = read_index_mutex!(self.transactions_status, blocking_tx_idx);
        if matches!(
            blocking_tx.status,
            IncarnationStatus::Executed | IncarnationStatus::Validated
        ) {
            return false;
        }

        let mut tx = write_index_mutex!(self.transactions_status, tx_idx);
        debug_assert_eq!(tx.status, IncarnationStatus::Executing);
        tx.status = IncarnationStatus::Aborting;

        //let mut blocking_dependents = index_mutex!(self.transactions_dependents, blocking_tx_idx);
        //blocking_dependents.push(tx_idx);
        panic!("This should never happen. Transaction detected as dependent");
    }

    fn set_ready_status(&self, tx_idx: TxIdx) {
        let mut tx = write_index_mutex!(self.transactions_status, tx_idx);
        debug_assert_eq!(tx.status, IncarnationStatus::Aborting);
        tx.status = IncarnationStatus::ReadyToExecute;
        tx.incarnation += 1;
    }

    fn finish_execution(
        &self,
        tx_version: TxVersion,
        flags: FinishExecFlags,
    ) -> Option<Task> {
        let mut tx = write_index_mutex!(self.transactions_status, tx_version.tx_idx);
        debug_assert_eq!(tx.status, IncarnationStatus::Executing);
        debug_assert_eq!(tx.incarnation, tx_version.tx_incarnation);

        // Resume dependent transactions
        if flags.contains(FinishExecFlags::NeedValidation) {
            tx.status = IncarnationStatus::Executed;
        } else {
            tx.status = IncarnationStatus::Validated;
            self.num_validated.fetch_add(1, Ordering::Relaxed);
        }
        drop(tx);

        let mut followup_tx = 0;

        // For all children
        for &child in vec_access!(self.transactions_dependents, tx_version.tx_idx) {
            if read_index_mutex!(self.transactions_status, child).status == ReadyToExecute {
                let mut can_schedule = true;
                for &parent in vec_access!(self.transactions_dependencies, child) {
                    if parent != tx_version.tx_idx {
                        let status = &read_index_mutex!(self.transactions_status, parent).status;
                        if *status != Executed && *status != Validated {
                            can_schedule = false;
                            break;
                        }
                    }
                }

                if can_schedule {
                    if self.transactions_dependents.get(child).unwrap().is_empty() {
                        self.default_channel.0.send(child).unwrap();
                    } else if followup_tx == 0 {
                        followup_tx = child;
                    } else {
                        self.priority_channel.0.send(child).unwrap();
                    }
                }
            }
        }

        if followup_tx != 0 {
            let version = self.try_execute(followup_tx);
            if let Some(version) = version {
                return Some(Task::Execution(version));
            }
            self.priority_channel.0.send(followup_tx).unwrap();
        }
        None
    }

    // Return whether the abort was successful. A successful abort leads to
    // scheduling the transaction for re-execution and the higher transactions
    // for validation during [finish_validation]. The scheduler ensures that only
    // one failing validation per version can lead to a successful abort.
    fn try_validation_abort(&self, tx_version: &TxVersion) -> bool {
        let mut tx = write_index_mutex!(self.transactions_status, tx_version.tx_idx);
        if tx.status == IncarnationStatus::Validated {
            self.num_validated.fetch_sub(1, Ordering::Relaxed);
        }

        let aborting = matches!(
            tx.status,
            IncarnationStatus::Executed | IncarnationStatus::Validated
        );
        if aborting {
            tx.status = IncarnationStatus::Aborting;
        }
        aborting
    }

    // When there is a successful abort, schedule the transaction for re-execution
    // and the higher transactions for validation. The re-execution task is returned
    // for the aborted transaction.
    fn finish_validation(&self, tx_version: &TxVersion, aborted: bool) -> Option<Task> {
        if aborted {
            self.set_ready_status(tx_version.tx_idx);
            self.validation_idx.fetch_add(1, Ordering::Relaxed);
        } else {
            let mut tx = write_index_mutex!(self.transactions_status, tx_version.tx_idx);
            if tx.status == IncarnationStatus::Executed {
                tx.status = IncarnationStatus::Validated;
                self.num_validated.fetch_add(1, Ordering::Relaxed);
            }
        }
        None
    }

    fn inc_exec(&self) {
        //noop
    }
}
