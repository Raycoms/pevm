use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Mutex,
    },
    thread,
};
use std::collections::HashMap;
use std::time::Instant;
use alloy_consensus::BlockHeader;
use crossbeam::channel;
use revm::primitives::{AccessListItem, TxEnv};
use rustc_hash::FxHashSet;

use crate::{FinishExecFlags, IncarnationStatus, Task, TxIdx, TxStatus, TxVersion};
use crate::IncarnationStatus::{Executed, ReadyToExecute};
use crate::scheduler::Scheduler;

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
    transactions_status: Vec<Mutex<TxStatus>>,
    // The list of dependent transactions to queue for execution when the
    // key transaction finished executing.
    transactions_dependents: Vec<FxHashSet<TxIdx>>,
    // The list of dependency transactions to wait for before
    // key transaction is executed.
    transactions_dependencies: Vec<FxHashSet<TxIdx>>,
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

    pub(crate) default_channel: (channel::Sender<TxIdx>, channel::Receiver<TxIdx>),

    pub(crate) priority_channel: (channel::Sender<TxIdx>, channel::Receiver<TxIdx>),
}

impl ChironScheduler {
    pub(crate) fn new(block_size: usize, txs: &Vec<TxEnv>) -> Self {

        let time = Instant::now();
        let default_channel = channel::unbounded();
        let priority_channel = channel::unbounded();

        let mut res_map : HashMap<&AccessListItem, FxHashSet<TxIdx>> = HashMap::with_capacity(txs.len());
        let mut parents: Vec<FxHashSet<TxIdx>> = Vec::with_capacity(txs.len());
        let mut children: Vec<FxHashSet<TxIdx>> = Vec::with_capacity(txs.len());
        for _ in 0..block_size
        {
            parents.push(FxHashSet::default());
            children.push(FxHashSet::default());
        }
        for (idx, tx) in txs.iter().enumerate() {
            let parent_set = parents.get_mut(idx).unwrap();
            for hint in &tx.access_list {
                let entry = res_map.entry(hint).or_insert_with(FxHashSet::default);
                for &parent in entry.iter() {
                    children.get_mut(parent).unwrap().insert(idx);
                    parent_set.insert(parent);
                }

                entry.insert(idx);
            }
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

        let passed = time.elapsed().as_millis();
        // 3ms for 1000

        Self {
            block_size,
            transactions_status: (0..block_size)
                .map(|_| {
                    Mutex::new(TxStatus {
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
            let mut tx = index_mutex!(self.transactions_status, tx_idx);
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
                if let Some(tx_version) = self.try_execute(tx_id)
                {
                    return Some(Task::Execution(tx_version));
                }
            }

            // Check in default channel for a task.
            if let Ok(tx_id) = self.default_channel.1.try_recv() {
                // Prioritize execution task
                if let Some(tx_version) = self.try_execute(tx_id)
                {
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
            if self.transactions_status.get(validation_idx).unwrap().lock().unwrap().status == Executed {
                if validation_idx < self.block_size {
                    let mut tx = index_mutex!(self.transactions_status, validation_idx);
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
        let blocking_tx = index_mutex!(self.transactions_status, blocking_tx_idx);
        if matches!(
            blocking_tx.status,
            IncarnationStatus::Executed | IncarnationStatus::Validated
        ) {
            return false;
        }

        let mut tx = index_mutex!(self.transactions_status, tx_idx);
        debug_assert_eq!(tx.status, IncarnationStatus::Executing);
        tx.status = IncarnationStatus::Aborting;

        //let mut blocking_dependents = index_mutex!(self.transactions_dependents, blocking_tx_idx);
        //blocking_dependents.push(tx_idx);
        panic!("This should never happen. Transaction detected as dependent");
        true
    }

    fn set_ready_status(&self, tx_idx: TxIdx) {
        let mut tx = index_mutex!(self.transactions_status, tx_idx);
        debug_assert_eq!(tx.status, IncarnationStatus::Aborting);
        tx.status = IncarnationStatus::ReadyToExecute;
        tx.incarnation += 1;
    }

    fn finish_execution(
        &self,
        tx_version: TxVersion,
        flags: FinishExecFlags,
    ) -> Option<Task> {
        let mut tx = index_mutex!(self.transactions_status, tx_version.tx_idx);
        debug_assert_eq!(tx.status, IncarnationStatus::Executing);
        debug_assert_eq!(tx.incarnation, tx_version.tx_incarnation);

        // Resume dependent transactions

        let mut followup_tx = 0;
        // check through transactions, and put into queue the transaction with no more parents missing to execute.
        for dependent in self.transactions_dependents.get(tx_version.tx_idx).unwrap().iter() {
            if self.transactions_status.get(*dependent).unwrap().lock().unwrap().status == ReadyToExecute {
                for dependency in self.transactions_dependencies.get(*dependent).unwrap().iter() {
                    if *dependency != tx_version.tx_idx && self.transactions_status.get(*dependency).unwrap().lock().unwrap().status != Executed {
                        break;
                    }
                }
                if self.transactions_dependents.get(*dependent).unwrap().is_empty() {
                    self.default_channel.0.send(*dependent).unwrap()
                } else if followup_tx == 0 {
                    followup_tx = *dependent;
                } else {
                    self.priority_channel.0.send(*dependent).unwrap()
                }
            }
        }

        if flags.contains(FinishExecFlags::NeedValidation) {
            tx.status = IncarnationStatus::Executed;
        } else {
            tx.status = IncarnationStatus::Validated;
            self.num_validated.fetch_add(1, Ordering::Relaxed);
        }

        if followup_tx != 0 {
            let version = self.try_execute(followup_tx);
            if let Some(version) = version {
                return Some(Task::Execution(version));
            }
        }
        None
    }

    // Return whether the abort was successful. A successful abort leads to
    // scheduling the transaction for re-execution and the higher transactions
    // for validation during [finish_validation]. The scheduler ensures that only
    // one failing validation per version can lead to a successful abort.
    fn try_validation_abort(&self, tx_version: &TxVersion) -> bool {
        let mut tx = index_mutex!(self.transactions_status, tx_version.tx_idx);
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
            let mut tx = index_mutex!(self.transactions_status, tx_version.tx_idx);
            if tx.status == IncarnationStatus::Executed {
                tx.status = IncarnationStatus::Validated;
                self.num_validated.fetch_add(1, Ordering::Relaxed);
            }
        }
        None
    }
}
