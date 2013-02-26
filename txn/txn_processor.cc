// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Christina Wallin (christina.wallin@yale.edu)

#include "txn/txn_processor.h"
#include <stdio.h>

#include "txn/lock_manager.h"

// Number of finished transactions to deal with in each pass through parallel
// OCC
#define N_FINISHED_TXNS (100)

// Thread & queue counts for StaticThreadPool initialization.
#define THREAD_COUNT 100
#define QUEUE_COUNT 10

TxnProcessor::TxnProcessor(CCMode mode)
    : mode_(mode), tp_(THREAD_COUNT, QUEUE_COUNT), next_unique_id_(1) {
  if (mode_ == LOCKING_EXCLUSIVE_ONLY)
    lm_ = new LockManagerA(&ready_txns_);
  else if (mode_ == LOCKING)
    lm_ = new LockManagerB(&ready_txns_);

  // Start 'RunScheduler()' running as a new task in its own thread.
  tp_.RunTask(
        new Method<TxnProcessor, void>(this, &TxnProcessor::RunScheduler));
}

TxnProcessor::~TxnProcessor() {
  if (mode_ == LOCKING_EXCLUSIVE_ONLY || mode_ == LOCKING)
    delete lm_;
}

void TxnProcessor::NewTxnRequest(Txn* txn) {
  // Atomically assign the txn a new number and add it to the incoming txn
  // requests queue.
  mutex_.Lock();
  txn->unique_id_ = next_unique_id_;
  next_unique_id_++;
  txn_requests_.Push(txn);
  mutex_.Unlock();
}

Txn* TxnProcessor::GetTxnResult() {
  Txn* txn;
  while (!txn_results_.Pop(&txn)) {
    // No result yet. Wait a bit before trying again (to reduce contention on
    // atomic queues).
    sleep(0.000001);
  }
  return txn;
}

void TxnProcessor::RunScheduler() {
  switch (mode_) {
    case SERIAL:                 RunSerialScheduler();
    case LOCKING:                RunLockingScheduler();
    case LOCKING_EXCLUSIVE_ONLY: RunLockingScheduler();
    case OCC:                    RunOCCScheduler();
    case P_OCC:                  RunOCCParallelScheduler();
  }
}

void TxnProcessor::RunSerialScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    // Get next txn request.
    if (txn_requests_.Pop(&txn)) {
      // Execute txn.
      ExecuteTxn(txn);

      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

      // Return result to client.
      txn_results_.Push(txn);
    }
  }
}

void TxnProcessor::RunLockingScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    // Start processing the next incoming transaction request.
    if (txn_requests_.Pop(&txn)) {
      int blocked = 0;
      // Request read locks.
      for (set<Key>::iterator it = txn->readset_.begin();
           it != txn->readset_.end(); ++it) {
        if (!lm_->ReadLock(txn, *it))
          blocked++;
      }

      // Request write locks.
      for (set<Key>::iterator it = txn->writeset_.begin();
           it != txn->writeset_.end(); ++it) {
        if (!lm_->WriteLock(txn, *it))
          blocked++;
      }

      // If all read and write locks were immediately acquired, this txn is
      // ready to be executed.
      if (blocked == 0)
        ready_txns_.push_back(txn);
    }

    // Process and commit all transactions that have finished running.
    while (completed_txns_.Pop(&txn)) {
      // Release read locks.
      for (set<Key>::iterator it = txn->readset_.begin();
           it != txn->readset_.end(); ++it) {
        lm_->Release(txn, *it);
      }
      // Release write locks.
      for (set<Key>::iterator it = txn->writeset_.begin();
           it != txn->writeset_.end(); ++it) {
        lm_->Release(txn, *it);
      }

      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

      // Return result to client.
      txn_results_.Push(txn);
    }

    // Start executing all transactions that have newly acquired all their
    // locks.
    while (ready_txns_.size()) {
      // Get next ready txn from the queue.
      txn = ready_txns_.front();
      ready_txns_.pop_front();

      // Start txn running in its own thread.
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteTxn,
            txn));
    }
  }
}

void TxnProcessor::RunOCCScheduler() {
  Txn* txn;
  bool validResult;

  while (tp_.Active()) {
    // Record the start time of the next incoming transaction request and run
    // it in its own thread.
    if (txn_requests_.Pop(&txn)) {
      txn->occ_start_time_ = GetTime();
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteTxn,
            txn));
    }

    // Validate and commit/restart finished txns
    while (completed_txns_.Pop(&txn)) {
      if (txn->Status() == COMPLETED_C) {
        // Make sure that nothing in txn's readset or writeset was updated
        // after txn started
        validResult = true;
        for (set<Key>::iterator it = txn->readset_.begin();
             validResult && it != txn->readset_.end(); it++) {
          if (storage_.Timestamp(*it) > txn->occ_start_time_)
            validResult = false;
        }
        for (set<Key>::iterator it = txn->writeset_.begin();
             validResult && it != txn->writeset_.end(); it++) {
          if (storage_.Timestamp(*it) > txn->occ_start_time_)
            validResult = false;
        }

        // Commit txn if it passed validation; otherwise, restart it.
        if (validResult) {
          ApplyWrites(txn);
          txn->status_ = COMMITTED;
        } else {
          txn->status_ = INCOMPLETE;
          txn->reads_.clear();
          txn->writes_.clear();
          txn->occ_start_time_ = GetTime();
          tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
                this,
                &TxnProcessor::ExecuteTxn,
                txn));
        }
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
        validResult = true;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
        validResult = false;
      }

      // Only push the result to client if txn wasn't restarted.
      if (validResult)
        txn_results_.Push(txn);
    }
  }
}

void TxnProcessor::RunOCCParallelScheduler() {
  Txn* txn;

  while (tp_.Active()) {
    // Record the start time of the next incoming transaction request and run
    // it in its own thread.
      if (txn_requests_.Pop(&txn)) {
      txn->occ_start_time_ = GetTime();
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteTxn,
            txn));
    }

    // Put at most N_FINISHED_TXNS completed txns into validation.
    for (int i = 0; i < N_FINISHED_TXNS; i++) {
      if (completed_txns_.Pop(&txn)) {
        if (txn->Status() == COMPLETED_C) {
          // Copy the active set, add txn to the active set, and start
          // validation in a new thread.
          txn->active_copy_ = active_txns_;
          active_txns_.insert(txn);
          tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
                this,
                &TxnProcessor::ParallelValidateTxn,
                txn));
        } else if (txn->Status() == COMPLETED_A) {
          txn->status_ = ABORTED;
          txn_results_.Push(txn);
        } else {
          // Invalid TxnStatus!
          DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
        }
      } else {
        // completed_txns_ is empty
        break;
      }
    }

    // Commit/restart all transactions that have finished with
    // ParallelValidateTxn. We want to clear txns out of the active set as soon
    // as possible so that we don't have txns fail validation unnecessarily.
    while (validated_txns_.Pop(&txn)) {
      active_txns_.erase(txn);

      if (txn->Status() == COMPLETED_C) {
        // txn is valid! Mark it as committed and push the results to the
        // client. (The writes were already applied in ParallelValidateTxn.)
        txn->status_ = COMMITTED;
        txn_results_.Push(txn);
      } else if (txn->Status() == COMPLETED_A) {
        // txn is invalid! Completely restart it.
        txn->status_ = INCOMPLETE;
        txn->reads_.clear();
        txn->writes_.clear();
        txn->active_copy_.clear();
        txn->occ_start_time_ = GetTime();
        tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
              this,
              &TxnProcessor::ExecuteTxn,
              txn));
      } else {
        DIE("Validated Txn has invalid TxnStatus: " << txn->Status());
      }
    }
  }
}

void TxnProcessor::ParallelValidateTxn(Txn* txn) {
  bool valid = true;

  // If any record in txn's read- or write-set was last updated after txn's
  //   start time, txn is invalid.
  // While iterating over the write-set, also check if the write-set intersects
  //   with any read- or write-sets in our copy of the active set; if so, txn
  //   is invalid.
  for (set<Key>::iterator it = txn->readset_.begin();
       valid && it != txn->readset_.end(); it++) {
    if (storage_.Timestamp(*it) > txn->occ_start_time_)
      valid = false;
  }

  for (set<Key>::iterator it = txn->writeset_.begin();
       valid && it != txn->writeset_.end(); it++) {
    if (storage_.Timestamp(*it) > txn->occ_start_time_) {
      valid = false;
    } else {
      for (set<Txn*>::iterator t = txn->active_copy_.begin();
           valid && t != txn->active_copy_.end(); t++) {
        if ((*t)->writeset_.count(*it) > 0 || (*t)->readset_.count(*it) > 0)
          valid = false;
      }
    }
  }

  // Apply the writes of valid transactions.
  if (valid) {
    ApplyWrites(txn);
    txn->status_ = COMPLETED_C;
  } else {
    txn->status_ = COMPLETED_A;
  }

  validated_txns_.Push(txn);
}

void TxnProcessor::ExecuteTxn(Txn* txn) {
  // Read everything in from readset.
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_.Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Also read everything in from writeset.
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_.Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Execute txn's program logic.
  txn->Run();

  // Hand the txn back to the RunScheduler thread.
  completed_txns_.Push(txn);
}

void TxnProcessor::ApplyWrites(Txn* txn) {
  // Write buffered writes out to storage.
  for (map<Key, Value>::iterator it = txn->writes_.begin();
       it != txn->writes_.end(); ++it) {
    storage_.Write(it->first, it->second);
  }
}
