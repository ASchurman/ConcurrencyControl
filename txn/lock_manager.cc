// Original Author: Alexander Thomson (thomson@cs.yale.edu)
// Methods implemented by Alexander Schurman (alexander.schurman@yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "txn/lock_manager.h"
#include <algorithm>

////////////////////////////////////////////////////////////////////////////////
///////////////////////////// Helper Functions /////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

// Increments txn's entry in waits. If txn is not currently in waits, inserts
// an entry (txn, 1) into waits.
static inline void IncrementWait(Txn* txn, unordered_map<Txn*, int>* waits) {
  if (waits->count(txn) > 0)
    (*waits)[txn]++;
  else
    (*waits)[txn] = 1;
}

// Decrements a txn's entry in waits. If txn's entry becomes 0, removes it from
// waits and adds it to readys.
// Assumes that an entry for txn is already in waits with a positive value.
static inline void DecrementWait(Txn* txn,
                                 unordered_map<Txn*, int>* waits,
                                 deque<Txn*>* readys) {
  DCHECK(waits->count(txn) > 0 && (*waits)[txn] > 0);

  if ((*waits)[txn] == 1) {
    waits->erase(txn);
    readys->push_back(txn);
  } else
    (*waits)[txn]--;
}

////////////////////////////////////////////////////////////////////////////////
///////// Shared implementation between LockManagerA and LockManagerB //////////
////////////////////////////////////////////////////////////////////////////////

LockManager::~LockManager() {
  unordered_map<Key, deque<LockRequest>*>::iterator it;

  for (it = lock_table_.begin(); it != lock_table_.end(); it++) {
    if (it->second)
      delete it->second;
  }
}

LockMode LockManager::Status(const Key& key, vector<Txn*>* owners) {
  deque<LockRequest>* locks = lock_table_[key];

  if (owners) owners->clear();

  if (!locks || locks->empty())
    return UNLOCKED;
  else if (locks->front().mode_ == EXCLUSIVE) {
    if (owners) owners->push_back(locks->front().txn_);
    return EXCLUSIVE;
  } else {
    // Many txns can own a shared lock. Add them all to owners.
    if (owners) {
      for (deque<LockRequest>::iterator i = locks->begin();
           i != locks->end() && i->mode_ == SHARED;
           i++)
        owners->push_back(i->txn_);
    }
    return SHARED;
  }
}

void LockManager::Release(Txn* txn, const Key& key) {
  deque<LockRequest>* locks = lock_table_[key];
  // If we're releasing a lock on key, then lock_table_[key] must be nonempty
  DCHECK(locks && !locks->empty());

  vector<Txn*> owners;
  LockMode mode = Status(key, &owners);
  // If txn is releasing a lock, key must be locked by someone.
  DCHECK(mode != UNLOCKED);

  // Find txn's request in locks. Since locks contains LockRequests rather than
  // Txns, we can't use std::find.
  deque<LockRequest>::iterator txn_request = locks->end();
  for (deque<LockRequest>::iterator i = locks->begin();
       i != locks->end(); i++) {
    if (i->txn_ == txn) {
      txn_request = i;
      break;
    }
  }
  // If txn has requested a lock, it must be somewhere in the queue.
  DCHECK(txn_request != locks->end());

  // Invalidate txn's entry in txn_waits_ if it exists
  if (txn_waits_.count(txn) > 0)
    txn_waits_.erase(txn);
  
  // Remove txn's LockRequest from the queue of locks on key
  locks->erase(txn_request);

  // See if we need to issue new locks. Every current owner who was not an
  // owner before we released txn needs to be issued a lock.
  vector<Txn*> newOwners;
  if (Status(key, &newOwners) != UNLOCKED) {
    for (vector<Txn*>::iterator newIt = newOwners.begin();
         newIt != newOwners.end(); newIt++) {
      if (std::find(owners.begin(), owners.end(), *newIt) == owners.end())
        DecrementWait(*newIt, &txn_waits_, ready_txns_);
    }
  }
}

bool LockManager::WriteLock(Txn* txn, const Key& key) {
  LockMode priorStatus = Status(key, NULL);

  if (!lock_table_[key])
    lock_table_[key] = new deque<LockRequest>();
  lock_table_[key]->push_back(LockRequest(EXCLUSIVE, txn));

  if (priorStatus == UNLOCKED) {
    // No one owned a lock on key before txn
    return true;
  } else {
    // key was already locked before we added txn to the queue
    IncrementWait(txn, &txn_waits_);
    return false;
  }
}

////////////////////////////////////////////////////////////////////////////////
//////////////////// LockManagerA/B-Specific Implementation ////////////////////
////////////////////////////////////////////////////////////////////////////////

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key) {
  // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
  // simply use the same logic as 'WriteLock'.
  return WriteLock(txn, key);
}

LockManagerB::LockManagerB(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key) {
  if (!lock_table_[key])
    lock_table_[key] = new deque<LockRequest>();
  lock_table_[key]->push_back(LockRequest(SHARED, txn));

  vector<Txn*> owners;
  LockMode mode = Status(key, &owners);

  // We just pushed a new LockRequest, so key shouldn't be unlocked.
  DCHECK(mode != UNLOCKED);

  if (std::find(owners.begin(), owners.end(), txn) == owners.end()) {
    // Our txn did not get a lock
    IncrementWait(txn, &txn_waits_);
    return false;
  } else {
    // Our txn got a lock
    return true;
  }
}

