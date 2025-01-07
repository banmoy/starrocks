// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "runtime/batch_write/txn_status_cache.h"

#include <utility>

#include "runtime/batch_write/batch_write_util.h"

namespace starrocks {

bool is_final_txn_status(const TTransactionStatus::type& status) {
    switch (status) {
    case TTransactionStatus::VISIBLE:
    case TTransactionStatus::ABORTED:
    case TTransactionStatus::UNKNOWN:
        return true;
    default:
        return false;
    }
}

void TxnStatusHolder::update_txn_status(TTransactionStatus::type new_status, const std::string& reason) {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    if (_stopped) {
        return;
    }
    if (is_final_txn_status(_txn_status)) {
        return;
    } else if (_txn_status == TTransactionStatus::PREPARED && new_status == TTransactionStatus::PREPARE) {
        return;
    } else if (_txn_status == TTransactionStatus::COMMITTED &&
               (new_status != TTransactionStatus::VISIBLE || TTransactionStatus::UNKNOWN)) {
        return;
    }
    TRACE_BATCH_WRITE << "update txn status, txn_id: " << _txn_id << ", old status: " << to_string(_txn_status)
                      << ", reason: " << _reason << ", new status: " << new_status << ", reason: " << reason;
    _txn_status = new_status;
    _reason = reason;
    if (is_final_txn_status(_txn_status)) {
        _cv.notify_all();
    }
}

Status TxnStatusHolder::wait_final_status(TxnStatusWaiter* waiter, int64_t timeout_us) {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    if (is_final_txn_status(_txn_status)) {
        return Status::OK();
    }
    if (_stopped) {
        return Status::ServiceUnavailable("Transaction status holder is stopped");
    }
    _num_waiting_final_status++;
    DeferOp defer([&] { _num_waiting_final_status--; });

    int64_t left_timeout_us = timeout_us;
    while (left_timeout_us > 0) {
        TRACE_BATCH_WRITE << "start wait final status, name: " << waiter->name() << ", txn_id: " << _txn_id
                          << ", timeout_us: " << left_timeout_us;
        auto start_us = MonotonicMicros();
        int ret = _cv.wait_for(lock, left_timeout_us);
        int64_t elapsed_us = MonotonicMicros() - start_us;
        TRACE_BATCH_WRITE << "finish wait final status, name: " << waiter->name() << ", txn_id: " << _txn_id
                          << ", elapsed: " << elapsed_us << " us, txn_status: " << to_string(_txn_status)
                          << ", reason: " << _reason << ", stopped: " << _stopped;
        left_timeout_us = std::max((int64_t)0, left_timeout_us - elapsed_us);
        if (is_final_txn_status(_txn_status)) {
            return Status::OK();
        } else if (_stopped) {
            return Status::ServiceUnavailable("Transaction status holder is stopped");
        } else if (ret == ETIMEDOUT) {
            break;
        }
    }
    return Status::TimedOut(fmt::format("Wait txn status timeout {} us", timeout_us));
}

void TxnStatusHolder::stop() {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    if (_stopped) {
        return;
    }
    _stopped = true;
    _cv.notify_all();
}

inline TTransactionStatus::type TxnStatusHolder::txn_status() {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    return _txn_status;
}

inline std::string TxnStatusHolder::reason() {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    return _reason;
}

std::string TxnStatusHolder::debug_string() {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    return fmt::format(
            "txn_id: {}, txn_status: {}, reason: {}, num_waiter: {}, num_waiting_final_status: {}, stopped: {}",
            _txn_id.load(), to_string(_txn_status), _reason, _num_waiter, _num_waiting_final_status, _stopped);
}

Status TxnStatusWaiter::wait_final_status(int64_t timeout_us) {
    return _entry->value().wait_final_status(this, timeout_us);
}

TTransactionStatus::type TxnStatusWaiter::txn_status() {
    return _entry->value().txn_status();
}

std::string TxnStatusWaiter::reason() {
    return _entry->value().reason();
}

TxnStatusCache::TxnStatusCache(size_t capacity) : _capacity(capacity) {
    size_t capacity_per_shard = (_capacity + (kNumShards - 1)) / kNumShards;
    for (int32_t i = 0; i < kNumShards; i++) {
        _shards[i] = std::make_unique<TxnStatusDynamicCache>(capacity_per_shard);
    }
}

Status TxnStatusCache::notify_txn(int64_t txn_id, TTransactionStatus::type status, const std::string& reason) {
    auto cache = _get_txn_cache(txn_id);
    ASSIGN_OR_RETURN(auto entry, _get_or_create_txn_entry(cache, txn_id));
    entry->value().update_txn_status(status, reason);
    cache->release(entry);
    return Status::OK();
}

StatusOr<TxnStatusWaiterPtr> TxnStatusCache::create_waiter(int64_t txn_id, const std::string& waiter_name) {
    auto cache = _get_txn_cache(txn_id);
    ASSIGN_OR_RETURN(auto entry, _get_or_create_txn_entry(cache, txn_id));
    return std::make_unique<TxnStatusWaiter>(cache, entry, waiter_name);
}

void TxnStatusCache::set_capacity(size_t new_capacity) {
    std::unique_lock<bthreads::BThreadSharedMutex> lock;
    if (_stopped) {
        return;
    }
    const size_t capacity_per_shard = (new_capacity + (kNumShards - 1)) / kNumShards;
    for (auto& _shard : _shards) {
        _shard->set_capacity(capacity_per_shard);
    }
    _capacity = new_capacity;
}

void TxnStatusCache::stop() {
    {
        std::unique_lock<bthreads::BThreadSharedMutex> lock;
        if (_stopped) {
            return;
        }
        _stopped = true;
    }
    for (auto& cache : _shards) {
        auto entries = cache->get_all_entries();
        for (auto entry : entries) {
            entry->value().stop();
            cache->release(entry);
        }
    }
}

StatusOr<TxnStatusDynamicCacheEntry*> TxnStatusCache::_get_or_create_txn_entry(TxnStatusDynamicCache* cache,
                                                                               int64_t txn_id) {
    // use lock to avoid creating new entry after stopped
    TxnStatusDynamicCacheEntry* entry = nullptr;
    {
        std::shared_lock<bthreads::BThreadSharedMutex> lock;
        if (_stopped) {
            return Status::ServiceUnavailable("Transaction status cache is stopped");
        }
        entry = cache->get_or_create(txn_id, 1);
    }
    // initialize txn_id
    entry->value().set_txn_id(txn_id);
    return entry;
}

} // namespace starrocks