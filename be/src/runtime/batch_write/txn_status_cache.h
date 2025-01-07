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

#pragma once

#include <map>

#include "util/bthreads/bthread_shared_mutex.h"
#include "util/dynamic_cache.h"
#include "util/threadpool.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks {

class TxnStatusHolder;
class TxnStatusWaiter;
class TxnStatusCache;
using TxnStatusDynamicCache = DynamicCache<int64_t, TxnStatusHolder>;
using TxnStatusDynamicCachePtr = std::unique_ptr<TxnStatusDynamicCache>;
using TxnStatusDynamicCacheEntry = TxnStatusDynamicCache::Entry;

class TxnStatusHolder {
public:
    void update_txn_status(TTransactionStatus::type new_status, const std::string& reason);

    void inc_waiter();
    void dec_waiter();
    Status wait_final_status(TxnStatusWaiter* waiter, int64_t timeout_us);

    void set_txn_id(int64_t txn_id) { _txn_id.store(txn_id); }
    TTransactionStatus::type txn_status();
    std::string reason();
    std::string debug_string();

    void stop();

private:
    // lazy initialized
    std::atomic<int64_t> _txn_id{-1};
    bthread::Mutex _mutex;
    bthread::ConditionVariable _cv;
    TTransactionStatus::type _txn_status{TTransactionStatus::PREPARE};
    std::string _reason;
    int32_t _num_waiter{0};
    int32_t _num_waiting_final_status{0};
    bool _stopped{false};
};

inline std::ostream& operator<<(std::ostream& os, TxnStatusHolder& holder) {
    os << holder.debug_string();
    return os;
}

class TxnStatusWaiter {
public:
    TxnStatusWaiter(TxnStatusDynamicCache* cache, TxnStatusDynamicCacheEntry* entry, const std::string& name)
            : _cache(cache), _entry(entry), _name(name) {
        _entry->value().inc_waiter();
    }

    ~TxnStatusWaiter() {
        _entry->value().dec_waiter();
        _cache->release(_entry);
    }

    const std::string& name() const { return _name; }

    Status wait_final_status(int64_t timeout_us);
    TTransactionStatus::type txn_status();
    std::string reason();

private:
    TxnStatusDynamicCache* _cache;
    TxnStatusDynamicCacheEntry* _entry;
    std::string _name;
};
using TxnStatusWaiterPtr = std::unique_ptr<TxnStatusWaiter>;

// TODO
// 1. support txn status expire
// 2. support poll txn status
class TxnStatusCache {
public:
    TxnStatusCache(size_t capacity);

    Status notify_txn(int64_t txn_id, TTransactionStatus::type status, const std::string& reason);

    StatusOr<TxnStatusWaiterPtr> create_waiter(int64_t txn_id, const std::string& waiter_name);

    void set_capacity(size_t new_capacity);

    void stop();

private:
    static const int kNumShardBits = 5;
    static const int kNumShards = 1 << kNumShardBits;

    friend class TxnStatusHolder;

    TxnStatusDynamicCache* _get_txn_cache(int64_t txn_id);
    StatusOr<TxnStatusDynamicCacheEntry*> _get_or_create_txn_entry(TxnStatusDynamicCache* cache, int64_t txn_id);

    size_t _capacity;
    TxnStatusDynamicCachePtr _shards[kNumShards];
    bthreads::BThreadSharedMutex _rw_mutex;
    bool _stopped{false};
};

inline TxnStatusDynamicCache* TxnStatusCache::_get_txn_cache(int64_t txn_id) {
    return _shards[txn_id & (kNumShards - 1)].get();
}
} // namespace starrocks