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

class TxnStateHolder;
class TxnStateSubscriber;
class TxnStateCache;
using TxnStateDynamicCache = DynamicCache<int64_t, TxnStateHolder>;
using TxnStateDynamicCachePtr = std::unique_ptr<TxnStateDynamicCache>;
using TxnStateDynamicCacheEntry = TxnStateDynamicCache::Entry;

class TxnStateHolder {
public:
    void update_state(TTransactionStatus::type new_status, const std::string& reason);

    void add_subscriber();
    void release_subscriber();
    Status wait_final_status(TxnStateSubscriber* subscriber, int64_t timeout_us);

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
    int32_t _num_subscriber{0};
    int32_t _num_waiting_final_status{0};
    bool _stopped{false};
};

inline void TxnStateHolder::add_subscriber() {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    _num_subscriber++;
}

inline void TxnStateHolder::release_subscriber() {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    _num_subscriber--;
}

inline std::ostream& operator<<(std::ostream& os, TxnStateHolder& holder) {
    os << holder.debug_string();
    return os;
}

class TxnStateSubscriber {
public:
    TxnStateSubscriber(TxnStateDynamicCache* cache, TxnStateDynamicCacheEntry* entry, const std::string& name)
            : _cache(cache), _entry(entry), _name(name) {
        _entry->value().add_subscriber();
    }

    ~TxnStateSubscriber() {
        _entry->value().release_subscriber();
        _cache->release(_entry);
    }

    const std::string& name() const { return _name; }

    Status wait_final_status(int64_t timeout_us);
    TTransactionStatus::type txn_status();
    std::string reason();

private:
    TxnStateDynamicCache* _cache;
    TxnStateDynamicCacheEntry* _entry;
    std::string _name;
};
using TxnStateSubscriberPtr = std::unique_ptr<TxnStateSubscriber>;

// TODO
// 1. support txn status expire
// 2. support poll txn status
class TxnStateCache {
public:
    TxnStateCache(size_t capacity);

    Status update_state(int64_t txn_id, TTransactionStatus::type status, const std::string& reason);

    StatusOr<TxnStateSubscriberPtr> create_subscriber(int64_t txn_id, const std::string& subscriber_name);

    void set_capacity(size_t new_capacity);

    void stop();

private:
    static const int kNumShardBits = 5;
    static const int kNumShards = 1 << kNumShardBits;

    friend class TxnStateHolder;

    TxnStateDynamicCache* _get_txn_cache(int64_t txn_id);
    StatusOr<TxnStateDynamicCacheEntry*> _get_or_create_txn_entry(TxnStateDynamicCache* cache, int64_t txn_id);

    size_t _capacity;
    TxnStateDynamicCachePtr _shards[kNumShards];
    bthreads::BThreadSharedMutex _rw_mutex;
    bool _stopped{false};
};

inline TxnStateDynamicCache* TxnStateCache::_get_txn_cache(int64_t txn_id) {
    return _shards[txn_id & (kNumShards - 1)].get();
}
} // namespace starrocks