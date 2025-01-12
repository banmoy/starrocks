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
#include <unordered_set>
#include <vector>

#include "common/utils.h"
#include "testutil/sync_point.h"
#include "util/bthreads/bthread_shared_mutex.h"
#include "util/dynamic_cache.h"
#include "util/threadpool.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks {

class ThreadPoolToken;
class TxnStateHandler;
class TxnStateSubscriber;
class TxnStateCache;
using TxnStateDynamicCache = DynamicCache<int64_t, TxnStateHandler>;
using TxnStateDynamicCachePtr = std::unique_ptr<TxnStateDynamicCache>;
using TxnStateDynamicCacheEntry = TxnStateDynamicCache::Entry;

struct TxnState {
    TTransactionStatus::type txn_status{TTransactionStatus::PREPARE};
    std::string reason;
};

inline std::ostream& operator<<(std::ostream& os, const TxnState& txn_state) {
    os << "txn_status: " << to_string(txn_state.txn_status) << ", reason: " << txn_state.reason;
    return os;
}

class TxnStateHandler {
public:
    ~TxnStateHandler();

    void update_state(TTransactionStatus::type new_status, const std::string& reason);
    // return whether to continue polling
    bool notify_poll_result(const StatusOr<TxnState>& result);

    // return whether to trigger polling
    bool acquire_subscriber();
    void release_subscriber();
    int32_t num_waiting_subscriber();
    StatusOr<TxnState> wait_finished_state(const std::string& subscriber_name, int64_t timeout_us);

    void set_txn_id(int64_t txn_id) { _txn_id.store(txn_id); }
    int64_t txn_id() { return _txn_id.load(); }
    TxnState txn_state();
    std::string debug_string();

    void stop();

private:
    void _transition_txn_state(TTransactionStatus::type new_status, const std::string& reason);
    // Whether the current status indicate the load is finished
    bool _is_finished_txn_state();

    // lazy initialized
    std::atomic<int64_t> _txn_id{-1};
    bthread::Mutex _mutex;
    bthread::ConditionVariable _cv;
    TxnState _txn_state;
    int32_t _num_subscriber{0};
    int32_t _num_waiting_subscriber{0};
    bool _stopped{false};
};

inline bool TxnStateHandler::acquire_subscriber() {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    _num_subscriber++;
    // should trigger polling if this is the first subscriber
    return _num_subscriber == 1 && !_is_finished_txn_state();
}

inline void TxnStateHandler::release_subscriber() {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    _num_subscriber--;
}

inline int32_t TxnStateHandler::num_waiting_subscriber() {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    return _num_waiting_subscriber;
}

inline TxnState TxnStateHandler::txn_state() {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    return _txn_state;
}

inline std::ostream& operator<<(std::ostream& os, TxnStateHandler& holder) {
    os << holder.debug_string();
    return os;
}

class TxnStateSubscriber {
public:
    TxnStateSubscriber(TxnStateDynamicCache* cache, TxnStateDynamicCacheEntry* entry, const std::string& name)
            : _cache(cache), _entry(entry), _name(name) {};

    ~TxnStateSubscriber() {
        _entry->value().release_subscriber();
        _cache->release(_entry);
    }

    const std::string& name() const { return _name; }
    StatusOr<TxnState> wait_finished_state(int64_t timeout_us);
    TxnState current_state();
    TxnStateDynamicCacheEntry* entry() { return _entry; }

private:
    TxnStateDynamicCache* _cache;
    TxnStateDynamicCacheEntry* _entry;
    std::string _name;
};
using TxnStateSubscriberPtr = std::unique_ptr<TxnStateSubscriber>;

struct TxnStatePollTask {
    int64_t txn_id;
    std::string db;
    std::string tbl;
    AuthInfo auth;
};

class TxnStatePoller {
public:
    TxnStatePoller(TxnStateCache* txn_state_cache, ThreadPoolToken* poll_token)
            : _txn_state_cache(txn_state_cache), _poll_token(poll_token) {}
    Status init();
    void submit(const TxnStatePollTask& task, int64_t delay_ms);
    void stop();

private:
    void _schedule_func();
    void _schedule_poll_tasks(const std::vector<TxnStatePollTask>& poll_tasks);
    void _execute_poll(const TxnStatePollTask& task);

    TxnStateCache* _txn_state_cache;
    ThreadPoolToken* _poll_token;
    std::unique_ptr<std::thread> _schedule_thread;
    bthread::Mutex _mutex;
    bthread::ConditionVariable _cv;
    // pending txn ids, used to avoid duplicated poll
    std::unordered_set<int64_t> _pending_txn_ids;
    // execute time in milliseconds -> task
    std::multimap<int64_t, TxnStatePollTask> _pending_tasks;
    bool _stopped{false};
};

// TODO support txn state expire
class TxnStateCache {
public:
    TxnStateCache(size_t capacity, std::unique_ptr<ThreadPoolToken> poller_token);
    Status init();

    Status update_state(int64_t txn_id, TTransactionStatus::type status, const std::string& reason);
    // Return Status::NotFound if txn_id is not in cache
    StatusOr<TxnState> get_state(int64_t txn_id);

    StatusOr<TxnStateSubscriberPtr> subscribe_state(int64_t txn_id, const std::string& subscriber_name,
                                                    const std::string& db, const std::string& tbl,
                                                    const AuthInfo& auth);

    void set_capacity(size_t new_capacity);
    int32_t size();

    void stop();

    // For testing
    std::vector<TxnStateDynamicCache*> get_cache_shards() {
        std::vector<TxnStateDynamicCache*> ret;
        for (auto& shard : _shards) {
            ret.push_back(shard.get());
        }
        return ret;
    }

private:
    static const int kNumShardBits = 5;
    static const int kNumShards = 1 << kNumShardBits;

    friend class TxnStatePoller;

    TxnStateDynamicCache* _get_txn_cache(int64_t txn_id);
    StatusOr<TxnStateDynamicCacheEntry*> _get_txn_entry(TxnStateDynamicCache* cache, int64_t txn_id,
                                                        bool create_if_not_exist);
    void _notify_poll_result(const TxnStatePollTask& task, StatusOr<TxnState> result);

    size_t _capacity;
    std::unique_ptr<ThreadPoolToken> _poller_token;
    TxnStateDynamicCachePtr _shards[kNumShards];
    std::unique_ptr<TxnStatePoller> _txn_state_poller;
    bthreads::BThreadSharedMutex _rw_mutex;
    bool _stopped{false};
};

inline TxnStateDynamicCache* TxnStateCache::_get_txn_cache(int64_t txn_id) {
    return _shards[txn_id & (kNumShards - 1)].get();
}
} // namespace starrocks