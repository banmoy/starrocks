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

#include <bthread/condition_variable.h>
#include <bthread/execution_queue.h>
#include <bthread/mutex.h>

#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "common/statusor.h"
#include "util/bthreads/executor.h"
#include "util/countdown_latch.h"
#include "util/time.h"

namespace starrocks {

class StreamLoadContext;
class Status;

struct pair_hash {
    template <class T1, class T2>
    std::size_t operator()(const std::pair<T1, T2>& p) const {
        auto hash1 = std::hash<T1>{}(p.first);
        auto hash2 = std::hash<T2>{}(p.second);
        return hash1 ^ (hash2 << 1);
    }
};
using TableMeta = std::pair<std::string, std::string>;

using BThreadCountDownLatch = GenericCountDownLatch<bthread::Mutex, bthread::ConditionVariable>;

struct Task {
    int64_t create_time_ns;
    bool unregister{false};
    StreamLoadContext* load_ctx;
    BThreadCountDownLatch* latch;
};

class TableGroupCommit {
public:
    explicit TableGroupCommit(std::string db, const std::string& table, bthreads::ThreadPoolExecutor* executor);

    Status init();

    void register_stream_load_context(StreamLoadContext* context);
    void unregister_stream_load_context(StreamLoadContext* context);

    Status append_load(StreamLoadContext* ctx);

    void stop();

private:
    static int _execute(void* meta, bthread::TaskIterator<Task>& iter);

    Status _try_append_load(StreamLoadContext* ctx);
    Status _do_append_load(StreamLoadContext* ctx);
    void _clean_useless_contexts();
    void _request_group_commit_load();
    void _send_rpc_request();

    std::string _db;
    std::string _table;
    bthreads::ThreadPoolExecutor* _executor;
    std::mutex _mutex_for_new_contexts;
    std::condition_variable _cv_for_new_contexts;
    std::unordered_set<StreamLoadContext*> _new_contexts;
    // single thread access
    std::unordered_set<StreamLoadContext*> _loading_contexts;
    std::unordered_set<StreamLoadContext*> _useless_contexts;

    // Undocemented rule of bthread that -1(0xFFFFFFFFFFFFFFFF) is an invalid ExecutionQueueId
    constexpr static uint64_t kInvalidQueueId = (uint64_t)-1;
    bthread::ExecutionQueueId<Task> _queue_id{kInvalidQueueId};
    std::atomic<bool> _stopped{false};
};
using TableGroupCommitSharedPtr = std::shared_ptr<TableGroupCommit>;

class GroupCommitMgr {
public:
    GroupCommitMgr(std::unique_ptr<bthreads::ThreadPoolExecutor> executor) : _executor(std::move(executor)){};

    StatusOr<TableGroupCommitSharedPtr> get_table_group_commit(const std::string& db, const std::string& table);

    void stop();

private:
    std::unique_ptr<bthreads::ThreadPoolExecutor> _executor;
    std::shared_mutex _mutex;
    std::unordered_map<TableMeta, TableGroupCommitSharedPtr, pair_hash> _tables;
    bool _stopped{false};
};

} // namespace starrocks
