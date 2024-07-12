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

#include "runtime/stream_load/group_commit_mgr.h"

#include "agent/master_info.h"
#include "gen_cpp/FrontendService.h"
#include "runtime/client_cache.h"
#include "stream_load_context.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks {

TableGroupCommit::TableGroupCommit(const std::string& db, const std::string& table,
                                   bthreads::ThreadPoolExecutor* executor)
        : _db(db), _table(table), _executor(executor) {}

Status TableGroupCommit::init() {
    bthread::ExecutionQueueOptions opts;
    opts.executor = _executor;
    if (int r = bthread::execution_queue_start(&_queue_id, &opts, _execute, this); r != 0) {
        return Status::InternalError(fmt::format("fail to create bthread execution queue: {}", r));
    }
    return Status::OK();
}

void TableGroupCommit::register_stream_load_context(StreamLoadContext* context) {
    std::unique_lock<std::mutex> lock(_mutex_for_new_contexts);
    _new_contexts.emplace(context);
}

void TableGroupCommit::unregister_stream_load_context(StreamLoadContext* context) {
    // TODO unregister from _loading_contexts
    std::unique_lock<std::mutex> lock(_mutex_for_new_contexts);
    _new_contexts.erase(context);
}

Status TableGroupCommit::append_load(StreamLoadContext* ctx) {
    auto count_down_latch = BThreadCountDownLatch(1);
    Task task{.create_time_ns = MonotonicNanos(), .load_ctx = ctx, .latch = &count_down_latch};
    int r = bthread::execution_queue_execute(_queue_id, task);
    if (r != 0) {
        LOG(WARNING) << "Fail to add task to execution queue for " << _db << "." << _table << ": " << r;
        return Status::InternalError("Failed to append load to group commit execution queue");
    }
    count_down_latch.wait();
    return ctx->status;
}

int TableGroupCommit::_execute(void* meta, bthread::TaskIterator<Task>& iter) {
    if (iter.is_queue_stopped()) {
        return 0;
    }
    auto table_group_commit = static_cast<TableGroupCommit*>(meta);
    for (; iter; ++iter) {
        iter->load_ctx->status = table_group_commit->_try_append_load(iter->load_ctx);
        iter->latch->count_down();
    }
    return 0;
}

Status TableGroupCommit::_try_append_load(StreamLoadContext* load_ctx) {
    int num_retries = 0;
    Status st;
    do {
        st = _do_append_load(load_ctx);
        if (st.ok()) {
            return st;
        }
        _request_group_commit_load();
        num_retries += 1;
    } while (num_retries < 5);
    return Status::InternalError("Can't append load after 5 retries, last status: " + st.to_string());
}

Status TableGroupCommit::_do_append_load(StreamLoadContext* load_ctx) {
    ByteBufferPtr buffer = load_ctx->buffer;
    Status st = Status::CapacityLimitExceed("no candidates group commit loads");
    for (auto* context : _loading_contexts) {
        st = context->body_sink->append(std::move(load_ctx->buffer));
        if (st.ok()) {
            load_ctx->txn_id = context->txn_id;
            load_ctx->label = context->label;
            load_ctx->fragment_instance_id = context->fragment_instance_id;
            break;
        }
        _useless_contexts.emplace(context);
        load_ctx->buffer = buffer;
    }
    if (!_useless_contexts.empty()) {
        _loading_contexts.erase(_useless_contexts.begin(), _useless_contexts.end());
        _useless_contexts.clear();
    }
    return st;
}

void TableGroupCommit::_request_group_commit_load() {
    {
        std::unique_lock<std::mutex> lock(_mutex_for_new_contexts);
        if (!_new_contexts.empty()) {
            _loading_contexts.insert(_new_contexts.begin(), _new_contexts.end());
            _new_contexts.clear();
            return;
        }
    }
    _send_rpc_request();
    {
        std::unique_lock<std::mutex> lock(_mutex_for_new_contexts);
        if (!_new_contexts.empty()) {
            _loading_contexts.insert(_new_contexts.begin(), _new_contexts.end());
            _new_contexts.clear();
            return;
        }
        _cv_for_new_contexts.wait_for(lock, std::chrono::milliseconds(config::group_commit_wait_load_ms),
                                      [&]() { return !_new_contexts.empty(); });
        if (!_new_contexts.empty()) {
            _loading_contexts.insert(_new_contexts.begin(), _new_contexts.end());
            _new_contexts.clear();
        }
    }
}

void TableGroupCommit::_send_rpc_request() {
    TNetworkAddress master_addr = get_master_address();
    TGroupCommitNotifyDataRequest request;
    request.__set_db(_db);
    request.__set_table(_table);
    request.__set_host(BackendOptions::get_localhost());
    TGroupCommitNotifyDataResponse response;
    Status st = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &response](FrontendServiceConnection& client) {
                client->groupCommitNotifyData(response, request);
            },
            config::group_commit_thrift_rpc_timeout_ms);
    LOG(INFO) << "Request group commit load for " << _db << "." << _table << ", st: " << st;
}

StatusOr<TableGroupCommitSharedPtr> GroupCommitMgr::get_table_group_commit(const std::string& db,
                                                                           const std::string& table) {
    TableMeta table_id = std::make_pair(db, table);
    {
        std::shared_lock<std::shared_mutex> lock(_mutex);
        auto it = _tables.find(table_id);
        if (it != _tables.end()) {
            return it->second;
        }
    }

    std::unique_lock<std::shared_mutex> lock(_mutex);
    auto it = _tables.find(table_id);
    if (it != _tables.end()) {
        return it->second;
    }
    TableGroupCommitSharedPtr table_group_commit = std::make_shared<TableGroupCommit>(db, table, _executor.get());
    Status st = table_group_commit->init();
    if (!st.ok()) {
        LOG(ERROR) << "Fail to init table group commit, db: " << db << ", table: " << table << ", status: " << st;
        return Status::InternalError("Fail to init table group commit, " + st.to_string());
    }
    _tables.emplace(table_id, table_group_commit);
    return table_group_commit;
}

void GroupCommitMgr::stop() {
    // TODO
}

} // namespace starrocks