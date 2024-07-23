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

#include <brpc/controller.h>

#include <utility>

#include "agent/master_info.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "stream_load_context.h"
#include "util/defer_op.h"
#include "util/runtime_profile.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks {

TableGroupCommit::TableGroupCommit(std::string db, const std::string& table, bthreads::ThreadPoolExecutor* executor)
        : _db(std::move(db)), _table(table), _executor(executor) {}

Status TableGroupCommit::init() {
    bthread::ExecutionQueueOptions opts;
    opts.executor = _executor;
    if (int r = bthread::execution_queue_start(&_queue_id, &opts, _execute, this); r != 0) {
        LOG(ERROR) << "Fail to start execution queue for table group commit, db: " << _db << ", table: " << _table
                   << ", result: " << r;
        return Status::InternalError(fmt::format("fail to start bthread execution queue: {}", r));
    }
    LOG(INFO) << "Init table group commit, db: " << _db << ", table: " << _table;
    return Status::OK();
}

void TableGroupCommit::register_stream_load_context(StreamLoadContext* ctx) {
    ctx->ref();
    std::unique_lock<std::mutex> lock(_mutex_for_new_contexts);
    _new_contexts.emplace(ctx);
    _cv_for_new_contexts.notify_one();
    LOG(INFO) << "Register group commit load, db: " << ctx->db << ", table: " << ctx->table
              << ", txn_id: " << ctx->txn_id << ", label: " << ctx->label
              << ", fragment: " << print_id(ctx->fragment_instance_id) << ", active_time_ms: " << ctx->active_time_ms;
}

void TableGroupCommit::unregister_stream_load_context(StreamLoadContext* ctx) {
    // TODO unregister from _loading_contexts
    bool found = false;
    {
        std::unique_lock<std::mutex> lock(_mutex_for_new_contexts);
        if (_new_contexts.erase(ctx)) {
            found = true;
        }
    }
    if (found) {
        if (ctx->unref()) {
            delete ctx;
        }
    }
    LOG(INFO) << "Unregister group commit load, db: " << ctx->db << ", table: " << ctx->table
              << ", txn_id: " << ctx->txn_id << ", label: " << ctx->label
              << ", fragment: " << print_id(ctx->fragment_instance_id) << ", found: " << found;
}

Status TableGroupCommit::append_load(StreamLoadContext* ctx) {
    if (config::enable_stream_load_verbose_log) {
        LOG(INFO) << "start to append load to group commit, db: " << ctx->db << ", table: " << ctx->table
                  << ", id: " << ctx->id;
    }
    auto count_down_latch = BThreadCountDownLatch(1);
    Task task{.create_time_ns = MonotonicNanos(), .load_ctx = ctx, .latch = &count_down_latch};
    int r = bthread::execution_queue_execute(_queue_id, task);
    if (r != 0) {
        LOG(WARNING) << "Fail to add load to execution queue, db: " << ctx->db << ", table: " << ctx->table
                     << ", id: " << ctx->id;
        return Status::InternalError("Failed to append load to group commit execution queue");
    }
    count_down_latch.wait();
    if (config::enable_stream_load_verbose_log) {
        LOG(INFO) << "finish to append load to group commit, db: " << ctx->db << ", table: " << ctx->table
                  << ", id: " << ctx->id << ", txn_id: " << ctx->txn_id << ", label: " << ctx->label
                  << ", fragment: " << print_id(ctx->fragment_instance_id);
    }
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
    load_ctx->start_append_load_ts = MonotonicNanos();
    int64_t wait_group_commit_time_ns = 0;
    while (num_retries <= config::group_commit_request_load_retry_num) {
        if (config::enable_stream_load_verbose_log) {
            LOG(INFO) << "try to append load, db: " << load_ctx->db << ", table: " << load_ctx->table
                      << ", id: " << load_ctx->id << ", num_retries: " << num_retries;
        }
        st = _do_append_load(load_ctx);
        _clean_useless_contexts();
        if (st.ok()) {
            break;
        }
        {
            SCOPED_RAW_TIMER(&wait_group_commit_time_ns);
            _request_group_commit_load(load_ctx->label);
        }
        num_retries += 1;
    }
    load_ctx->group_commit_request_load_num = num_retries;
    load_ctx->finish_append_load_ts = MonotonicNanos();
    load_ctx->wait_group_commit_time_ns = wait_group_commit_time_ns;
    if (st.ok()) {
        return st;
    }
    if (config::enable_stream_load_verbose_log) {
        LOG(INFO) << "fail to append load after retries " << num_retries << ", db: " << load_ctx->db
                  << ", table: " << load_ctx->table << ", id: " << load_ctx->id << ", status: " << st.to_string();
    }
    return Status::InternalError(
            fmt::format("Can't append load after {} retries, last status: {}", num_retries, st.to_string()));
}

Status TableGroupCommit::_do_append_load(StreamLoadContext* load_ctx) {
    ByteBufferPtr buffer = load_ctx->buffer;
    Status st = Status::CapacityLimitExceed("no candidate group commit loads");
    for (auto* context : _loading_contexts) {
        st = context->body_sink->append(std::move(load_ctx->buffer));
        if (st.ok()) {
            load_ctx->txn_id = context->txn_id;
            load_ctx->label = context->label;
            load_ctx->fragment_instance_id = context->fragment_instance_id;
            int64_t left_time_ms = context->active_time_ms - (MonotonicNanos() - context->start_nanos) / 1000000;
            load_ctx->left_time_ms = std::max((int64_t)0, left_time_ms);
            if (config::enable_stream_load_verbose_log) {
                LOG(INFO) << "finish to send buffer to pipe, db: " << load_ctx->db << ", table: " << load_ctx->table
                          << ", id: " << load_ctx->id << ", txn_id: " << context->txn_id
                          << ", label: " << context->label << ", fragment: " << print_id(context->fragment_instance_id)
                          << ", left_time_ms: " << load_ctx->left_time_ms;
            }
            break;
        }
        if (config::enable_stream_load_verbose_log) {
            LOG(INFO) << "fail to send buffer to pipe, db: " << load_ctx->db << ", table: " << load_ctx->table
                      << ", id: " << load_ctx->id << ", txn_id: " << context->txn_id << ", label: " << context->label
                      << ", fragment: " << print_id(context->fragment_instance_id) << ", status: " << st;
        }
        _useless_contexts.emplace(context);
        load_ctx->buffer = buffer;
    }
    return st;
}

void TableGroupCommit::_clean_useless_contexts() {
    if (!_useless_contexts.empty()) {
        for (auto* ctx : _useless_contexts) {
            _loading_contexts.erase(ctx);
            if (config::enable_stream_load_verbose_log) {
                LOG(INFO) << "Clean useless group commit load, db: " << ctx->db << ", table: " << ctx->table
                          << ", txn_id: " << ctx->txn_id << ", label: " << ctx->label
                          << ", fragment: " << print_id(ctx->fragment_instance_id);
            }
            if (ctx->unref()) {
                delete ctx;
            }
        }
        _useless_contexts.clear();
    }
}

void TableGroupCommit::_request_group_commit_load(const std::string& user_label) {
    {
        std::unique_lock<std::mutex> lock(_mutex_for_new_contexts);
        if (!_new_contexts.empty()) {
            _loading_contexts.insert(_new_contexts.begin(), _new_contexts.end());
            _new_contexts.clear();
            return;
        }
    }
    _send_rpc_request(user_label);
    {
        std::unique_lock<std::mutex> lock(_mutex_for_new_contexts);
        if (!_new_contexts.empty()) {
            _loading_contexts.insert(_new_contexts.begin(), _new_contexts.end());
            _new_contexts.clear();
            return;
        }
        _cv_for_new_contexts.wait_for(lock, std::chrono::milliseconds(config::group_commit_request_load_interval_ms),
                                      [&]() { return !_new_contexts.empty(); });
        if (!_new_contexts.empty()) {
            _loading_contexts.insert(_new_contexts.begin(), _new_contexts.end());
            _new_contexts.clear();
        }
    }
}

void TableGroupCommit::_send_rpc_request(const std::string& user_label) {
    TNetworkAddress master_addr = get_master_address();
    TGroupCommitNotifyDataRequest request;
    request.__set_db(_db);
    request.__set_table(_table);
    request.__set_host(BackendOptions::get_localhost());
    request.__set_user_label(user_label);
    TGroupCommitNotifyDataResponse response;
    Status st = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &response](FrontendServiceConnection& client) {
                client->groupCommitNotifyData(response, request);
            },
            config::group_commit_thrift_rpc_timeout_ms);
    LOG(INFO) << "Request group commit load, db: " << _db << ", table: " << _table << ", user: " << user_label
              << ", st: " << st;
}

void TableGroupCommit::stop() {
    if (_stopped.load(std::memory_order_acquire)) {
        return;
    }
    bool expect = false;
    if (_stopped.compare_exchange_strong(expect, true, std::memory_order_acq_rel) &&
        _queue_id.value != kInvalidQueueId) {
        int r = bthread::execution_queue_stop(_queue_id);
        LOG_IF(WARNING, r != 0) << "Fail to stop execution queue, db: " << _db << ", table: " << _table
                                << ", result: " << r;
        r = bthread::execution_queue_join(_queue_id);
        LOG_IF(WARNING, r != 0) << "Fail to join execution queue db: " << _db << ", table: " << _table
                                << ", result: " << r;

        std::unordered_set<StreamLoadContext*> ctxs;
        ctxs.insert(_loading_contexts.begin(), _loading_contexts.end());
        ctxs.insert(_useless_contexts.begin(), _useless_contexts.end());
        for (auto* ctx : ctxs) {
            if (ctx->unref()) {
                delete ctx;
            }
        }
        LOG(INFO) << "Stop table group commit, db: " << _db << ", table: " << _table;
    }
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
    if (_stopped) {
        return Status::InternalError("GroupCommitMgr is stopped");
    }

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
    LOG(INFO) << "Create table group commit, db: " << db << ", table: " << table;
    return table_group_commit;
}

void GroupCommitMgr::stop() {
    {
        std::unique_lock<std::shared_mutex> lock(_mutex);
        if (_stopped) {
            return;
        }
        _stopped = true;
    }

    for (auto it : _tables) {
        it.second->stop();
    }
}

void GroupCommitMgr::execute_load(ExecEnv* exec_env, brpc::Controller* cntl, const PGroupCommitLoadRequest* request,
                                  PGroupCommitLoadResponse* response) {
    if (config::enable_stream_load_verbose_log) {
        LOG(INFO) << "Receive load request, " << request->DebugString() << ", size: " << cntl->request_attachment().size();
    }

    auto* ctx = new StreamLoadContext(exec_env);
    ctx->ref();
    DeferOp defer([&]() {
        ctx->buffer = nullptr;
        if (ctx->unref()) {
            delete ctx;
        }
    });
    ctx->load_type = TLoadType::MANUAL_LOAD;
    ctx->load_src_type = TLoadSourceType::RAW;
    ctx->db = request->db();
    ctx->table = request->table();
    ctx->label = request->user_label();
    ctx->group_commit = true;
    ctx->client_time_ms = request->client_time_ms();
    ctx->format = TFileFormatType::FORMAT_JSON;
    ctx->timeout_second = request->timeout();
    ctx->use_streaming = true;
    ctx->receive_header_unix_ms = UnixMillis();
    ctx->start_nanos = MonotonicNanos();

    if (request->has_data()) {
        ctx->body_bytes = request->data().size();
        ctx->buffer = ByteBuffer::allocate(ctx->body_bytes);
        ctx->buffer->put_bytes(request->data().data(), ctx->body_bytes);
        ctx->receive_bytes += ctx->body_bytes;
        ctx->total_receive_bytes += ctx->body_bytes;
        ctx->buffer->flip();
    } else {
        butil::IOBuf& io_buf = cntl->request_attachment();
        ctx->buffer = ByteBuffer::allocate(io_buf.size());
        io_buf.cutn(ctx->buffer->ptr, io_buf.size());
        ctx->buffer->pos += io_buf.size();
        ctx->body_bytes = io_buf.size();
        ctx->receive_bytes += ctx->body_bytes;
        ctx->total_receive_bytes += ctx->body_bytes;
        ctx->buffer->flip();
    }
    ctx->receive_chunk_end_ts = MonotonicNanos();
    auto status_or = exec_env->group_commit_mgr()->get_table_group_commit(ctx->db, ctx->table);
    if (!status_or.ok()) {
        LOG(ERROR) << "Can't find table group commit, db: " << ctx->db << ", table: " << ctx->table
                   << ", status: " << status_or.status();
        response->set_status("Fail");
        response->set_message("Can't find table group commit, " + status_or.status().to_string());
        return;
    }
    ctx->handle_start_ts = MonotonicNanos();
    auto st = status_or.value()->append_load(ctx);
    if (!st.ok()) {
        LOG(ERROR) << "Can't append group commit load, db: " << ctx->db << ", table: " << ctx->table
                   << ", status: " << st;
        response->set_status("Fail");
        response->set_message("Can't append group commit load, " + st.to_string());
        return;
    }
    ctx->handle_end_ts = MonotonicNanos();

    response->set_txn_id(ctx->txn_id);
    response->set_label(ctx->label);
    response->set_host(BackendOptions::get_localhost());
    response->set_fragment_id(print_id(ctx->fragment_instance_id));
    response->set_left_time_ms(ctx->left_time_ms);
    response->set_status("Success");
    response->set_message("OK");
    response->set_network_cost_ms(ctx->receive_header_unix_ms - ctx->client_time_ms);
    response->set_load_cost_ms((ctx->handle_end_ts - ctx->start_nanos) / 1000000);
    response->set_copy_data_ms((ctx->receive_chunk_end_ts - ctx->start_nanos) / 1000000);
    response->set_group_commit_ms((ctx->handle_end_ts - ctx->handle_start_ts) / 1000000);
    response->set_pending_ms((ctx->start_append_load_ts - ctx->handle_start_ts) / 1000000);
    response->set_wait_plan_ms(ctx->wait_group_commit_time_ns / 1000000);
    response->set_append_ms((ctx->finish_append_load_ts - ctx->start_append_load_ts - ctx->wait_group_commit_time_ns) /
                            1000000);
    response->set_request_plan_num(ctx->group_commit_request_load_num);
    response->set_finish_ts(UnixMillis());

    if (config::enable_stream_load_verbose_log) {
        LOG(INFO) << "Finish load, " << response->DebugString();
    }
}

} // namespace starrocks