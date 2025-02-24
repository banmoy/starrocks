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

#include "diagnose/diagnose_daemon.h"

#include <fmt/format.h>

#include "common/config.h"
#include "util/stack_util.h"

namespace starrocks {

static std::string diagnose_type_name(DiagnoseType type) {
    switch (type) {
    case DiagnoseType::STACK_TRACE:
        return "STACK_TRACE";
    default:
        return fmt::format("UNKNOWN({})", static_cast<int>(type));
    }
}

Status DiagnoseDaemon::init() {
    _daemon = std::make_unique<std::thread>([this] { _schedule(); });
    return Status::OK();
}

Status DiagnoseDaemon::diagnose(starrocks::DiagnoseRequest request) {
    std::lock_guard<bthread::Mutex> l(_mutex);
    if (_stopped) {
        return Status::InternalError("diagnose daemon is stopped");
    }
    _requests.push_back(std::move(request));
    _cv.notify_all();
    return Status::OK();
}

void DiagnoseDaemon::stop() {
    {
        std::lock_guard<bthread::Mutex> l(_mutex);
        _stopped = true;
        _cv.notify_all();
    }
    if (_daemon && _daemon->joinable()) {
        _daemon->join();
    }
}

void DiagnoseDaemon::_schedule() {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    while (!_stopped) {
        if (!_requests.empty()) {
            auto request = _requests.front();
            _requests.pop_front();
            lock.unlock();
            _execute_request(request);
            lock.lock();
        } else {
            _cv.wait(lock);
        }
    }
}

void DiagnoseDaemon::_execute_request(const starrocks::DiagnoseRequest& request) {
    switch (request.type) {
    case DiagnoseType::STACK_TRACE:
        _diagnose_stack_trace(request.context);
        break;
    default:
        LOG(WARNING) << "unknown diagnose type: " << diagnose_type_name(request.type)
                     << ", context: " << request.context;
    }
}

void DiagnoseDaemon::_diagnose_stack_trace(const std::string& context) {
    int64_t interval = MonotonicMillis() - _last_stack_trace_time_ms;
    if (interval < config::diagnose_stack_trace_interval_ms) {
        LOG(WARNING) << "diagnose stack trace is too frequent, interval: " << interval << "ms";
        return;
    }
    std::string stack_trace = get_stack_trace_for_all_threads(30000);
    _last_stack_trace_time_ms = MonotonicMillis();
    LOG(INFO) << "diagnose stack trace: " << context << "\n" << stack_trace;
}

} // namespace starrocks