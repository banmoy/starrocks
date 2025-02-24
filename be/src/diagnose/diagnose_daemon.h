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

#include <list>
#include <string>
#include <thread>

#include "bthread/condition_variable.h"
#include "bthread/mutex.h"
#include "common/status.h"

namespace starrocks {

enum class DiagnoseType { STACK_TRACE };

struct DiagnoseRequest {
    DiagnoseType type;
    std::string context;
};

class DiagnoseDaemon {
public:
    DiagnoseDaemon() = default;
    ~DiagnoseDaemon() = default;

    Status init();

    Status diagnose(DiagnoseRequest request);

    void stop();

private:
    void _schedule();
    void _execute_request(const DiagnoseRequest& request);
    void _diagnose_stack_trace(const std::string& context);

    std::unique_ptr<std::thread> _daemon;
    bthread::Mutex _mutex;
    bthread::ConditionVariable _cv;
    std::list<DiagnoseRequest> _requests;
    bool _stopped = false;

    int64_t _last_stack_trace_time_ms = 0;
};

} // namespace starrocks