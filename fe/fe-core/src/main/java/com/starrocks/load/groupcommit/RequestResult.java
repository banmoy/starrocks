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

package com.starrocks.load.groupcommit;

import com.google.common.base.Preconditions;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;

public abstract class RequestResult<T> {

    protected final TStatus status;
    protected final T result;

    public RequestResult(TStatus status, T result) {
        Preconditions.checkArgument((
                status.status_code == TStatusCode.OK && result != null)
                || (status.status_code != TStatusCode.OK && result == null));
        this.status = status;
        this.result = result;
    }

    public boolean isOk() {
        return status.status_code.equals(TStatusCode.OK);
    }

    public TStatus getStatus() {
        return status;
    }

    public T getResult() {
        return result;
    }
}
