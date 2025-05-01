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

package com.starrocks.load.mergecommit;

import com.starrocks.system.ComputeNode;

import java.util.List;
import java.util.Optional;

/**
 * Interface for assigning coordinator backends to loads.
 */
public interface CoordinatorBackendAssigner {

    /**
     * Starts the backend assigner.
     */
    void start();

    /**
     * Registers a merge commit with the specified parameters.
     *
     * @param id The ID of the merge commit operation.
     * @param warehouseId The id of the warehouse.
     * @param tableId The identifier of the table.
     * @param expectParallel The expected parallelism.
     */
    void registerMergeCommit(long id, long warehouseId, TableId tableId, int expectParallel);

    /**
     * Unregisters a merge commit.
     *
     * @param id The ID of the merge commit to unregister.
     */
    void unregisterMergeCommit(long id);

    /**
     * Retrieves the list of compute nodes assigned to the merge commit.
     *
     * @param id The ID of the merge commit.
     * @return A list of compute nodes assigned to the merge commit.
     */
    Optional<List<ComputeNode>> getBackends(long id);
}
