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

#include "storage/binlog_file_writer.h"
#include "storage/binlog_manager.h"

namespace starrocks {

// The result of builder which will be applied to the metas in BinlogManager
struct BinlogBuildResult {
    // the writer can be used for the next build
    std::shared_ptr<BinlogFileWriter> active_writer;
    // Binlog file metas generated in this build
    std::vector<std::shared_ptr<BinlogFileMetaPB>> metas;
    // Rowsets used by the binlog in this build
    RowsetIdMap rowsets;
};

class BinlogBuilder {
public:
    BinlogBuilder(int64_t version, std::shared_ptr<BinlogManager> binlog_manager,
                  std::shared_ptr<BinlogFileMetaPB> reused_file_meta,
                  std::shared_ptr<BinlogFileWriter> reused_file_writer);

    int64_t version() { return _version; }

    // Generate binlog for duplicate key table which only contains appended
    // data. Return Status::OK() if the binlog data has been persisted.
    Status append_rowset(const RowsetSharedPtr& rowset);

    // Get the result of a successful build, that's, append_rowset() has
    // returned Status::OK
    std::shared_ptr<BinlogBuildResult> build();

    // Abort the builder, and clean up the resource created by this builder,
    // such as truncating the binlog file to discard the useless data appended
    // by this builder, and deleted useless binlog files. abort() can be both
    // called when append_rowset() is successful or failed.
    // A BinlogBuildResult will be returned to tell the BinlogManager how to
    // update metas. Actually only BinlogBuildResult#active_writer is useful
    // in this case
    std::shared_ptr<BinlogBuildResult> abort();

private:
    Status _commit_current_writer(bool end_of_rowset, bool close_writer);
    void _abort_current_writer();
    void _delete_new_files();

    int64_t _version;
    std::shared_ptr<BinlogManager> _binlog_manager;
    std::shared_ptr<BinlogFileMetaPB> _reused_file_meta;
    std::shared_ptr<BinlogFileWriter> _reused_file_writer;

    int64_t _next_seq_id;
    std::vector<std::string> _new_files;
    std::shared_ptr<BinlogFileWriter> _current_writer;
    std::vector<std::shared_ptr<BinlogFileMetaPB>> _new_metas;
    RowsetIdMap _new_rowsets;
};

} // namespace starrocks
