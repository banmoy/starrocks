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

#include <re2/re2.h>

#include "fs/fs_util.h"
#include "gen_cpp/binlog.pb.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/substitute.h"
#include "storage/rowset/rowset.h"
#include "storage/olap_common.h"
#include "storage/uint24.h"

namespace starrocks {

using RowsetVersionMap = std::unordered_map<Version, RowsetSharedPtr, HashOfVersion>;
using RowsetIdMap = std::unordered_map<RowsetId, RowsetSharedPtr, HashOfRowsetId>;

class BinlogUtil {
public:
    static std::string binlog_file_path(std::string& binlog_dir, int64_t file_id) {
        return strings::Substitute("$0/$1.binlog", binlog_dir, file_id);
    }

    static int128_t get_lsn(int64_t version, int64_t seq_id) { return (((int128_t)version) << 64) | seq_id; }

    static void convert_pb_to_rowset_id(const RowsetIdPB& pb, RowsetId* rowset_id) {
        rowset_id->hi = pb.hi();
        rowset_id->mi = pb.mi();
        rowset_id->lo = pb.lo();
    }

    static void convert_rowset_id_to_pb(const RowsetId& rowset_id, RowsetIdPB* pb) {
        pb->set_hi(rowset_id.hi);
        pb->set_mi(rowset_id.mi);
        pb->set_lo(rowset_id.lo);
    }

    static bool get_file_id_from_name(const std::string& file_name, int64_t* file_id) {
        static re2::RE2 re(R"((\d+)\.binlog)", re2::RE2::Quiet);
        std::string fild_id_str;
        bool ret = RE2::PartialMatch(file_name, re, &fild_id_str);
        if (!ret) {
            return false;
        }

        ret = safe_strto64(fild_id_str, file_id);
        if (!ret) {
            LOG(WARNING) << "Invalid binlog file name, file_name: " << file_name << ", file_id_str: " << fild_id_str;
            return false;
        }
        return true;
    }

    static Status list_binlog_file_ids(std::string& binlog_dir, std::set<int64_t>* binlog_file_ids) {
        std::set<std::string> file_names;
        RETURN_IF_ERROR(fs::list_dirs_files(binlog_dir, nullptr, &file_names));
        int64_t file_id;
        for (auto& name : file_names) {
            bool ret = get_file_id_from_name(name, &file_id);
            if (ret) {
                binlog_file_ids->emplace(file_id);
            }
        }
        return Status::OK();
    }
};

} // namespace starrocks
