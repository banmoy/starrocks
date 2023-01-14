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

#include <gtest/gtest.h>

#include "gen_cpp/binlog.pb.h"
#include "storage/binlog_test_base.h"
#include "storage/binlog_util.h"
#include "testutil/assert.h"

namespace starrocks {

class BinlogManagerTest : public BinlogTestBase {};

TEST_F(BinlogManagerTest, test_ingestion) {
    int32_t start_key = 0;
    std::vector<RowsetSharedPtr> rowsets;
    rowsets.push_back(build_rowset(1, &start_key, {}));
    rowsets.push_back(build_rowset(2, &start_key, {}));
    BinlogConfig config;
    config.update(1, true, INT64_MAX, INT64_MAX);
    std::shared_ptr<BinlogManager> binlog_manager =
            std::make_shared<BinlogManager>(_binlog_file_dir, 10000, 20, DUP_KEYS, LZ4_FRAME, &config);
    for (const auto& rowset : rowsets) {
        ASSERT_OK(rowset->load());
        ASSERT_OK(binlog_manager->begin(rowset->start_version()));
        ASSERT_OK(binlog_manager->append_rowset(rowset));
        binlog_manager->publish(rowset->start_version());
    }
}

TEST_F(BinlogManagerTest, test_delete) {}

TEST_F(BinlogManagerTest, test_load) {
    int32_t start_key = 0;
    std::vector<RowsetSharedPtr> rowsets;
    rowsets.push_back(build_rowset(1, &start_key, {1000, 10, 20}));
    rowsets.push_back(build_rowset(2, &start_key, {5, 90, 1}));
    rowsets.push_back(build_rowset(3, &start_key, {10, 40, 10}));

    BinlogConfig config;
    config.update(1, true, INT64_MAX, INT64_MAX);
    std::shared_ptr<BinlogManager> binlog_manager =
            std::make_shared<BinlogManager>(_binlog_file_dir, 100, 20, DUP_KEYS, LZ4_FRAME, &config);
    for (const auto& rowset : rowsets) {
        ASSERT_OK(rowset->load());
        ASSERT_OK(binlog_manager->begin(rowset->start_version()));
        ASSERT_OK(binlog_manager->append_rowset(rowset));
        binlog_manager->publish(rowset->start_version());
    }

    std::map<int128_t, BinlogFileMetaPBSharedPtr>& file_metas = binlog_manager->get_file_metas();
    ASSERT_EQ(6, binlog_manager->num_binlog_files());
    int128_t lsn = BinlogUtil::get_lsn(0, 0);
    ASSERT_EQ(1, file_metas.count(lsn));
    BinlogFileMetaPBSharedPtr meta = file_metas[lsn];
    ASSERT_EQ(1, meta->id());
    ASSERT_EQ(1, meta->start_version());
    ASSERT_EQ(0, meta->start_seq_id());
    ASSERT_EQ(rowsets[0]->creation_time() * 1000000, meta->start_timestamp_in_us());
    ASSERT_EQ(1, meta->end_version());
    ASSERT_EQ(1009, meta->end_seq_id());
    ASSERT_EQ(rowsets[0]->creation_time() * 1000000, meta->end_timestamp_in_us());
    ASSERT_EQ(1, meta->num_pages());

    std::unordered_map<RowsetId, int32_t, HashOfRowsetId>& rowset_count_map = binlog_manager->get_rowset_count();

    std::unordered_map<RowsetId, RowsetSharedPtr, HashOfRowsetId>& rowset = binlog_manager->get_rowsets();
}

TEST_F(BinlogManagerTest, test_update_config) {}

} // namespace starrocks
