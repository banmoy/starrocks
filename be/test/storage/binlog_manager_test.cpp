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

#include "storage/binlog_manager.h"

#include <gtest/gtest.h>

#include "fs/fs_util.h"
#include "storage/binlog_test_base.h"
#include "testutil/assert.h"

namespace starrocks {

class BinlogManagerTest : public BinlogTestBase {
public:
    void SetUp() override {
        CHECK_OK(fs::remove_all(_binlog_file_dir));
        CHECK_OK(fs::create_directories(_binlog_file_dir));
        ASSIGN_OR_ABORT(_fs, FileSystem::CreateSharedFromString(_binlog_file_dir));
    }

    void TearDown() override { fs::remove_all(_binlog_file_dir); }

protected:
    std::shared_ptr<FileSystem> _fs;
    std::string _binlog_file_dir = "binlog_manager_test";
};

class MockRowsetFetcher : public RowsetFetcher {
public:
    RowsetSharedPtr get_rowset(int64_t uid) {
        auto iter = _rowsets.find(uid);
        if (iter == _rowsets.end()) {
            return nullptr;
        }
        return iter->second;
    }

    void add_rowset(int64_t uid, RowsetSharedPtr rowset) {
        _rowsets[uid] = rowset;
    }

private:
    std::unordered_map<int64_t, RowsetSharedPtr> _rowsets;
};

using LsnMap = std::map<int128_t, BinlogFileMetaPBPtr>;
using RowsetCountMap = std::unordered_map<int64_t, int32_t>;

TEST_F(BinlogManagerTest, test_ingestion_commit) {
    std::shared_ptr<MockRowsetFetcher> rowset_fetcher;
    std::shared_ptr<BinlogManager> binlog_manager =
            std::make_shared<BinlogManager>(_binlog_file_dir, 10000, 20, LZ4_FRAME, rowset_fetcher);
    ASSERT_EQ(0, binlog_manager->next_file_id());
    ASSERT_EQ(-1, binlog_manager->ingestion_version());
    ASSERT_TRUE(binlog_manager->build_result() == nullptr);

    int64_t version = 1;
    StatusOr<BinlogBuilderParamsPtr> status_or = binlog_manager->begin_ingestion(version);
    ASSERT_TRUE(status_or.ok());
    BinlogBuilderParamsPtr params = status_or.value();
    ASSERT_EQ(_binlog_file_dir, params->binlog_storage_path);
    ASSERT_EQ(10000, params->max_file_size);
    ASSERT_EQ(20, params->max_page_size);
    ASSERT_EQ(LZ4_FRAME, params->compression_type);
    ASSERT_EQ(0, params->start_file_id);
    ASSERT_TRUE(params->active_file_writer == nullptr);
    ASSERT_TRUE(params->active_file_meta == nullptr);

    ASSERT_EQ(version, binlog_manager->ingestion_version());
    ASSERT_TRUE(binlog_manager->build_result() == nullptr);

    BinlogBuildResultPtr result = std::make_shared<BinlogBuildResult>();
    result->params = params;
    result->next_file_id = 5;
    for (int64_t file_id = 0; file_id < result->next_file_id; file_id++) {
        BinlogFileMetaPBPtr file_meta = std::shared_ptr<BinlogFileMetaPB>();
        result->metas.push_back(file_meta);
        file_meta->set_id(file_id);
        file_meta->set_start_version(file_id);
        file_meta->set_start_seq_id(file_id);
        file_meta->add_rowsets(version);
    }
    result->active_writer = nullptr;

    binlog_manager->precommit_ingestion(version, result);
    ASSERT_EQ(result.get(), binlog_manager->build_result());

    binlog_manager->commit_ingestion(version);
    ASSERT_EQ(-1, binlog_manager->ingestion_version());
    ASSERT_TRUE(binlog_manager->build_result() == nullptr);
    ASSERT_EQ(result->next_file_id, binlog_manager->next_file_id());
    ASSERT_TRUE(binlog_manager->active_binlog_writer() == nullptr);

    LsnMap & lsn_map = binlog_manager->file_metas();
    RowsetCountMap& rowset_count_map = binlog_manager->rowset_count_map();
    ASSERT_EQ(result->metas.size(), lsn_map.size());
    for (auto& meta : result->metas) {
        int128_t lsn = BinlogUtil::get_lsn(meta->start_version(), meta->start_seq_id());
        ASSERT_EQ(1, lsn_map.count(lsn));
        ASSERT_EQ(meta.get(), lsn_map[lsn].get());
    }

    ASSERT_EQ(1, rowset_count_map.size());
    ASSERT_TRUE(rowset_count_map.count(rowset_id) == 1);
    ASSERT_TRUE(rowset_count_map[rowset_id] == 1);
}

TEST_F(BinlogManagerTest, test_ingestion_abort) {

}

TEST_F(BinlogManagerTest, test_ingestion_delete) {

}

} // namespace starrocks
