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

#include "column/datum_tuple.h"
#include "fs/fs_util.h"
#include "gtest/gtest.h"
#include "storage/binlog_manager.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/tablet_schema_helper.h"
#include "testutil/assert.h"

namespace starrocks {

class BinlogTestBase : public testing::Test {
public:
    void SetUp() override {
        CHECK_OK(fs::remove_all(_binlog_file_dir));
        CHECK_OK(fs::create_directories(_binlog_file_dir));
        create_tablet_schema();
    }

    void TearDown() override { fs::remove_all(_binlog_file_dir); }

protected:
    ColumnPB create_column_pb(int32_t id, string name, string type, int length, bool is_key) {
        ColumnPB col;
        col.set_unique_id(id);
        col.set_name(name);
        col.set_type(type);
        col.set_is_key(is_key);
        col.set_is_nullable(false);
        col.set_length(length);
        col.set_index_length(4);
        col.set_is_bf_column(false);
        col.set_has_bitmap_index(false);
        return col;
    }

    void create_tablet_schema() {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(DUP_KEYS);
        schema_pb.set_num_short_key_columns(2);
        schema_pb.set_num_rows_per_row_block(5);
        schema_pb.set_next_column_unique_id(4);

        auto* col1 = schema_pb.add_column();
        *col1 = create_column_pb(1, "col1", "INT", 4, true);
        auto* col2 = schema_pb.add_column();
        *col2 = create_column_pb(2, "col2", "INT", 4, true);
        auto* col3 = schema_pb.add_column();
        *col3 = create_column_pb(3, "col3", "VARCHAR", 20, false);

        _tablet_schema = std::make_unique<TabletSchema>(schema_pb);
        _schema = ChunkHelper::convert_schema(*_tablet_schema);
    }

    VectorizedFieldPtr make_field(ColumnId cid, const std::string& cname, LogicalType type) {
        return std::make_shared<VectorizedField>(cid, cname, get_type_info(type), false);
    }

    void create_rowset_writer_context(RowsetId& rowset_id, int64_t version,
                                      RowsetWriterContext* rowset_writer_context) {
        rowset_writer_context->rowset_id = rowset_id;
        rowset_writer_context->tablet_id = _tablet_id;
        rowset_writer_context->tablet_schema_hash = 1111;
        rowset_writer_context->partition_id = 10;
        rowset_writer_context->version = Version(version, 0);
        rowset_writer_context->rowset_path_prefix = _binlog_file_dir;
        rowset_writer_context->rowset_state = VISIBLE;
        rowset_writer_context->tablet_schema = _tablet_schema.get();
        rowset_writer_context->writer_type = kHorizontal;
    }

    void build_segment(int32_t start_key, int32_t num_rows, RowsetWriter* rowset_writer, SegmentPB* seg_info) {
        std::vector<uint32_t> column_indexes{0, 1, 2};
        auto chunk = ChunkHelper::new_chunk(_schema, num_rows);
        for (int i = start_key; i < num_rows + start_key; i++) {
            auto& cols = chunk->columns();
            cols[0]->append_datum(Datum(static_cast<int32_t>(i)));
            cols[1]->append_datum(Datum(static_cast<int32_t>(i)));
            cols[2]->append_datum(Datum(std::to_string(i)));
        }
        ASSERT_OK(rowset_writer->flush_chunk(*chunk, seg_info));
    }

    RowsetSharedPtr build_rowset(int version, int32_t* start_key, std::vector<int32_t> rows_per_segment) {
        RowsetId rowset_id;
        rowset_id.init(2, ++_next_row_id, 2, 3);
        RowsetWriterContext writer_context;
        create_rowset_writer_context(rowset_id, version, &writer_context);
        std::unique_ptr<RowsetWriter> rowset_writer;
        RowsetFactory::create_rowset_writer(writer_context, &rowset_writer);

        int32_t total_rows = 0;
        std::vector<std::unique_ptr<SegmentPB>> seg_infos;
        for (int32_t num_rows : rows_per_segment) {
            seg_infos.emplace_back(std::make_unique<SegmentPB>());
            build_segment(*start_key, num_rows, rowset_writer.get(), seg_infos.back().get());
            *start_key += num_rows;
            total_rows += num_rows;
        }

        RowsetSharedPtr rowset = rowset_writer->build().value();
        return rowset;
    }

protected:
    int64_t _next_row_id;
    std::unique_ptr<TabletSchema> _tablet_schema;
    VectorizedSchema _schema;
    int64_t _tablet_id = 100;
    std::string _binlog_file_dir = "binlog_reader_test";
};

} // namespace starrocks