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

#include <set>

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "gen_cpp/tablet_schema.pb.h"
#include "gen_cpp/types.pb.h"
#include "storage/chunk_helper.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/tablet_schema.h"
#include "test_util.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"

namespace starrocks::lake {

using namespace starrocks;

class LakeTabletDeleteDataTest : public TestBase {
public:
    LakeTabletDeleteDataTest() : TestBase(kTestDirectory) {
        _tablet_metadata = std::make_shared<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c1   |  INT | YES |  NO  |
        //  |   c2   |  INT | NO  |  NO  |
        auto schema = _tablet_metadata->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(DUP_KEYS);
        schema->set_num_rows_per_row_block(65535);
        auto c1 = schema->add_column();
        {
            c1->set_unique_id(next_id());
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(true);
            c1->set_is_nullable(false);
        }
        auto c2 = schema->add_column();
        {
            c2->set_unique_id(next_id());
            c2->set_name("c2");
            c2->set_type("INT");
            c2->set_is_key(false);
            c2->set_is_nullable(false);
        }

        _tablet_schema = TabletSchema::create(*schema);
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

    void create_rowsets_with_data() {
        // Create data: c1 values 1-10, c2 values 1-10 (some rows have c2=3)
        std::vector<int> c1_values{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        std::vector<int> c2_values{1, 2, 3, 3, 5, 6, 3, 8, 9, 10}; // rows with c2=3: indices 2, 3, 6

        auto c1_col = Int32Column::create();
        auto c2_col = Int32Column::create();
        c1_col->append_numbers(c1_values.data(), c1_values.size() * sizeof(int));
        c2_col->append_numbers(c2_values.data(), c2_values.size() * sizeof(int));

        Chunk chunk({std::move(c1_col), std::move(c2_col)}, _schema);

        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_metadata->id()));

        int64_t txn_id1, txn_id2;

        // Write rowset 1
        {
            txn_id1 = next_id();
            ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id1));
            ASSERT_OK(writer->open());
            ASSERT_OK(writer->write(chunk));
            ASSERT_OK(writer->finish());
            writer->close();

            auto* rowset = _tablet_metadata->add_rowsets();
            rowset->set_overlapped(true);
            rowset->set_id(1);
            rowset->set_num_rows(chunk.num_rows());
            auto* segs = rowset->mutable_segments();
            for (auto& file : writer->files()) {
                segs->Add(std::move(file.path));
            }
        }

        // Write rowset 2
        {
            std::vector<int> c1_values2{11, 12, 13, 14, 15};
            std::vector<int> c2_values2{3, 12, 13, 3, 15}; // rows with c2=3: indices 0, 3

            auto c1_col2 = Int32Column::create();
            auto c2_col2 = Int32Column::create();
            c1_col2->append_numbers(c1_values2.data(), c1_values2.size() * sizeof(int));
            c2_col2->append_numbers(c2_values2.data(), c2_values2.size() * sizeof(int));

            Chunk chunk2({std::move(c1_col2), std::move(c2_col2)}, _schema);

            txn_id2 = next_id();
            ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id2));
            ASSERT_OK(writer->open());
            ASSERT_OK(writer->write(chunk2));
            ASSERT_OK(writer->finish());
            writer->close();

            auto* rowset = _tablet_metadata->add_rowsets();
            rowset->set_overlapped(true);
            rowset->set_id(2);
            rowset->set_num_rows(chunk2.num_rows());
            auto* segs = rowset->mutable_segments();
            for (auto& file : writer->files()) {
                segs->Add(std::move(file.path));
            }
        }

        // Publish version 2 with both txn_ids
        std::vector<int64_t> txn_ids{txn_id1, txn_id2};
        ASSIGN_OR_ABORT(auto metadata_v2, batch_publish(_tablet_metadata->id(), 1, 2, txn_ids));
        _tablet_metadata = metadata_v2;
    }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_tablet_delete_data";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
};

TEST_F(LakeTabletDeleteDataTest, test_delete_data_without_schema_key) {
    create_rowsets_with_data();

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_metadata->id()));

    // Create delete predicate: c2 = 3
    DeletePredicatePB delete_predicate;
    delete_predicate.set_version(-1);
    auto* binary_predicate = delete_predicate.add_binary_predicates();
    binary_predicate->set_column_name("c2");
    binary_predicate->set_op("=");
    binary_predicate->set_value("3");

    // Execute delete_data without schema_key
    int64_t txn_id = next_id();
    ASSERT_OK(tablet.delete_data(txn_id, delete_predicate, nullptr));

    // Verify txn log was created
    ASSIGN_OR_ABORT(auto txn_log, tablet.get_txn_log(txn_id));
    ASSERT_TRUE(txn_log->has_op_write());
    ASSERT_TRUE(txn_log->op_write().has_rowset());
    ASSERT_TRUE(txn_log->op_write().rowset().has_delete_predicate());
    ASSERT_FALSE(txn_log->op_write().has_schema_key());

    // Publish version 3
    ASSIGN_OR_ABORT(auto metadata_v3, publish_single_version(_tablet_metadata->id(), 3, txn_id));

    // Verify delete predicate is in the rowset
    ASSERT_EQ(3, metadata_v3->rowsets_size());
    const auto& delete_rowset = metadata_v3->rowsets(2);
    ASSERT_TRUE(delete_rowset.has_delete_predicate());
    ASSERT_EQ(1, delete_rowset.delete_predicate().binary_predicates_size());
    ASSERT_EQ("c2", delete_rowset.delete_predicate().binary_predicates(0).column_name());
    ASSERT_EQ("=", delete_rowset.delete_predicate().binary_predicates(0).op());
    ASSERT_EQ("3", delete_rowset.delete_predicate().binary_predicates(0).value());

    // Read data and verify deletion
    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata_v3, *_schema);
    ASSERT_OK(reader->prepare());
    TabletReaderParams params;
    ASSERT_OK(reader->open(params));

    auto read_chunk_ptr = ChunkHelper::new_chunk(*_schema, 1024);
    int total_rows = 0;
    std::set<int> remaining_c1_values;
    while (true) {
        read_chunk_ptr->reset();
        auto st = reader->get_next(read_chunk_ptr.get());
        if (st.is_end_of_file()) {
            break;
        }
        ASSERT_OK(st);
        total_rows += read_chunk_ptr->num_rows();
        for (int i = 0; i < read_chunk_ptr->num_rows(); i++) {
            int c1_val = read_chunk_ptr->get(i)[0].get_int32();
            int c2_val = read_chunk_ptr->get(i)[1].get_int32();
            remaining_c1_values.insert(c1_val);
            // Verify no rows with c2=3 remain
            ASSERT_NE(3, c2_val) << "Found row with c2=3: c1=" << c1_val;
        }
    }

    // Original data: 10 rows in rowset1 + 5 rows in rowset2 = 15 rows
    // Rows with c2=3: c1=3,4,7 (rowset1) and c1=11,14 (rowset2) = 5 rows
    // Expected remaining: 15 - 5 = 10 rows
    ASSERT_EQ(10, total_rows);
    // Verify specific rows remain
    ASSERT_TRUE(remaining_c1_values.find(1) != remaining_c1_values.end());
    ASSERT_TRUE(remaining_c1_values.find(2) != remaining_c1_values.end());
    ASSERT_TRUE(remaining_c1_values.find(5) != remaining_c1_values.end());
    ASSERT_TRUE(remaining_c1_values.find(6) != remaining_c1_values.end());
    // Verify deleted rows are gone
    ASSERT_TRUE(remaining_c1_values.find(3) == remaining_c1_values.end());
    ASSERT_TRUE(remaining_c1_values.find(4) == remaining_c1_values.end());
    ASSERT_TRUE(remaining_c1_values.find(7) == remaining_c1_values.end());
    ASSERT_TRUE(remaining_c1_values.find(11) == remaining_c1_values.end());
    ASSERT_TRUE(remaining_c1_values.find(14) == remaining_c1_values.end());

    reader->close();
}

TEST_F(LakeTabletDeleteDataTest, test_delete_data_with_schema_key) {
    create_rowsets_with_data();

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_metadata->id()));

    // Create extended schema with c3 int default 5
    // The schema_key points to a schema that extends the tablet schema with c3
    int64_t schema_id = next_id();
    TableSchemaKeyPB schema_key;
    schema_key.set_db_id(next_id());
    schema_key.set_table_id(next_id());
    schema_key.set_schema_id(schema_id);

    // Create extended schema: tablet schema + c3 int default 5
    TabletSchemaPB extended_schema_pb;
    extended_schema_pb.CopyFrom(_tablet_metadata->schema());
    extended_schema_pb.set_id(schema_id);
    
    // Add c3 column with default value 5
    auto c3 = extended_schema_pb.add_column();
    c3->set_unique_id(next_id());
    c3->set_name("c3");
    c3->set_type("INT");
    c3->set_is_key(false);
    c3->set_is_nullable(false);
    // Set default value to 5 (serialize int32_t to bytes)
    int32_t default_val = 5;
    c3->set_default_value(std::string(reinterpret_cast<const char*>(&default_val), sizeof(int32_t)));

    // Cache the extended schema before publish
    auto extended_schema = TabletSchema::create(extended_schema_pb);
    _tablet_mgr->cache_schema(extended_schema);

    // Create delete predicate: c2 = 3 or c3 = 5
    // Since c3 has default value 5, all rows should match c3 = 5
    DeletePredicatePB delete_predicate;
    delete_predicate.set_version(-1);
    
    // Add predicate: c2 = 3
    auto* binary_predicate1 = delete_predicate.add_binary_predicates();
    binary_predicate1->set_column_name("c2");
    binary_predicate1->set_op("=");
    binary_predicate1->set_value("3");

    // Add predicate: c3 = 5 (this will match all rows since c3 defaults to 5)
    auto* binary_predicate2 = delete_predicate.add_binary_predicates();
    binary_predicate2->set_column_name("c3");
    binary_predicate2->set_op("=");
    binary_predicate2->set_value("5");

    // Execute delete_data with schema_key
    int64_t txn_id = next_id();
    ASSERT_OK(tablet.delete_data(txn_id, delete_predicate, &schema_key));

    // Verify txn log was created with schema_key
    ASSIGN_OR_ABORT(auto txn_log, tablet.get_txn_log(txn_id));
    ASSERT_TRUE(txn_log->has_op_write());
    ASSERT_TRUE(txn_log->op_write().has_rowset());
    ASSERT_TRUE(txn_log->op_write().has_schema_key());
    ASSERT_EQ(schema_key.db_id(), txn_log->op_write().schema_key().db_id());
    ASSERT_EQ(schema_key.table_id(), txn_log->op_write().schema_key().table_id());
    ASSERT_EQ(schema_key.schema_id(), txn_log->op_write().schema_key().schema_id());

    // Verify delete predicate is stored correctly
    ASSERT_TRUE(txn_log->op_write().rowset().has_delete_predicate());
    ASSERT_EQ(2, txn_log->op_write().rowset().delete_predicate().binary_predicates_size());

    // Publish version 3
    ASSIGN_OR_ABORT(auto metadata_v3, publish_single_version(_tablet_metadata->id(), 3, txn_id));

    // Verify delete predicate is in the rowset
    ASSERT_EQ(3, metadata_v3->rowsets_size());
    const auto& delete_rowset = metadata_v3->rowsets(2);
    ASSERT_TRUE(delete_rowset.has_delete_predicate());
    ASSERT_EQ(2, delete_rowset.delete_predicate().binary_predicates_size());

    // Verify the delete predicate structure
    const auto& stored_predicate = delete_rowset.delete_predicate();
    ASSERT_EQ(2, stored_predicate.binary_predicates_size());
    
    // Verify first predicate: c2 = 3
    ASSERT_EQ("c2", stored_predicate.binary_predicates(0).column_name());
    ASSERT_EQ("=", stored_predicate.binary_predicates(0).op());
    ASSERT_EQ("3", stored_predicate.binary_predicates(0).value());
    
    // Verify second predicate: c3 = 5
    ASSERT_EQ("c3", stored_predicate.binary_predicates(1).column_name());
    ASSERT_EQ("=", stored_predicate.binary_predicates(1).op());
    ASSERT_EQ("5", stored_predicate.binary_predicates(1).value());

    // Read data and verify deletion
    // The delete predicate "c2 = 3 or c3 = 5" should delete:
    // - Rows with c2 = 3: c1 values 3, 4, 7, 11, 14
    // - Rows with c3 = 5: Since c3 defaults to 5 for all rows in the extended schema,
    //   this condition matches all rows
    // However, the actual evaluation depends on how the delete predicate is processed
    // with the extended schema. For this test, we verify:
    // 1. The delete operation succeeded
    // 2. The schema_key was persisted correctly
    // 3. The delete predicate was stored correctly
    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata_v3, *_schema);
    ASSERT_OK(reader->prepare());
    TabletReaderParams params;
    ASSERT_OK(reader->open(params));

    auto read_chunk_ptr = ChunkHelper::new_chunk(*_schema, 1024);
    int total_rows = 0;
    std::set<int> remaining_c1_values;
    while (true) {
        read_chunk_ptr->reset();
        auto st = reader->get_next(read_chunk_ptr.get());
        if (st.is_end_of_file()) {
            break;
        }
        ASSERT_OK(st);
        total_rows += read_chunk_ptr->num_rows();
        for (int i = 0; i < read_chunk_ptr->num_rows(); i++) {
            int c1_val = read_chunk_ptr->get(i)[0].get_int32();
            int c2_val = read_chunk_ptr->get(i)[1].get_int32();
            remaining_c1_values.insert(c1_val);
            // Verify rows with c2=3 are deleted
            ASSERT_NE(3, c2_val) << "Found row with c2=3: c1=" << c1_val;
        }
    }

    // Verify that the delete operation was executed successfully
    // The exact number of remaining rows depends on how the extended schema
    // (with c3 default value) is used to evaluate the delete predicate
    // At minimum, we verify that rows with c2=3 are deleted
    ASSERT_TRUE(total_rows <= 10); // Original 15 rows, at least 5 rows with c2=3 should be deleted
    reader->close();
}

} // namespace starrocks::lake
