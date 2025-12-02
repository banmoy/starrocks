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

#include "storage/lake/table_schema_service.h"

#include <gtest/gtest.h>

#include "agent/master_info.h"
#include "fs/fs_util.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/lake_types.pb.h"
#include "runtime/mem_tracker.h"
#include "storage/lake/filenames.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/update_manager.h"
#include "storage/metadata_util.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "testutil/sync_point.h"
#include "util/defer_op.h"

namespace starrocks::lake {

using namespace starrocks;

class TableSchemaServiceTest : public testing::Test {
public:
    TableSchemaServiceTest() : _test_directory("test_table_schema_service") {
        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _location_provider = std::make_unique<FixedLocationProvider>(_test_directory);
        _update_manager = std::make_unique<UpdateManager>(_location_provider, _mem_tracker.get());
        _tablet_manager = std::make_unique<TabletManager>(_location_provider, _update_manager.get(), 1024 * 1024);
        _schema_service = _tablet_manager->table_schema_service();
    }

    void SetUp() override {
        clear_and_init_test_dir();
    }

    void TearDown() override {
        remove_test_dir_ignore_error();
    }

protected:
    void clear_and_init_test_dir() {
        (void)fs::remove_all(_test_directory);
        CHECK_OK(fs::create_directories(join_path(_test_directory, kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(join_path(_test_directory, kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(join_path(_test_directory, kTxnLogDirectoryName)));
    }

    void remove_test_dir_ignore_error() { (void)fs::remove_all(_test_directory); }

    TabletSchemaPtr create_test_schema(int64_t schema_id) {
        TabletSchemaPB schema_pb;
        schema_pb.set_id(schema_id);
        schema_pb.set_num_short_key_columns(1);
        schema_pb.set_keys_type(DUP_KEYS);
        schema_pb.set_num_rows_per_row_block(65535);
        schema_pb.set_schema_version(1);

        auto c0 = schema_pb.add_column();
        c0->set_unique_id(next_id());
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);

        auto c1 = schema_pb.add_column();
        c1->set_unique_id(next_id());
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);

        return TabletSchema::create(schema_pb);
    }

    TabletMetadataPtr create_test_tablet_metadata(int64_t tablet_id, int64_t schema_id) {
        auto metadata = std::make_shared<TabletMetadata>();
        metadata->set_id(tablet_id);
        metadata->set_version(1);

        auto schema = metadata->mutable_schema();
        schema->set_id(schema_id);
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(DUP_KEYS);
        schema->set_num_rows_per_row_block(65535);
        schema->set_schema_version(1);

        auto c0 = schema->add_column();
        c0->set_unique_id(next_id());
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);

        auto c1 = schema->add_column();
        c1->set_unique_id(next_id());
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);

        return metadata;
    }

    TabletMetadataPtr create_test_tablet_metadata_with_historical(int64_t tablet_id, int64_t current_schema_id,
                                                                  int64_t historical_schema_id) {
        auto metadata = create_test_tablet_metadata(tablet_id, current_schema_id);

        // Add historical schema
        TabletSchemaPB historical_schema;
        historical_schema.set_id(historical_schema_id);
        historical_schema.set_num_short_key_columns(1);
        historical_schema.set_keys_type(DUP_KEYS);
        historical_schema.set_num_rows_per_row_block(65535);
        historical_schema.set_schema_version(0);

        auto c0 = historical_schema.add_column();
        c0->set_unique_id(next_id());
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);

        (*metadata->mutable_historical_schemas())[historical_schema_id] = historical_schema;

        return metadata;
    }

    TableSchemaService::TableSchemaInfo create_schema_info(int64_t schema_id, int64_t db_id, int64_t table_id,
                                                           int64_t tablet_id) {
        return TableSchemaService::TableSchemaInfo{
                .schema_id = schema_id, .db_id = db_id, .table_id = table_id, .tablet_id = tablet_id};
    }

    TUniqueId create_test_query_id() {
        TUniqueId query_id;
        query_id.hi = next_id();
        query_id.lo = next_id();
        return query_id;
    }

    TNetworkAddress create_test_fe_address() {
        TNetworkAddress fe;
        fe.hostname = "127.0.0.1";
        fe.port = 9020;
        return fe;
    }

    void setup_sync_point_for_rpc_success(TBatchGetTableSchemaResponse* response_batch) {
        SyncPoint::GetInstance()->SetCallBack("TableSchemaService::_send_rpc::before_rpc",
                                              [](void* arg) { *(Status*)arg = Status::OK(); });
        SyncPoint::GetInstance()->SetCallBack("TableSchemaService::_send_rpc::after_rpc_callback",
                                              [response_batch](void* arg) {
                                                  auto* batch = static_cast<TBatchGetTableSchemaResponse*>(arg);
                                                  batch->__set_responses(std::vector<TGetTableSchemaResponse>{});
                                                  auto& resp = batch->responses.emplace_back();
                                                  resp.status.__set_status_code(TStatusCode::OK);
                                                  // Create a simple TTabletSchema
                                                  TTabletSchema t_schema;
                                                  t_schema.__set_id(100);
                                                  t_schema.__set_num_short_key_columns(1);
                                                  t_schema.__set_keys_type(TKeysType::DUP_KEYS);
                                                  TColumn col;
                                                  col.column_name = "c0";
                                                  col.column_type.type = TPrimitiveType::INT;
                                                  col.is_key = true;
                                                  col.is_nullable = false;
                                                  t_schema.columns.push_back(col);
                                                  resp.__set_schema(t_schema);
                                              });
    }

    void setup_sync_point_for_rpc_error(Status error_status) {
        SyncPoint::GetInstance()->SetCallBack("TableSchemaService::_send_rpc::before_rpc",
                                              [error_status](void* arg) { *(Status*)arg = error_status; });
    }

    void setup_sync_point_for_rpc_empty_response() {
        SyncPoint::GetInstance()->SetCallBack("TableSchemaService::_send_rpc::before_rpc",
                                              [](void* arg) { *(Status*)arg = Status::OK(); });
        SyncPoint::GetInstance()->SetCallBack("TableSchemaService::_send_rpc::after_rpc_callback",
                                              [](void* arg) {
                                                  auto* batch = static_cast<TBatchGetTableSchemaResponse*>(arg);
                                                  batch->__set_responses(std::vector<TGetTableSchemaResponse>{});
                                              });
    }

    void setup_sync_point_for_rpc_method_not_found() {
        SyncPoint::GetInstance()->SetCallBack("TableSchemaService::_send_rpc::before_rpc",
                                              [](void* arg) {
                                                  *(Status*)arg = Status::ThriftRpcError(
                                                          "Invalid method name 'getTableSchema'");
                                              });
    }

    std::string _test_directory;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<FixedLocationProvider> _location_provider;
    std::unique_ptr<UpdateManager> _update_manager;
    std::unique_ptr<TabletManager> _tablet_manager;
    TableSchemaService* _schema_service;
};

// Category 1: Local Schema Retrieval Tests

TEST_F(TableSchemaServiceTest, get_local_schema_from_global_cache) {
    int64_t schema_id = next_id();
    auto schema = create_test_schema(schema_id);
    _tablet_manager->cache_global_schema(schema);

    auto result = _schema_service->get_load_schema(create_schema_info(schema_id, 100, 101, 1000), 1);
    ASSERT_OK(result);
    ASSERT_EQ(result.value()->id(), schema_id);
}

TEST_F(TableSchemaServiceTest, get_local_schema_from_tablet_metadata_current) {
    int64_t tablet_id = next_id();
    int64_t schema_id = next_id();
    auto metadata = create_test_tablet_metadata(tablet_id, schema_id);
    ASSERT_OK(_tablet_manager->put_tablet_metadata(*metadata));

    auto result = _schema_service->get_load_schema(create_schema_info(schema_id, 100, 101, tablet_id), 1, metadata);
    ASSERT_OK(result);
    ASSERT_EQ(result.value()->id(), schema_id);
}

TEST_F(TableSchemaServiceTest, get_local_schema_from_tablet_metadata_historical) {
    int64_t tablet_id = next_id();
    int64_t current_schema_id = next_id();
    int64_t historical_schema_id = next_id();
    auto metadata = create_test_tablet_metadata_with_historical(tablet_id, current_schema_id, historical_schema_id);
    ASSERT_OK(_tablet_manager->put_tablet_metadata(*metadata));

    auto result = _schema_service->get_load_schema(
            create_schema_info(historical_schema_id, 100, 101, tablet_id), 1, metadata);
    ASSERT_OK(result);
    ASSERT_EQ(result.value()->id(), historical_schema_id);
}

TEST_F(TableSchemaServiceTest, get_local_schema_not_found) {
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    auto metadata = create_test_tablet_metadata(tablet_id, schema_id + 100); // Different schema_id

    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("TableSchemaService::_send_rpc::before_rpc");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    setup_sync_point_for_rpc_method_not_found();

    auto result = _schema_service->get_load_schema(create_schema_info(schema_id, 100, 101, tablet_id), 1, metadata);
    // Should fallback to tablet schema
    ASSERT_OK(result);
}

TEST_F(TableSchemaServiceTest, get_local_schema_caches_after_retrieval) {
    int64_t tablet_id = next_id();
    int64_t schema_id = next_id();
    auto metadata = create_test_tablet_metadata(tablet_id, schema_id);
    ASSERT_OK(_tablet_manager->put_tablet_metadata(*metadata));

    // First call should retrieve from metadata
    auto result1 = _schema_service->get_load_schema(create_schema_info(schema_id, 100, 101, tablet_id), 1, metadata);
    ASSERT_OK(result1);

    // Second call should retrieve from cache
    auto cached_schema = _tablet_manager->get_global_schema(schema_id);
    ASSERT_NE(cached_schema, nullptr);
    ASSERT_EQ(cached_schema->id(), schema_id);
}

// Category 2: Load Schema Retrieval Tests

TEST_F(TableSchemaServiceTest, get_load_schema_from_local_cache) {
    int64_t schema_id = next_id();
    auto schema = create_test_schema(schema_id);
    _tablet_manager->cache_global_schema(schema);

    auto result = _schema_service->get_load_schema(create_schema_info(schema_id, 100, 101, 1000), 1);
    ASSERT_OK(result);
    ASSERT_EQ(result.value()->id(), schema_id);
}

TEST_F(TableSchemaServiceTest, get_load_schema_from_tablet_metadata) {
    int64_t tablet_id = next_id();
    int64_t schema_id = next_id();
    auto metadata = create_test_tablet_metadata(tablet_id, schema_id);
    ASSERT_OK(_tablet_manager->put_tablet_metadata(*metadata));

    auto result = _schema_service->get_load_schema(create_schema_info(schema_id, 100, 101, tablet_id), 1, metadata);
    ASSERT_OK(result);
    ASSERT_EQ(result.value()->id(), schema_id);
}

TEST_F(TableSchemaServiceTest, get_load_schema_fallback_when_fe_not_supported) {
    int64_t tablet_id = next_id();
    int64_t schema_id = next_id();
    auto metadata = create_test_tablet_metadata(tablet_id, schema_id);
    ASSERT_OK(_tablet_manager->put_tablet_metadata(*metadata));
    ASSERT_OK(_tablet_manager->get_tablet(tablet_id));

    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("TableSchemaService::_send_rpc::before_rpc");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    setup_sync_point_for_rpc_method_not_found();

    auto result = _schema_service->get_load_schema(create_schema_info(schema_id, 100, 101, tablet_id), 1);
    ASSERT_OK(result);
    ASSERT_EQ(result.value()->id(), schema_id);
}

// Category 3: Scan Schema Retrieval Tests

TEST_F(TableSchemaServiceTest, get_scan_schema_from_local_cache) {
    int64_t schema_id = next_id();
    auto schema = create_test_schema(schema_id);
    _tablet_manager->cache_global_schema(schema);

    auto query_id = create_test_query_id();
    auto fe = create_test_fe_address();
    auto result = _schema_service->get_scan_schema(create_schema_info(schema_id, 100, 101, 1000), query_id, fe, nullptr);
    ASSERT_OK(result);
    ASSERT_EQ(result.value()->id(), schema_id);
}

TEST_F(TableSchemaServiceTest, get_scan_schema_from_tablet_metadata) {
    int64_t tablet_id = next_id();
    int64_t schema_id = next_id();
    auto metadata = create_test_tablet_metadata(tablet_id, schema_id);
    ASSERT_OK(_tablet_manager->put_tablet_metadata(*metadata));

    auto query_id = create_test_query_id();
    auto fe = create_test_fe_address();
    auto result = _schema_service->get_scan_schema(create_schema_info(schema_id, 100, 101, tablet_id), query_id, fe,
                                                   metadata);
    ASSERT_OK(result);
    ASSERT_EQ(result.value()->id(), schema_id);
}

// Category 7: Fallback Mechanism Tests

TEST_F(TableSchemaServiceTest, fallback_to_tablet_schema_by_id_success) {
    int64_t tablet_id = next_id();
    int64_t schema_id = next_id();
    auto metadata = create_test_tablet_metadata(tablet_id, schema_id);
    ASSERT_OK(_tablet_manager->put_tablet_metadata(*metadata));
    ASSERT_OK(_tablet_manager->get_tablet(tablet_id));

    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("TableSchemaService::_send_rpc::before_rpc");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    setup_sync_point_for_rpc_method_not_found();

    auto result = _schema_service->get_load_schema(create_schema_info(schema_id, 100, 101, tablet_id), 1);
    ASSERT_OK(result);
    ASSERT_EQ(result.value()->id(), schema_id);
}

} // namespace starrocks::lake

