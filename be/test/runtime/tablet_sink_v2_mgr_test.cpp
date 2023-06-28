// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/tablet_info.h"
#include "gtest/gtest_pred_impl.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/vtablet_sink_v2_mgr.h"

namespace doris {

static const uint32_t MAX_PATH_LEN = 1024;

static void create_tablet_request(int64_t tablet_id, int32_t schema_hash,
                                  TCreateTabletReq* request) {
    request->tablet_id = tablet_id;
    request->__set_version(1);
    request->tablet_schema.schema_hash = schema_hash;
    request->tablet_schema.short_key_column_count = 6;
    request->tablet_schema.keys_type = TKeysType::AGG_KEYS;
    request->tablet_schema.storage_type = TStorageType::COLUMN;
    request->__set_storage_format(TStorageFormat::V2);

    TColumn k1;
    k1.column_name = "k1";
    k1.__set_is_key(true);
    k1.column_type.type = TPrimitiveType::TINYINT;
    request->tablet_schema.columns.push_back(k1);

    TColumn k2;
    k2.column_name = "k2";
    k2.__set_is_key(true);
    k2.column_type.type = TPrimitiveType::SMALLINT;
    request->tablet_schema.columns.push_back(k2);

    TColumn k3;
    k3.column_name = "k3";
    k3.__set_is_key(true);
    k3.column_type.type = TPrimitiveType::INT;
    request->tablet_schema.columns.push_back(k3);

    TColumn k4;
    k4.column_name = "k4";
    k4.__set_is_key(true);
    k4.column_type.type = TPrimitiveType::BIGINT;
    request->tablet_schema.columns.push_back(k4);

    TColumn k5;
    k5.column_name = "k5";
    k5.__set_is_key(true);
    k5.column_type.type = TPrimitiveType::LARGEINT;
    request->tablet_schema.columns.push_back(k5);

    TColumn k6;
    k6.column_name = "k6";
    k6.__set_is_key(true);
    k6.column_type.type = TPrimitiveType::DATE;
    request->tablet_schema.columns.push_back(k6);

    TColumn k7;
    k7.column_name = "k7";
    k7.__set_is_key(true);
    k7.column_type.type = TPrimitiveType::DATETIME;
    request->tablet_schema.columns.push_back(k7);

    TColumn k8;
    k8.column_name = "k8";
    k8.__set_is_key(true);
    k8.column_type.type = TPrimitiveType::CHAR;
    k8.column_type.__set_len(4);
    request->tablet_schema.columns.push_back(k8);

    TColumn k9;
    k9.column_name = "k9";
    k9.__set_is_key(true);
    k9.column_type.type = TPrimitiveType::VARCHAR;
    k9.column_type.__set_len(65);
    request->tablet_schema.columns.push_back(k9);

    TColumn k10;
    k10.column_name = "k10";
    k10.__set_is_key(true);
    k10.column_type.type = TPrimitiveType::DECIMALV2;
    k10.column_type.__set_precision(6);
    k10.column_type.__set_scale(3);
    request->tablet_schema.columns.push_back(k10);

    TColumn k11;
    k11.column_name = "k11";
    k11.__set_is_key(true);
    k11.column_type.type = TPrimitiveType::DATEV2;
    request->tablet_schema.columns.push_back(k11);

    TColumn v1;
    v1.column_name = "v1";
    v1.__set_is_key(false);
    v1.column_type.type = TPrimitiveType::TINYINT;
    v1.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v1);

    TColumn v2;
    v2.column_name = "v2";
    v2.__set_is_key(false);
    v2.column_type.type = TPrimitiveType::SMALLINT;
    v2.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v2);

    TColumn v3;
    v3.column_name = "v3";
    v3.__set_is_key(false);
    v3.column_type.type = TPrimitiveType::INT;
    v3.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v3);

    TColumn v4;
    v4.column_name = "v4";
    v4.__set_is_key(false);
    v4.column_type.type = TPrimitiveType::BIGINT;
    v4.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v4);

    TColumn v5;
    v5.column_name = "v5";
    v5.__set_is_key(false);
    v5.column_type.type = TPrimitiveType::LARGEINT;
    v5.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v5);

    TColumn v6;
    v6.column_name = "v6";
    v6.__set_is_key(false);
    v6.column_type.type = TPrimitiveType::DATE;
    v6.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(v6);

    TColumn v7;
    v7.column_name = "v7";
    v7.__set_is_key(false);
    v7.column_type.type = TPrimitiveType::DATETIME;
    v7.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(v7);

    TColumn v8;
    v8.column_name = "v8";
    v8.__set_is_key(false);
    v8.column_type.type = TPrimitiveType::CHAR;
    v8.column_type.__set_len(4);
    v8.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(v8);

    TColumn v9;
    v9.column_name = "v9";
    v9.__set_is_key(false);
    v9.column_type.type = TPrimitiveType::VARCHAR;
    v9.column_type.__set_len(65);
    v9.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(v9);

    TColumn v10;
    v10.column_name = "v10";
    v10.__set_is_key(false);
    v10.column_type.type = TPrimitiveType::DECIMALV2;
    v10.column_type.__set_precision(6);
    v10.column_type.__set_scale(3);
    v10.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v10);

    TColumn v11;
    v11.column_name = "v11";
    v11.__set_is_key(false);
    v11.column_type.type = TPrimitiveType::DATEV2;
    v11.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(v11);
}

static TDescriptorTable create_descriptor_tablet() {
    TDescriptorTableBuilder dtb;
    TTupleDescriptorBuilder tuple_builder;

    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_TINYINT).column_name("k1").column_pos(0).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_SMALLINT).column_name("k2").column_pos(1).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_INT).column_name("k3").column_pos(2).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("k4").column_pos(3).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_LARGEINT).column_name("k5").column_pos(4).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_DATE).column_name("k6").column_pos(5).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_DATETIME).column_name("k7").column_pos(6).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().string_type(4).column_name("k8").column_pos(7).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().string_type(65).column_name("k9").column_pos(8).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().decimal_type(6, 3).column_name("k10").column_pos(9).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_DATEV2).column_name("k11").column_pos(10).build());

    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_TINYINT)
                                   .column_name("v1")
                                   .column_pos(11)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_SMALLINT)
                                   .column_name("v2")
                                   .column_pos(12)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_INT)
                                   .column_name("v3")
                                   .column_pos(13)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_BIGINT)
                                   .column_name("v4")
                                   .column_pos(14)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_LARGEINT)
                                   .column_name("v5")
                                   .column_pos(15)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_DATE)
                                   .column_name("v6")
                                   .column_pos(16)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_DATETIME)
                                   .column_name("v7")
                                   .column_pos(17)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .string_type(4)
                                   .column_name("v8")
                                   .column_pos(18)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .string_type(65)
                                   .column_name("v9")
                                   .column_pos(19)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .decimal_type(6, 3)
                                   .column_name("v10")
                                   .column_pos(20)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_DATEV2)
                                   .column_name("v11")
                                   .column_pos(21)
                                   .nullable(false)
                                   .build());
    tuple_builder.build(&dtb);
    return dtb.desc_tbl();
}

class VOlapTabletSinkV2MgrTest : public testing::Test {
public:
    ~VOlapTabletSinkV2MgrTest() {
        delete _mgr;
    }
protected:
    virtual void SetUp() {
        // set path
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        config::storage_root_path = std::string(buffer) + "/data_test";
        io::global_local_filesystem()->delete_and_create_directory(config::storage_root_path);
        std::vector<StorePath> paths;
        paths.emplace_back(config::storage_root_path, -1);

        doris::EngineOptions options;
        options.store_paths = paths;
        Status s = doris::StorageEngine::open(options, &_engine);
        ExecEnv* exec_env = doris::ExecEnv::GetInstance();
        exec_env->set_storage_engine(_engine);
        _engine->start_bg_threads();
    }

    virtual void TearDown() {
        if (_engine != nullptr) {
            _engine->stop();
            delete _engine;
            _engine = nullptr;
        }
        if (_mgr != nullptr) {
            delete _mgr;
            _mgr = nullptr;
        }
        EXPECT_EQ(system("rm -rf ./data_test"), 0);
        io::global_local_filesystem()->delete_directory(std::string(getenv("DORIS_HOME")) + "/" +
                                                        UNUSED_PREFIX);
    }

    StorageEngine* _engine = nullptr;
    VOlapTableSinkV2Mgr* _mgr = new VOlapTableSinkV2Mgr();
};

TEST_F(VOlapTabletSinkV2MgrTest, handle_memtable_flush_test) {
    std::unique_ptr<MemTrackerLimiter> mem_tracker = std::make_unique<MemTrackerLimiter>(
            MemTrackerLimiter::Type::LOAD, "VOlapTableSinkV2TestMgr");

    TCreateTabletReq request;
    create_tablet_request(10000, 270068372, &request);
    Status res = _engine->create_tablet(request);
    ASSERT_TRUE(res.ok());

    TDescriptorTable tdesc_tbl = create_descriptor_tablet();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl);
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
    OlapTableSchemaParam param;

    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    WriteRequest write_req = {10000,   270068372,  WriteType::LOAD,        20002, 30002,
                              load_id, tuple_desc, &(tuple_desc->slots()), false, &param};
    DeltaWriter* delta_writer = nullptr;
    std::unique_ptr<RuntimeProfile> profile;
    profile = std::make_unique<RuntimeProfile>("VOlapTableSinkV2TestMgr");
    DeltaWriter::open(&write_req, &delta_writer, profile.get(), TUniqueId());
    ASSERT_NE(delta_writer, nullptr);

    vectorized::Block block;
    for (const auto& slot_desc : tuple_desc->slots()) {
        block.insert(vectorized::ColumnWithTypeAndName(slot_desc->get_empty_mutable_column(),
                                                       slot_desc->get_data_type_ptr(),
                                                       slot_desc->col_name()));
    }
    auto columns = block.mutate_columns();
    {
        int8_t k1 = -127;
        columns[0]->insert_data((const char*)&k1, sizeof(k1));

        int16_t k2 = -32767;
        columns[1]->insert_data((const char*)&k2, sizeof(k2));

        int32_t k3 = -2147483647;
        columns[2]->insert_data((const char*)&k3, sizeof(k3));

        int64_t k4 = -9223372036854775807L;
        columns[3]->insert_data((const char*)&k4, sizeof(k4));

        int128_t k5 = -90000;
        columns[4]->insert_data((const char*)&k5, sizeof(k5));

        vectorized::VecDateTimeValue k6;
        k6.from_date_str("2048-11-10", 10);
        auto k6_int = k6.to_int64();
        columns[5]->insert_data((const char*)&k6_int, sizeof(k6_int));

        vectorized::VecDateTimeValue k7;
        k7.from_date_str("2636-08-16 19:39:43", 19);
        auto k7_int = k7.to_int64();
        columns[6]->insert_data((const char*)&k7_int, sizeof(k7_int));

        columns[7]->insert_data("abcd", 4);
        columns[8]->insert_data("abcde", 5);

        DecimalV2Value decimal_value;
        decimal_value.assign_from_double(1.1);
        columns[9]->insert_data((const char*)&decimal_value, sizeof(decimal_value));

        doris::vectorized::DateV2Value<doris::vectorized::DateV2ValueType> date_v2;
        date_v2.from_date_str("2048-11-10", 10);
        auto date_v2_int = date_v2.to_date_int_val();
        columns[10]->insert_data((const char*)&date_v2_int, sizeof(date_v2_int));

        int8_t v1 = -127;
        columns[11]->insert_data((const char*)&v1, sizeof(v1));

        int16_t v2 = -32767;
        columns[12]->insert_data((const char*)&v2, sizeof(v2));

        int32_t v3 = -2147483647;
        columns[13]->insert_data((const char*)&v3, sizeof(v3));

        int64_t v4 = -9223372036854775807L;
        columns[14]->insert_data((const char*)&v4, sizeof(v4));

        int128_t v5 = -90000;
        columns[15]->insert_data((const char*)&v5, sizeof(v5));

        vectorized::VecDateTimeValue v6;
        v6.from_date_str("2048-11-10", 10);
        auto v6_int = v6.to_int64();
        columns[16]->insert_data((const char*)&v6_int, sizeof(v6_int));

        vectorized::VecDateTimeValue v7;
        v7.from_date_str("2636-08-16 19:39:43", 19);
        auto v7_int = v7.to_int64();
        columns[17]->insert_data((const char*)&v7_int, sizeof(v7_int));

        columns[18]->insert_data("abcd", 4);
        columns[19]->insert_data("abcde", 5);

        decimal_value.assign_from_double(1.1);
        columns[20]->insert_data((const char*)&decimal_value, sizeof(decimal_value));

        date_v2.from_date_str("2048-11-10", 10);
        date_v2_int = date_v2.to_date_int_val();
        columns[21]->insert_data((const char*)&date_v2_int, sizeof(date_v2_int));

        res = delta_writer->write(&block, {0});
        ASSERT_TRUE(res.ok());
    }
    std::shared_ptr<DeltaWriter> writer(delta_writer);

    _mgr->init(100);
    _mgr->register_writer(writer);
    _mgr->handle_memtable_flush();
    CHECK_EQ(0, writer.get()->active_memtable_mem_consumption());
    _mgr->deregister_writer(writer);

    res = writer.get()->close();
    EXPECT_EQ(Status::OK(), res);
    res = writer.get()->close_wait(PSlaveTabletNodes(), false);
    EXPECT_EQ(Status::OK(), res);
    res = _engine->tablet_manager()->drop_tablet(request.tablet_id, request.replica_id, false);
    EXPECT_EQ(Status::OK(), res);
    writer.reset();
}
} // namespace doris