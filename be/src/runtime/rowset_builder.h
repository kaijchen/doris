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

#pragma once

#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/types.pb.h>
#include <stdint.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "olap/memtable.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "util/spinlock.h"
#include "util/uid_util.h"

namespace doris {

class StorageEngine;
class TupleDescriptor;
class SlotDescriptor;
class OlapTableSchemaParam;
class RowsetWriter;

namespace vectorized {
class Block;
} // namespace vectorized

struct BuildContext {
    int64_t tablet_id;
    int32_t schema_hash;
    int64_t txn_id;
    int64_t partition_id;
    PUniqueId load_id;
    bool is_high_priority = false;
    OlapTableSchemaParam* table_schema_param;
    int64_t index_id = 0;
};

// Builder from segments (load, index, tablet).
class RowsetBuilder {
public:
    RowsetBuilder(BuildContext* context, const UniqueId& load_id = TUniqueId());

    ~RowsetBuilder();

    Status init();

    Status append_data(uint32_t segid, butil::IOBuf& buf);
    Status close_segment(uint32_t segid);

    void add_segments(std::vector<SegmentStatistics>& segstat);

    // flush the last memtable to flush queue, must call it before close_wait()
    Status close();
    // wait for all memtables to be flushed.
    Status close_wait();

    // abandon current memtable and wait for all pending-flushing memtables to be destructed.
    // mem_consumption() should be 0 after this function returns.
    Status cancel();
    Status cancel_with_status(const Status& st);

    int64_t tablet_id() { return _tablet->tablet_id(); }

private:
    void _garbage_collection();

    void _build_current_tablet_schema(int64_t index_id,
                                      const OlapTableSchemaParam* table_schema_param,
                                      const TabletSchema& ori_tablet_schema);

    bool _is_init = false;
    bool _is_cancelled = false;
    bool _is_closed = false;
    Status _cancel_status;
    BuildContext _context;
    TabletSharedPtr _tablet;
    RowsetSharedPtr _cur_rowset;
    std::unique_ptr<RowsetWriter> _rowset_writer;
    //const TabletSchema* _tablet_schema;
    // tablet schema owned by delta writer, all write will use this tablet schema
    // it's build from tablet_schema（stored when create tablet） and OlapTableSchema
    // every request will have it's own tablet schema so simple schema change can work
    TabletSchemaSPtr _tablet_schema;
    bool _delta_written_success;

    StorageEngine* _storage_engine;
    UniqueId _load_id;
    std::mutex _lock;

    DeleteBitmapPtr _delete_bitmap = nullptr;
    // current rowset_ids, used to do diff in publish_version
    RowsetIdUnorderedSet _rowset_ids;
    // current max version, used to calculate delete bitmap
    int64_t _cur_max_version;
};

using RowsetBuilderPtr = std::shared_ptr<RowsetBuilder>;

} // namespace doris
