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
#include <brpc/controller.h>
#include <bthread/types.h>
#include <butil/errno.h>
#include <fmt/format.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/callback.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <functional>
#include <initializer_list>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "exec/data_sink.h"
#include "exec/tablet_info.h"
#include "gutil/ref_counted.h"
#include "runtime/decimalv2_value.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/thread_context.h"
#include "runtime/types.h"
#include "util/bitmap.h"
#include "util/countdown_latch.h"
#include "util/runtime_profile.h"
#include "util/spinlock.h"
#include "util/stopwatch.hpp"
#include "vec/columns/column.h"
#include "vec/common/allocator.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {
class ObjectPool;
class RowDescriptor;
class RuntimeState;
class TDataSink;
class TExpr;
class Thread;
class ThreadPoolToken;
class TupleDescriptor;
template <typename T>
class RefCountClosure;

namespace stream_load {

class OpenPartitionClosure;

// The counter of add_batch rpc of a single node
struct AddBatchCounter {
    // total execution time of a add_batch rpc
    int64_t add_batch_execution_time_us = 0;
    // lock waiting time in a add_batch rpc
    int64_t add_batch_wait_execution_time_us = 0;
    // number of add_batch call
    int64_t add_batch_num = 0;
    // time passed between marked close and finish close
    int64_t close_wait_time_ms = 0;

    AddBatchCounter& operator+=(const AddBatchCounter& rhs) {
        add_batch_execution_time_us += rhs.add_batch_execution_time_us;
        add_batch_wait_execution_time_us += rhs.add_batch_wait_execution_time_us;
        add_batch_num += rhs.add_batch_num;
        close_wait_time_ms += rhs.close_wait_time_ms;
        return *this;
    }
    friend AddBatchCounter operator+(const AddBatchCounter& lhs, const AddBatchCounter& rhs) {
        AddBatchCounter sum = lhs;
        sum += rhs;
        return sum;
    }
};

// It's very error-prone to guarantee the handler capture vars' & this closure's destruct sequence.
// So using create() to get the closure pointer is recommended. We can delete the closure ptr before the capture vars destruction.
// Delete this point is safe, don't worry about RPC callback will run after ReusableClosure deleted.
template <typename T>
class ReusableClosure final : public google::protobuf::Closure {
public:
    ReusableClosure() : cid(INVALID_BTHREAD_ID) {}
    ~ReusableClosure() override {
        // shouldn't delete when Run() is calling or going to be called, wait for current Run() done.
        join();
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->orphan_mem_tracker());
        cntl.Reset();
    }

    static ReusableClosure<T>* create() { return new ReusableClosure<T>(); }

    void addFailedHandler(const std::function<void(bool)>& fn) { failed_handler = fn; }
    void addSuccessHandler(const std::function<void(const T&, bool)>& fn) { success_handler = fn; }

    void join() {
        // We rely on in_flight to assure one rpc is running,
        // while cid is not reliable due to memory order.
        // in_flight is written before getting callid,
        // so we can not use memory fence to synchronize.
        while (_packet_in_flight) {
            // cid here is complicated
            if (cid != INVALID_BTHREAD_ID) {
                // actually cid may be the last rpc call id.
                brpc::Join(cid);
            }
            if (_packet_in_flight) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
    }

    // plz follow this order: reset() -> set_in_flight() -> send brpc batch
    void reset() {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->orphan_mem_tracker());
        cntl.Reset();
        cid = cntl.call_id();
    }

    bool try_set_in_flight() {
        bool value = false;
        return _packet_in_flight.compare_exchange_strong(value, true);
    }

    void clear_in_flight() { _packet_in_flight = false; }

    void end_mark() {
        DCHECK(_is_last_rpc == false);
        _is_last_rpc = true;
    }

    void Run() override {
        DCHECK(_packet_in_flight);
        if (cntl.Failed()) {
            LOG(WARNING) << "failed to send brpc batch, error=" << berror(cntl.ErrorCode())
                         << ", error_text=" << cntl.ErrorText();
            failed_handler(_is_last_rpc);
        } else {
            success_handler(result, _is_last_rpc);
        }
        clear_in_flight();
    }

    brpc::Controller cntl;
    T result;

private:
    brpc::CallId cid;
    std::atomic<bool> _packet_in_flight {false};
    std::atomic<bool> _is_last_rpc {false};
    std::function<void(bool)> failed_handler;
    std::function<void(const T&, bool)> success_handler;
};

class VOlapTableSinkV2;

// pair<row_id,tablet_id>
using Payload = std::pair<std::unique_ptr<vectorized::IColumn::Selector>, std::vector<int64_t>>;

// pair<block,row_ids>
struct WriteMemtableTaskClosure {
    VOlapTableSinkV2* sink;
    const vectorized::Block* block;
    const int64_t partition_id;
    const int64_t index_id;
    const int64_t tablet_id;
    const std::vector<int32_t>& row_idxes;
};

class StreamSinkHandler: public brpc::StreamInputHandler {
public:
    StreamSinkHandler(std::condition_variable& cv) : _all_stream_done_cv(cv) {}

    int on_received_messages(brpc::StreamId id, butil::IOBuf *const messages[], size_t size) override {
        /*
        <loadid, index, tablet, beid, status> = parseHdr(message);
        switch (status) {
        case FAILED:
            s = check_tolerable(); // 检查 tablet_error_map 这个 tablet 的错误副本数是否超过 (replica+1)/2
            if (s.ok()) {
                cancel load; // 对于无法容忍的错误（多数副本失败），cancel 本次 load
            } else {
                tablet_error_map[<index, tablet>] = <tablet, beid> // 将错误记录在小本本上
            }
        case SUCCESS:
            tablet_success_map[<index, tablet>] = <tablet, beid>
        }
        */
        return 0;
    }

    void on_idle_timeout(brpc::StreamId id) override {
    }

    void on_closed(brpc::StreamId id) override {
        _all_stream_done_cv.notify_one();
    }

private:
    std::condition_variable& _all_stream_done_cv;
};

// Write block data to Olap Table.
// When OlapTableSink::open() called, there will be a consumer thread running in the background.
// When you call VOlapTableSinkV2::send(), you will be the producer who products pending batches.
// Join the consumer thread in close().
class VOlapTableSinkV2 final : public DataSink {
public:
    // Construct from thrift struct which is generated by FE.
    VOlapTableSinkV2(ObjectPool* pool, const RowDescriptor& row_desc,
                   const std::vector<TExpr>& texprs, Status* status);

    ~VOlapTableSinkV2() override;

    Status init(const TDataSink& sink) override;
    // TODO: unify the code of prepare/open/close with result sink
    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status close(RuntimeState* state, Status close_status) override;
    Status send(RuntimeState* state, vectorized::Block* block, bool eos = false) override;

    const RowDescriptor& row_desc() { return _input_row_desc; }

    // Returns the runtime profile for the sink.
    RuntimeProfile* profile() override { return _profile; }

private:

    using StreamPool = std::vector<brpc::StreamId>;
    Status _init_stream_pool(StreamPool& stream_pool);

    // tuple<partition_id, index_id, tablet_id>
    using TabletKey = std::tuple<int64_t, int64_t, int64_t>;
    struct TabletKeyHash {
        std::size_t operator()(const TabletKey& k) const {
            return (std::get<0>(k) << 32) ^ (std::get<1>(k) << 16) ^ std::get<2>(k);
        }
    };
    // map<TabletKey, row_idxes>
    using RowsForTablet = std::unordered_map<TabletKey, std::vector<int32_t>, TabletKeyHash>;

    void _generate_rows_for_tablet(RowsForTablet& rows_for_tablet,
                                   const VOlapTablePartition* partition,
                                   uint32_t tablet_index, int row_idx, size_t row_cnt);
    static void* _write_memtable_task(void* write_ctx);

    // make input data valid for OLAP table
    // return number of invalid/filtered rows.
    // invalid row number is set in Bitmap
    // set stop_processing if we want to stop the whole process now.
    Status _validate_data(RuntimeState* state, vectorized::Block* block, Bitmap* filter_bitmap,
                          int* filtered_rows, bool* stop_processing);

    template <bool is_min>
    DecimalV2Value _get_decimalv2_min_or_max(const TypeDescriptor& type);

    template <typename DecimalType, bool IsMin>
    DecimalType _get_decimalv3_min_or_max(const TypeDescriptor& type);

    Status _validate_column(RuntimeState* state, const TypeDescriptor& type, bool is_nullable,
                            vectorized::ColumnPtr column, size_t slot_index, Bitmap* filter_bitmap,
                            bool* stop_processing, fmt::memory_buffer& error_prefix,
                            vectorized::IColumn::Permutation* rows = nullptr);

    // some output column of output expr may have different nullable property with dest slot desc
    // so here need to do the convert operation
    void _convert_to_dest_desc_block(vectorized::Block* block);

    Status find_tablet(RuntimeState* state, vectorized::Block* block, int row_index,
                       const VOlapTablePartition** partition, uint32_t& tablet_index,
                       bool& stop_processing, bool& is_continue);

    std::shared_ptr<MemTracker> _mem_tracker;

    ObjectPool* _pool;
    const RowDescriptor& _input_row_desc;

    // unique load id
    PUniqueId _load_id;
    int64_t _txn_id = -1;
    int _num_replicas = -1;
    int _tuple_desc_id = -1;

    // this is tuple descriptor of destination OLAP table
    TupleDescriptor* _output_tuple_desc = nullptr;
    RowDescriptor* _output_row_desc = nullptr;

    // number of senders used to insert into OlapTable, if we only support single node insert,
    // all data from select should collectted and then send to OlapTable.
    // To support multiple senders, we maintain a channel for each sender.
    int _sender_id = -1;
    int _num_senders = -1;
    bool _is_high_priority = false;

    // TODO(zc): think about cache this data
    std::shared_ptr<OlapTableSchemaParam> _schema;
    OlapTableLocationParam* _location = nullptr;
    bool _write_single_replica = false;
    OlapTableLocationParam* _slave_location = nullptr;
    DorisNodesInfo* _nodes_info = nullptr;

    RuntimeProfile* _profile = nullptr;

    std::set<int64_t> _partition_ids;
    // only used for partition with random distribution
    std::map<int64_t, int64_t> _partition_to_tablet_map;

    Bitmap _filter_bitmap;

    std::unique_ptr<ThreadPoolToken> _send_batch_thread_pool_token;

    std::map<std::pair<int, int>, DecimalV2Value> _max_decimalv2_val;
    std::map<std::pair<int, int>, DecimalV2Value> _min_decimalv2_val;

    std::map<int, int32_t> _max_decimal32_val;
    std::map<int, int32_t> _min_decimal32_val;
    std::map<int, int64_t> _max_decimal64_val;
    std::map<int, int64_t> _min_decimal64_val;
    std::map<int, int128_t> _max_decimal128_val;
    std::map<int, int128_t> _min_decimal128_val;

    // Stats for this
    int64_t _validate_data_ns = 0;
    int64_t _send_data_ns = 0;
    int64_t _number_input_rows = 0;
    int64_t _number_output_rows = 0;
    int64_t _number_filtered_rows = 0;
    int64_t _number_immutable_partition_filtered_rows = 0;

    MonotonicStopWatch _row_distribution_watch;

    RuntimeProfile::Counter* _input_rows_counter = nullptr;
    RuntimeProfile::Counter* _output_rows_counter = nullptr;
    RuntimeProfile::Counter* _filtered_rows_counter = nullptr;
    RuntimeProfile::Counter* _send_data_timer = nullptr;
    RuntimeProfile::Counter* _row_distribution_timer = nullptr;
    RuntimeProfile::Counter* _append_node_channel_timer = nullptr;
    RuntimeProfile::Counter* _filter_timer = nullptr;
    RuntimeProfile::Counter* _where_clause_timer = nullptr;
    RuntimeProfile::Counter* _wait_mem_limit_timer = nullptr;
    RuntimeProfile::Counter* _validate_data_timer = nullptr;
    RuntimeProfile::Counter* _open_timer = nullptr;
    RuntimeProfile::Counter* _close_timer = nullptr;
    RuntimeProfile::Counter* _non_blocking_send_timer = nullptr;
    RuntimeProfile::Counter* _non_blocking_send_work_timer = nullptr;
    RuntimeProfile::Counter* _serialize_batch_timer = nullptr;
    RuntimeProfile::Counter* _total_add_batch_exec_timer = nullptr;
    RuntimeProfile::Counter* _max_add_batch_exec_timer = nullptr;
    RuntimeProfile::Counter* _total_wait_exec_timer = nullptr;
    RuntimeProfile::Counter* _max_wait_exec_timer = nullptr;
    RuntimeProfile::Counter* _add_batch_number = nullptr;
    RuntimeProfile::Counter* _num_node_channels = nullptr;

    // load mem limit is for remote load channel
    int64_t _load_mem_limit = -1;

    // the timeout of load channels opened by this tablet sink. in second
    int64_t _load_channel_timeout_s = 0;

    int32_t _send_batch_parallelism = 1;
    // Save the status of close() method
    Status _close_status;

    // User can change this config at runtime, avoid it being modified during query or loading process.
    bool _transfer_large_data_by_brpc = false;

    // FIND_TABLET_EVERY_ROW is used for both hash and random distribution info, which indicates that we
    // should compute tablet index for every row
    // FIND_TABLET_EVERY_BATCH is only used for random distribution info, which indicates that we should
    // compute tablet index for every row batch
    // FIND_TABLET_EVERY_SINK is only used for random distribution info, which indicates that we should
    // only compute tablet index in the corresponding partition once for the whole time in olap table sink
    enum FindTabletMode { FIND_TABLET_EVERY_ROW, FIND_TABLET_EVERY_BATCH, FIND_TABLET_EVERY_SINK };
    FindTabletMode findTabletMode = FindTabletMode::FIND_TABLET_EVERY_ROW;

    VOlapTablePartitionParam* _vpartition = nullptr;
    vectorized::VExprContextSPtrs _output_vexpr_ctxs;

    RuntimeState* _state = nullptr;

    std::unordered_set<int64_t> _opened_partitions;

    std::shared_ptr<StreamPool> _stream_pool;
    int32_t _stream_pool_index = 0;

    std::atomic<int32_t> _flying_task_count {0};

    std::unordered_map<uint64_t, DeltaWriter*> _delta_writer_for_tablet;
    std::mutex _delta_writer_for_tablet_mutex;

    std::mutex _all_stream_done_mutex;
    std::condition_variable _all_stream_done_cv;
};

} // namespace stream_load
} // namespace doris
