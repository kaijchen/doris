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

#include <gen_cpp/internal_service.pb.h>
#include <stdint.h>

#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include <runtime/sink_stream_mgr.h>
#include <runtime/rowset_builder.h>

namespace doris {

class TabletStream {
public:
    TabletStream(int64_t id);
    void append_data(uint32_t segid, bool eos, butil::IOBuf* data);
    Status close();
    int64_t id() { return _id; }

private:
    int64_t _id;
    RowsetBuilderSharedPtr _rowset_builder;
    std::vector<ThreadPoolToken> _flush_tokens;
};
using TabletStreamSharedPtr = std::shared_ptr<TabletStream>;

class IndexStream {
public:
    IndexStream(int64_t id): _id(id) {}

    void append_data(int64_t tablet_id, uint32_t segid, bool eos, butil::IOBuf* data);

    void flush(uint32_t sender_id);
    void close(std::vector<int64_t>* success_tablet_ids, std::vector<int64_t>* failed_tablet_ids);

private:
    int64_t _id;
    std::unordered_map<int64_t /*tabletid*/, TabletStreamSharedPtr> _tablet_streams_map;
    bthread::Mutex _lock;
};
using IndexStreamSharedPtr = std::shared_ptr<IndexStream>;

class LoadStream : public StreamInputHandler {
public:
    LoadStream(PUniqueId load_id, uint32_t num_senders);
    ~LoadStream();

    uint32_t add_rpc_stream() { return ++_num_rpc_streams; }
    uint32_t remove_rpc_stream() { return --_num_rpc_streams; }

    void close(uint32_t sender_id, std::vector<int64_t>* success_tablet_ids,
               std::vector<int64_t>* failed_tablet_ids);

    // callbacks called by brpc
    int on_received_messages(StreamId id, butil::IOBuf* const messages[], size_t size) override;
    void on_idle_timeout(StreamId id) override;
    void on_closed(StreamId id) override;

private:
    void _parse_header(butil::IOBuf* const message, PStreamHeader& hdr);
    void _append_data(int64_t index_id, int64_t tablet_id, uint32_t segid, bool eos, butil::IOBuf* data);
    void _report_result(StreamId stream, std::vector<int64_t>* success_tablet_ids,
                         std::vector<int64_t>* failed_tablet_ids);
 
private:
    PUniqueId _id;
    std::unordered_map<int64_t, IndexStreamSharedPtr> _index_streams_map;
    std::atomic<uint32_t> _num_rpc_streams;
    std::vector<bool> _senders_status;
    uint32_t _num_working_senders;
    bthread::Mutex _lock;
};

using LoadStreamSharedPtr = std::shared_ptr<LoadStream>;

} // namespace doris
