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

// locate a rowset
struct TargetRowset {
    brpc::StreamId streamid;
    // UniqueId loadid; // TODO: remove it here and sink side
    int64_t indexid;
    int64_t tabletid;
    RowsetId rowsetid; // TODO probably not needed

    std::string to_string();
};
using TargetRowsetPtr = std::shared_ptr<TargetRowset>;
struct TargetRowsetComparator {
    bool operator()(const TargetRowsetPtr& lhs, const TargetRowsetPtr& rhs) const;
};

// locate a segment file
struct TargetSegment {
    TargetRowsetPtr target_rowset;
    int64_t segmentid;
    int64_t backendid;

    std::string to_string();
};
using TargetSegmentPtr = std::shared_ptr<TargetSegment>;
struct TargetSegmentComparator {
    bool operator()(const TargetSegmentPtr& lhs, const TargetSegmentPtr& rhs) const;
};

class IndexStream {
public:
    IndexStream(int64_t indexid): _indexid(indexid) {}

    RowsetBuilderPtr find_or_create_rowset_builder(int64_t tablet_id);

    Status close(uint32_t sender_id);

    int64_t index_id() { return _indexid; }

private:
    int64_t _indexid;
    std::map<int64_t /*tabletid*/, RowsetBuilderPtr> _rowset_builder_map;

};
using IndexStreamPtr = std::shared_ptr<IndexStream>;

class LoadStream : public StreamInputHandler {
public:
    LoadStream();
    ~LoadStream();

    int on_received_messages(StreamId id, butil::IOBuf* const messages[], size_t size) override;
    void on_idle_timeout(StreamId id) override;
    void on_closed(StreamId id) override;

    IndexStreamPtr find_or_create_index_stream(uint64_t indexid);

private:
    void _handle_message(StreamId stream, PStreamHeader hdr,
                         TargetRowsetPtr target_rowset,
                         TargetSegmentPtr target_segment,
                         IndexStreamPtr index_stream,
                         RowsetBuilderPtr rowset_builder,
                         std::shared_ptr<butil::IOBuf> message);
    void _parse_header(butil::IOBuf* const message, PStreamHeader& hdr);
    Status _create_and_open_file(TargetSegmentPtr target_segment, std::string path);
    Status _append_data(TargetSegmentPtr target_segment, std::shared_ptr<butil::IOBuf> message);
    Status _close_file(TargetSegmentPtr target_segment);
    void _report_status(StreamId stream, TargetRowsetPtr target_rowset, bool is_success,
                        std::string error_msg);
    uint64_t get_next_segmentid(TargetRowsetPtr target_rowset);
    uint64_t get_next_segmentid(TargetRowsetPtr target_rowset, int64_t segmentid,
                                int64_t backendid);
    Status _build_rowset(TargetRowsetPtr target_rowset, const RowsetMetaPB& rowset_meta);

private:
    std::unique_ptr<ThreadPool> _workers;
    std::map<TargetSegmentPtr, std::shared_ptr<io::LocalFileWriter>, TargetSegmentComparator>
            _file_map;
    std::mutex _file_map_lock;
    std::map<TargetRowsetPtr, size_t, TargetRowsetComparator> _tablet_segment_next_id;
    std::mutex _tablet_segment_next_id_lock;
    std::map<TargetSegmentPtr, int64_t, TargetSegmentComparator> _tablet_segment_pos;
    int64_t _current_id = 0;
    std::map<TargetSegmentPtr, std::shared_ptr<ThreadPoolToken>, TargetSegmentComparator>
            _segment_token_map; // accessed in single thread, safe
    std::mutex _segment_token_map_lock;
    std::map<TargetSegmentPtr, TargetSegmentPtr, TargetSegmentComparator>
            _rawsegment_finalsegment_map;
    std::mutex _rawsegment_finalsegment_map_lock;
    std::map<int64_t /*indexid*/, IndexStreamPtr> _index_stream_map;
};

using LoadStreamSharedPtr = std::shared_ptr<LoadStream>;

} // namespace doris
