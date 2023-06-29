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

namespace doris {

class LoadStream : public StreamInputHandler {
public:
    LoadStream();
    ~LoadStream();

    int on_received_messages(StreamId id, butil::IOBuf* const messages[], size_t size) override;
    void on_idle_timeout(StreamId id) override;
    void on_closed(StreamId id) override;

private:
    void _handle_message(StreamId id, PStreamHeader hdr, TargetRowsetPtr rowset,
                         TargetSegmentPtr segment, std::shared_ptr<butil::IOBuf> message);
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
    // TODO: make it per load
    std::map<TargetSegmentPtr, std::shared_ptr<io::LocalFileWriter>, TargetSegmentComparator>
            _file_map;
    std::mutex _file_map_lock;
    // TODO: make it per load
    std::map<TargetRowsetPtr, size_t, TargetRowsetComparator> _tablet_segment_next_id;
    std::mutex _tablet_segment_next_id_lock;
    std::map<TargetSegmentPtr, int64_t, TargetSegmentComparator> _tablet_segment_pos;
    int64_t _current_id = 0;
    // TODO: make it per load
    std::map<TargetSegmentPtr, std::shared_ptr<ThreadPoolToken>, TargetSegmentComparator>
            _segment_token_map; // accessed in single thread, safe
    std::mutex _segment_token_map_lock;
    std::map<TargetSegmentPtr, TargetSegmentPtr, TargetSegmentComparator>
            _rawsegment_finalsegment_map;
    std::mutex _rawsegment_finalsegment_map_lock;
};

using LoadStreamSharedPtr = std::shared_ptr<LoadStream>;

class LoadStreamMgr {
public:
    LoadStreamMgr() = default;
    ~LoadStreamMgr() = default;

    LoadStreamSharedPtr find_or_create_load(PUniqueId loadid) {
        std::string loadid_str = loadid.SerializeAsString();
        std::lock_guard<std::mutex> l(_load_streams_lock);
        if (_load_streams.find(loadid_str) == _load_streams.end()) {
            _load_streams[loadid_str] = std::make_shared<LoadStream>();
        }
        return _load_streams[loadid_str];
    }

    void clear_load(PUniqueId loadid) {
        std::string loadid_str = loadid.SerializeAsString();
        _load_streams.erase(loadid_str);
    }

    Status bind_stream_to_load(LoadStreamSharedPtr loadstream, std::shared_ptr<StreamId> streamid) {
        std::lock_guard<std::mutex> l(_load_stream_to_streamid_lock);
        _load_stream_to_streamid[loadstream] = streamid;
        return Status::OK();
    }

    Status unbind_stream_to_load(LoadStreamSharedPtr loadstream) {
        CHECK(false); // not implemented
        return Status::OK();
    }

private:
    std::map<std::string, LoadStreamSharedPtr> _load_streams;
    std::mutex _load_streams_lock;
    std::map<LoadStreamSharedPtr, std::shared_ptr<StreamId>> _load_stream_to_streamid;
    std::mutex _load_stream_to_streamid_lock;
};

} // namespace doris
