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

#include "runtime/load_stream_mgr.h"
#include "runtime/load_stream.h"
#include <runtime/exec_env.h>
#include "gutil/ref_counted.h"
#include "olap/lru_cache.h"
#include "runtime/load_channel.h"
#include "util/uid_util.h"
#include <brpc/stream.h>
#include <olap/rowset/rowset_factory.h>
#include <olap/rowset/rowset_meta.h>
#include <olap/storage_engine.h>
#include <olap/tablet_manager.h>

namespace doris {

LoadStreamMgr::LoadStreamMgr(uint32_t segment_file_writer_thread_num) {
    ThreadPoolBuilder("SegmentFileWriterThreadPool")
            .set_min_threads(segment_file_writer_thread_num)
            .set_max_threads(segment_file_writer_thread_num)
            .build(&_file_writer_thread_pool);
}

LoadStreamMgr::~LoadStreamMgr() {
    _file_writer_thread_pool->shutdown();
}

LoadStreamSharedPtr LoadStreamMgr::try_open_load_stream(PUniqueId load_id, size_t num_senders) {
    std::string load_id_str = load_id.SerializeAsString();
    LoadStreamSharedPtr load_stream;

    {
        std::lock_guard l(_lock);
        auto it = _load_streams_map.find(load_id_str);
        if (it != _load_streams_map.end()) {
            load_stream = it->second;
        } else {
            load_stream = std::make_shared<LoadStream>(load_id, num_senders);
            _load_streams_map[load_id_str] = load_stream;
        }
    }
    load_stream->add_rpc_stream();
    return load_stream;
}

void LoadStreamMgr::clear_load(PUniqueId load_id) {
    std::string load_id_str = load_id.SerializeAsString();
    std::lock_guard l(_lock);
    _load_streams_map.erase(load_id_str);
}

} // namespace doris
