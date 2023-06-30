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

LoadStreamSharedPtr LoadStreamMgr::find_or_create_load(PUniqueId loadid) {
    std::string loadid_str = loadid.SerializeAsString();
    std::lock_guard<std::mutex> l(_load_streams_lock);
    if (_load_streams.find(loadid_str) == _load_streams.end()) {
        _load_streams[loadid_str] = std::make_shared<LoadStream>();
    }
    return _load_streams[loadid_str];
}

void LoadStreamMgr::clear_load(PUniqueId loadid) {
    std::string loadid_str = loadid.SerializeAsString();
    _load_streams.erase(loadid_str);
}

Status LoadStreamMgr::bind_stream_to_load(LoadStreamSharedPtr loadstream, std::shared_ptr<StreamId> streamid) {
    std::lock_guard<std::mutex> l(_load_stream_to_streamid_lock);
    _load_stream_to_streamid[loadstream] = streamid;
    return Status::OK();
}

Status LoadStreamMgr::unbind_stream_to_load(LoadStreamSharedPtr loadstream) {
    CHECK(false); // not implemented
    return Status::OK();
}

} // namespace doris
