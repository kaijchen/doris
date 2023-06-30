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
#include <runtime/load_stream.h>

namespace doris {

class LoadStreamMgr {
public:
    LoadStreamMgr() = default;
    ~LoadStreamMgr() = default;

    LoadStreamSharedPtr find_or_create_load(PUniqueId loadid);
    void clear_load(PUniqueId loadid);
    Status bind_stream_to_load(LoadStreamSharedPtr loadstream, std::shared_ptr<StreamId> streamid);
    Status unbind_stream_to_load(LoadStreamSharedPtr loadstream);

private:
    std::map<std::string, LoadStreamSharedPtr> _load_streams;
    std::mutex _load_streams_lock;
    std::map<LoadStreamSharedPtr, std::shared_ptr<StreamId>> _load_stream_to_streamid;
    std::mutex _load_stream_to_streamid_lock;
};

} // namespace doris
