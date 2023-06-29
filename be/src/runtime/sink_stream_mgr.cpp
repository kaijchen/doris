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

#include "sink_stream_mgr.h"

#include <olap/rowset/rowset_factory.h>
#include <olap/rowset/rowset_meta.h>
#include <olap/storage_engine.h>
#include <olap/tablet_manager.h>
#include <runtime/exec_env.h>

#include "common/config.h"
#include "util/uid_util.h"

namespace doris {

bool TargetSegmentComparator::operator()(const TargetSegmentPtr& lhs,
                                         const TargetSegmentPtr& rhs) const {
    TargetRowsetComparator rowset_cmp;
    auto less = rowset_cmp.operator()(lhs->target_rowset, rhs->target_rowset);
    auto greater = rowset_cmp.operator()(rhs->target_rowset, lhs->target_rowset);
    // if rowset not equal
    if (less || greater) {
        return less;
    }
    if (lhs->segmentid != rhs->segmentid) {
        return lhs->segmentid < rhs->segmentid;
    }
    if (lhs->backendid != rhs->backendid) {
        return lhs->backendid < rhs->backendid;
    }
    return false;
}

bool TargetRowsetComparator::operator()(const TargetRowsetPtr& lhs,
                                        const TargetRowsetPtr& rhs) const {
    if (lhs->streamid != rhs->streamid) {
        return lhs->streamid < rhs->streamid;
    }
    if (lhs->loadid.hi != rhs->loadid.hi) {
        return lhs->loadid.hi < rhs->loadid.hi;
    }
    if (lhs->loadid.lo != rhs->loadid.lo) {
        return lhs->loadid.lo < rhs->loadid.lo;
    }
    if (lhs->indexid != rhs->indexid) {
        return lhs->indexid < rhs->indexid;
    }
    if (lhs->tabletid != rhs->tabletid) {
        return lhs->tabletid < rhs->tabletid;
    }
    return false;
}

std::string TargetRowset::to_string() {
    std::stringstream ss;
    ss << "streamid: " << streamid << ", loadid: " << loadid << ", indexid: " << indexid
       << ", tabletid: " << tabletid;
    return ss.str();
}

std::string TargetSegment::to_string() {
    std::stringstream ss;
    ss << target_rowset->to_string() << ", segmentid: " << segmentid;
    return ss.str();
}

SinkStreamMgr::SinkStreamMgr() {
    for (int i = 0; i < 1000; ++i) {
        StreamIdPtr stream_id = std::make_shared<StreamId>();
        _free_stream_ids.push_back(stream_id);
    }
}

SinkStreamMgr::~SinkStreamMgr() {
    for (auto& ptr : _free_stream_ids) {
        ptr.reset();
    }
}

StreamIdPtr SinkStreamMgr::get_free_stream_id() {
    StreamIdPtr ptr = nullptr;
    {
        std::lock_guard<std::mutex> l(_lock);
        if (_free_stream_ids.empty()) {
            ptr = std::make_shared<StreamId>();
        } else {
            ptr = _free_stream_ids.back();
            _free_stream_ids.pop_back();
        }
    }
    return ptr;
}

void SinkStreamMgr::release_stream_id(StreamIdPtr id) {
    {
        std::lock_guard<std::mutex> l(_lock);
        _free_stream_ids.push_back(id);
    }
}

} // namespace doris
