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

#include "runtime/load_stream.h"
#include "runtime/load_stream_mgr.h"
#include "runtime/rowset_builder.h"
#include <runtime/exec_env.h>
#include "gutil/ref_counted.h"
#include "runtime/load_channel.h"
#include "util/uid_util.h"
#include <brpc/stream.h>
#include <olap/rowset/rowset_factory.h>
#include <olap/rowset/rowset_meta.h>
#include <olap/storage_engine.h>
#include <olap/tablet_manager.h>

namespace doris {

TabletStream::TabletStream(int64_t id) : _id(id) {
    // TODO: init flush token
}

void TabletStream::append_data(uint32_t segid, bool eos, butil::IOBuf* data) {
    // TODO: segid
    butil::IOBuf buf = data->movable();
    auto flush_func = [this, segid, buf]() {
         _rowset_builder->append_data(segid, buf);
    };
    _flush_tokens[segid % _flush_tokens.size()].submit_func(flush_func);
}

Status TabletStream::close() {
    for (auto &token : _flush_tokens) {
        token.wait();
    }
    return _rowset_builder->close();
}

void IndexStream::append_data(int64_t tablet_id, uint32_t segid, bool eos, butil::IOBuf* data) {
    TabletStreamSharedPtr tablet_stream;
    {
        std::lock_guard lock_guard(_lock);
        auto it = _tablet_streams_map.find(tablet_id);
        if (it == _tablet_streams_map.end()) {
            tablet_stream = std::make_shared<TabletStream>(tablet_id);
            _tablet_streams_map[tablet_id] = tablet_stream;
        } else {
            tablet_stream = it->second;
        }
    }

    // TODO: segid
    tablet_stream->append_data(segid, eos, data);
}

void IndexStream::close(std::vector<int64_t>* success_tablet_ids,
                        std::vector<int64_t>* failed_tablet_ids) {
    std::lock_guard lock_guard(_lock);
    for (auto& it : _tablet_streams_map) {
        auto st = it.second->close();
        if (st.ok()) {
            success_tablet_ids->push_back(it.second->id());
        } else {
            failed_tablet_ids->push_back(it.second->id());
        }
    }
}


LoadStream::LoadStream(PUniqueId load_id, uint32_t num_senders)
        : _id(load_id), _num_working_senders(num_senders) {
    _senders_status.resize(num_senders, true);
}

LoadStream::~LoadStream() {
}

void LoadStream::close(uint32_t sender_id, std::vector<int64_t>* success_tablet_ids,
                       std::vector<int64_t>* failed_tablet_ids) {
    if (sender_id >= _senders_status.size()) {
        LOG(WARNING) << "out of range sender id " << sender_id << "  num " <<  _senders_status.size();
        return;
    }

    std::lock_guard lock_guard(_lock);
    if (!_senders_status[sender_id]) {
        return;
    }
    _senders_status[sender_id] = false;
    _num_working_senders--;
    if (_num_working_senders == 0) {
        for (auto& it : _index_streams_map) {
            it.second->close(success_tablet_ids, failed_tablet_ids);
        }
    }
}

void LoadStream::_report_result(StreamId stream, std::vector<int64_t>* success_tablet_ids,
                                std::vector<int64_t>* failed_tablet_ids) {
    LOG(INFO) << "OOXXOO report result, success tablet num " << success_tablet_ids->size()
              << "failed tablet num " << failed_tablet_ids->size();
    // TODO
    butil::IOBuf buf;
    PWriteStreamSinkResponse response;
    buf.append(response.SerializeAsString());
    int ret = brpc::StreamWrite(stream, buf);
    // TODO: handle eagain
    if (ret == EAGAIN) {
        LOG(WARNING) << "OOXXOO report status EAGAIN";
    } else if (ret == EINVAL) {
        LOG(WARNING) << "OOXXOO report status EINVAL";
    } else {
        LOG(INFO) << "OOXXOO report status " << ret;
    }
}

void LoadStream::_parse_header(butil::IOBuf* const message, PStreamHeader& hdr) {
    butil::IOBufAsZeroCopyInputStream wrapper(*message);
    hdr.ParseFromZeroCopyStream(&wrapper);
    // TODO: make it VLOG
    LOG(INFO) << "header parse result:"
              << "opcode = " << hdr.opcode()
              << ", indexid = " << hdr.index_id() << ", tabletid = " << hdr.tablet_id()
              << ", segmentid = " << hdr.segment_id()
              << ", schema_hash = " << hdr.tablet_schema_hash();
}

void LoadStream::_append_data(int64_t index_id, int64_t tablet_id, uint32_t segid,
                              bool eos, butil::IOBuf* data) {
    IndexStreamSharedPtr index_stream;
    {
        std::lock_guard lock_guard(_lock);
        auto it = _index_streams_map.find(index_id);
        if (it == _index_streams_map.end()) {
            index_stream = std::make_shared<IndexStream>(index_id);
            _index_streams_map[index_id] = index_stream;
        } else {
            index_stream = it->second;
        }
    }

    index_stream->append_data(tablet_id, segid, eos, data);
}

//TODO trigger build meta when last segment of all cluster is closed
int LoadStream::on_received_messages(StreamId id, butil::IOBuf* const messages[],
                                     size_t size) {
    LOG(INFO) << "OOXXOO on_received_messages " << id << " " << size;
    for (size_t i = 0; i < size; ++i) {
        // step 1: parse header
        size_t hdr_len = 0;
        messages[i]->cutn((void*)&hdr_len, sizeof(size_t));
        butil::IOBuf hdr_buf;
        PStreamHeader hdr;
        messages[i]->cutn(&hdr_buf, hdr_len);
        _parse_header(&hdr_buf, hdr);

        // step 2: dispatch
        switch (hdr.opcode()) {
        case PStreamHeader::APPEND_DATA:
            _append_data(hdr.index_id(), hdr.tablet_id(), hdr.segment_id(), hdr.segment_eos(), messages[i]);
            break;
        case PStreamHeader::CLOSE_LOAD:
            {
                std::vector<int64_t> success_tablet_ids;
                std::vector<int64_t> failed_tablet_ids;
                close(hdr.sender_id(), &success_tablet_ids, &failed_tablet_ids);
                _report_result(id, &success_tablet_ids, &failed_tablet_ids);
            }
            break;
        default:
            LOG(WARNING) << "unexpected stream message " << hdr.opcode();
            DCHECK(false);
        }
    }
    return 0;
}

void LoadStream::on_idle_timeout(StreamId id) {
    brpc::StreamClose(id);
}

void LoadStream::on_closed(StreamId id) {
    if (remove_rpc_stream() == 0) {
        ExecEnv::GetInstance()->get_load_stream_mgr()->clear_load(_id);
    }
}

} // namespace doris
