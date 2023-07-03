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

#include "exec/tablet_info.h"
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

TabletStream::TabletStream(PUniqueId load_id, int64_t id, int64_t txn_id, uint32_t  num_senders)
        : _id(id), _next_segid(0), _load_id(load_id), _txn_id(txn_id) {
    for (int i = 0; i < 10; i++) {
        _flush_tokens.emplace_back(ExecEnv::GetInstance()->get_load_stream_mgr()->new_token());
    }

    _segids_mapping.resize(num_senders);
}

Status TabletStream::init(OlapTableSchemaParam* schema, int64_t partition_id) {
    BuildContext context;
    context.tablet_id = _id;
    // TODO schema_hash
    context.txn_id = _txn_id;
    context.partition_id = partition_id;
    context.load_id = _load_id;
    // TODO tablet_schema
    context.table_schema_param = schema;

    _rowset_builder = std::make_shared<RowsetBuilder>(&context, _load_id);
    return _rowset_builder->init();
}

void TabletStream::append_data(uint32_t sender_id, uint32_t segid, bool eos, butil::IOBuf* data) {
    // TODO failed early

    // We don't need a lock protecting _segids_mapping, because it is written once.
    if(sender_id >= _segids_mapping.size()) {
        LOG(WARNING) << "sender id is out of range, sender_id=" << sender_id << ", num_senders="
                     << _segids_mapping.size();
        std::lock_guard lock_guard(_lock);
        _failed_st = Status::Error<ErrorCode::INVALID_ARGUMENT>("sender id is out of range {}/{}",
                                                                sender_id,
                                                                _segids_mapping.size());
        return;
    }

    // Ensure there are enough space and mapping are built.
    if (segid + 1 > _segids_mapping[sender_id].size()) {
        // TODO: Each sender lock is enough.
        std::lock_guard lock_guard(_lock);
        ssize_t origin_size = _segids_mapping[sender_id].size();
        if (segid + 1 > origin_size) {
            _segids_mapping[sender_id].resize(segid + 1, std::numeric_limits<uint32_t>::max());
            // handle concurrency.
            for (ssize_t index = origin_size; index <= segid; index++) {
                _segids_mapping[sender_id][index] = _next_segid;
                _next_segid++;
            }
        }
    }

    // Each sender sends data in one segment sequential, so we also does not
    // need a lock here.
    uint32_t new_segid = _segids_mapping[sender_id][segid];
    DCHECK(new_segid != std::numeric_limits<uint32_t>::max());
    butil::IOBuf buf = data->movable();
    auto flush_func = [this, new_segid, eos, buf]() {
         auto st = _rowset_builder->append_data(new_segid, buf);
         if (eos && st.ok()) {
             st = _rowset_builder->close_segment(new_segid);
         }

         if (!st.ok()) {
             std::lock_guard lock_guard(_lock);
             _failed_st = st;
         }
    };
    _flush_tokens[segid % _flush_tokens.size()]->submit_func(flush_func);
}

Status TabletStream::close() {
    for (auto &token : _flush_tokens) {
        token->wait();
    }
    if (!_failed_st.ok()) {
        return _failed_st;
    }
    return _rowset_builder->close();
}

void IndexStream::append_data(uint32_t sender_id, int64_t tablet_id,
                              uint32_t segid, bool eos, butil::IOBuf* data) {
    auto it = _tablet_partitions.find(tablet_id);
    if (it == _tablet_partitions.end()) {
        _failed_tablet_ids.push_back(tablet_id);
        return;
    }
    int64_t partition_id = it->second;
    TabletStreamSharedPtr tablet_stream;
    {
        std::lock_guard lock_guard(_lock);
        auto it = _tablet_streams_map.find(tablet_id);
        if (it == _tablet_streams_map.end()) {
            tablet_stream = std::make_shared<TabletStream>(_load_id, tablet_id, _txn_id, _num_senders);
            _tablet_streams_map[tablet_id] = tablet_stream;
            tablet_stream->init(_schema.get(), partition_id);
        } else {
            tablet_stream = it->second;
        }
    }

    // TODO: segid
    tablet_stream->append_data(sender_id, segid, eos, data);
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
    failed_tablet_ids->insert(failed_tablet_ids->end(), _failed_tablet_ids.begin(),
                              _failed_tablet_ids.end());
}

LoadStream::LoadStream(PUniqueId id) : _id(id) {
}

LoadStream::~LoadStream() {
}

Status LoadStream::init(const POpenStreamSinkRequest* request) {
    _num_senders = request->num_senders();
    _num_working_senders = request->num_senders();
    _senders_status.resize(_num_senders, true);
    _txn_id = request->txn_id();

    _schema = std::make_shared<OlapTableSchemaParam>();
    RETURN_IF_ERROR(_schema->init(request->schema()));
    for (auto& index : request->schema().indexes()) {
        _index_streams_map[index.id()] = std::make_shared<IndexStream>(_id, index.id(), _txn_id,
                                                                       _num_senders, _schema);
    }
    return Status::OK();
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
              << ", failed tablet num " << failed_tablet_ids->size();
    // TODO
    butil::IOBuf buf;
    PWriteStreamSinkResponse response;
    response.set_allocated_id(&_id);
    for (auto id : *success_tablet_ids) {
        response.add_success_tablets(id);
    }
    for (auto id : *failed_tablet_ids) {
        response.add_failed_tablets(id);
    }
    buf.append(response.SerializeAsString());
    int ret = brpc::StreamWrite(stream, buf);
    response.release_id();
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

void LoadStream::_append_data(uint32_t sender_id, int64_t index_id, int64_t tablet_id, uint32_t segid,
                              bool eos, butil::IOBuf* data) {
    IndexStreamSharedPtr index_stream;

    auto it = _index_streams_map.find(index_id);
    if (it == _index_streams_map.end()) {
        // TODO ERROR
    } else {
        index_stream = it->second;
    }

    index_stream->append_data(sender_id, tablet_id, segid, eos, data);
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
            _append_data(hdr.sender_id(), hdr.index_id(), hdr.tablet_id(), hdr.segment_id(), hdr.segment_eos(), messages[i]);
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
