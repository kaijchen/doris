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
    //if (lhs->loadid.hi != rhs->loadid.hi) {
    //    return lhs->loadid.hi < rhs->loadid.hi;
    //}
    //if (lhs->loadid.lo != rhs->loadid.lo) {
    //    return lhs->loadid.lo < rhs->loadid.lo;
    //}
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
    ss << "streamid: " << streamid << ", indexid: " << indexid
       << ", tabletid: " << tabletid;
    return ss.str();
}

std::string TargetSegment::to_string() {
    std::stringstream ss;
    ss << target_rowset->to_string() << ", segmentid: " << segmentid;
    return ss.str();
}

LoadStream::LoadStream() {
    ThreadPoolBuilder("SinkStreamHandler")
            .set_min_threads(20) // TODO: make them configurable
            .set_max_threads(20)
            .build(&_workers);
}

LoadStream::~LoadStream() {
    if (_workers) {
        _workers->shutdown();
    }
}

Status LoadStream::_create_and_open_file(TargetSegmentPtr target_segment, std::string path) {
    LOG(INFO) << "create and open file, target_segment = " << target_segment->to_string()
              << ", path = " << path;
    int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        return Status::InternalError("open file error");
    }
    std::shared_ptr<io::LocalFileWriter> file_writer =
            std::make_shared<io::LocalFileWriter>(path, fd);
    {
        std::lock_guard<std::mutex> l(_file_map_lock);
        _file_map[target_segment] = file_writer;
    }
    return Status::OK();
}

Status LoadStream::_append_data(TargetSegmentPtr target_segment,
                                std::shared_ptr<butil::IOBuf> message) {
    LOG(INFO) << "append data, target_segment = " << target_segment->to_string()
              << ", data length = " << message->length();
    auto itr = _file_map.end();
    {
        std::lock_guard<std::mutex> l(_file_map_lock);
        itr = _file_map.find(target_segment);

        if (itr == _file_map.end()) {
            return Status::InternalError("file not found");
        }
    }
    std::shared_ptr<io::LocalFileWriter> file_writer = (itr->second);
    file_writer->append(message->to_string());
    return Status::OK();
}

Status LoadStream::_close_file(TargetSegmentPtr target_segment) {
    LOG(INFO) << "close file, target_segment = " << target_segment->to_string();
    std::shared_ptr<io::LocalFileWriter> file_writer = nullptr;
    {
        std::lock_guard<std::mutex> l(_file_map_lock);
        auto itr = _file_map.find(target_segment);
        if (itr == _file_map.end()) {
            return Status::InternalError("close file error");
        }
        file_writer = itr->second;
        _file_map.erase(itr);
    }
    file_writer->finalize();
    LOG(INFO) << "segment_size: " << file_writer->bytes_appended();
    file_writer->close();
    return Status::OK();
}

void LoadStream::_report_status(StreamId stream, TargetRowsetPtr target_rowset,
                                bool is_success, std::string error_msg) {
    LOG(INFO) << "OOXXOO report status " << is_success << " " << error_msg;
    butil::IOBuf buf;
    PWriteStreamSinkResponse response;
    response.set_success(is_success);
    response.set_error_msg(error_msg);
    response.set_index_id(target_rowset->indexid);
    response.set_tablet_id(target_rowset->tabletid);
    buf.append(response.SerializeAsString());
    int ret = brpc::StreamWrite(stream, buf);
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
              << ", segmentid = " << hdr.segment_id() << ", rowsetid = " << hdr.rowset_id()
              << ", schema_hash = " << hdr.tablet_schema_hash();
}

// TODO: delete this method
uint64_t LoadStream::get_next_segmentid(TargetRowsetPtr target_rowset) {
    // TODO: need support concurrent flush memtable
    {
        std::lock_guard<std::mutex> l(_tablet_segment_next_id_lock);
        if (_tablet_segment_next_id.find(target_rowset) == _tablet_segment_next_id.end()) {
            _tablet_segment_next_id[target_rowset] = 0;
            return 0;
        } else {
            return ++_tablet_segment_next_id[target_rowset];
        }
    }
}

uint64_t LoadStream::get_next_segmentid(TargetRowsetPtr target_rowset, int64_t segmentid,
                                        int64_t backendid) {
    // TODO: delete id;
    std::lock_guard<std::mutex> l(_tablet_segment_next_id_lock);
    TargetSegmentPtr target_segment = std::make_shared<TargetSegment>();
    target_segment->target_rowset = target_rowset;
    target_segment->segmentid = segmentid;
    target_segment->backendid = backendid;

    auto it = _tablet_segment_pos.find(target_segment);
    if (it != _tablet_segment_pos.end()) {
        return it->second;
    }
    for (int64_t i = 0; i <= segmentid; i++) {
        TargetSegmentPtr front_target_segment = std::make_shared<TargetSegment>();
        front_target_segment->target_rowset = target_rowset;
        front_target_segment->segmentid = i;
        front_target_segment->backendid = backendid;
        auto it = _tablet_segment_pos.find(front_target_segment);
        if (it == _tablet_segment_pos.end()) {
            _tablet_segment_pos.emplace(front_target_segment, _current_id);
            _current_id++;
        }
    }
    return _tablet_segment_pos.find(target_segment)->second;
}

Status LoadStream::_build_rowset(TargetRowsetPtr target_rowset,
                                 const RowsetMetaPB& rowset_meta_pb) {
    RowsetMetaSharedPtr rowset_meta(new RowsetMeta());
    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet( //TODO
            rowset_meta_pb.tablet_id(), rowset_meta_pb.tablet_schema_hash());
    std::string rowset_meta_str;
    bool ret = rowset_meta_pb.SerializeToString(&rowset_meta_str);
    if (!ret) {
        LOG(WARNING) << "failed to parse rowset meta pb sent by sink "
                     << "rowset_id=" << rowset_meta_pb.rowset_id()
                     << ", tablet_id=" << rowset_meta_pb.tablet_id()
                     << ", txn_id=" << rowset_meta_pb.txn_id();
        return Status::InternalError("failed to parse rowset meta pb sent by sink");
    }

    bool parsed = rowset_meta->init(rowset_meta_str);
    RowsetId new_rowset_id = StorageEngine::instance()->next_rowset_id();
    rowset_meta->set_rowset_id(new_rowset_id);
    rowset_meta->set_tablet_uid(tablet->tablet_uid());
    if (!parsed) {
        LOG(WARNING) << "failed to init rowset meta "
                     << "rowset_id=" << rowset_meta_pb.rowset_id()
                     << ", tablet_id=" << rowset_meta_pb.tablet_id()
                     << ", txn_id=" << rowset_meta_pb.txn_id();
        return Status::InternalError("failed to init rowset meta");
    }

    RowsetSharedPtr rowset;
    Status create_status = RowsetFactory::create_rowset(
            tablet->tablet_schema(), tablet->tablet_path(), rowset_meta, &rowset);
    if (!create_status) {
        LOG(WARNING) << "failed to create rowset "
                     << "rowset_id=" << rowset_meta_pb.rowset_id()
                     << ", tablet_id=" << rowset_meta_pb.tablet_id()
                     << ", txn_id=" << rowset_meta_pb.txn_id();
        return Status::InternalError("failed to create rowset");
    }

    return Status::OK();
}

void LoadStream::_handle_message(StreamId stream, PStreamHeader hdr,
                                 TargetRowsetPtr target_rowset,
                                 TargetSegmentPtr target_segment,
                                 index_stream,
                                 rowset_builder,
                                 std::shared_ptr<butil::IOBuf> message) {
    Status s = Status::OK();
    std::string path;
    TabletSharedPtr tablet = nullptr;

    switch (hdr.opcode()) {
    case PStreamHeader::OPEN_FILE:
        s = rowset_builder->create_and_open_file(target_segment, path);
        break;
    case PStreamHeader::APPEND_DATA:
        s = rowset_builder->append_data(target_segment, message);
        break;
    case PStreamHeader::CLOSE_FILE:
        s = rowset_builder->close_file(target_segment);
        s = rowset_builder->add_segment();
        record_success_status();
        break;
    case PStreamHeader::CLOSE_TABLET:
        DCHECK(hdr.has_rowset_meta());
        record_rowset_meta();
        _report_status(stream, target_rowset, true, s.to_string());
        break;
    case PStreamHeader::CLOSE_INDEX:
        break;
        if (--num_flying_sender) {
            // for each tablets in this index build meta
            rowset_builder->build_rowset();
        }
        break;
    default:
        DCHECK(false);
    }
    if (!s.ok()) {
        LOG(WARNING) << "Failed to handle " << PStreamHeader_Opcode_Name(hdr.opcode())
                     << " message in stream (" << stream << "), target segment ("
                     << target_segment->to_string() << "), reason: " << s.to_string();
        // _report_status(stream, target_rowset, false, s.to_string());
        record_failed_status();
    }
}

StreamRowsetBuilderPtr IndexStream::find_or_create_rowset_builder(int64_t tabletid) {
    if (_rowset_builder_map.find(tabletid) == _rowset_builder_map.end()) {
            _rowset_builder_map.emplace(tabletid, std::make_shared<StreamRowsetBuilder>());
    }
    return _rowset_builder_map[tabletid];
}

IndexStreamPtr LoadStream::find_or_create_index_stream(uint64_t index) {
    if (_index_stream_map.find(index) == _index_stream_map.end()) {
        _index_stream_map.emplace(index, std::make_shared<IndexStream>(index));
    }
    return _index_stream_map[index];
}

//TODO trigger build meta when last segment of all cluster is closed
int LoadStream::on_received_messages(StreamId id, butil::IOBuf* const messages[],
                                     size_t size) {
    LOG(INFO) << "OOXXOO on_received_messages " << id << " " << size;
    for (size_t i = 0; i < size; ++i) {
        std::shared_ptr<butil::IOBuf> messageBuf =
                std::make_shared<butil::IOBuf>(messages[i]->movable()); // hold the data
        size_t hdr_len = 0;
        messageBuf->cutn((void*)&hdr_len, sizeof(size_t));
        butil::IOBuf hdr_buf;
        PStreamHeader hdr;
        messageBuf->cutn(&hdr_buf, hdr_len);
        _parse_header(&hdr_buf, hdr);

        IndexStreamPtr index_stream = find_or_create_index_stream(hdr.index_id());
        if (hdr.has_tablet_id()) {
            rowset_builder = index_stream->find_or_create_rowset_builder(hdr.tablet_id());
        }

        TargetRowsetPtr target_rowset = std::make_shared<TargetRowset>();
        target_rowset->streamid = id;
        // target_rowset->loadid = hdr.load_id();
        target_rowset->indexid = hdr.index_id();
        target_rowset->tabletid = hdr.tablet_id();

        TargetSegmentPtr target_segment = nullptr;

        TargetSegmentPtr _raw_target_segment = std::make_shared<TargetSegment>();
        _raw_target_segment->target_rowset = target_rowset;
        _raw_target_segment->segmentid = hdr.segment_id();

        if (hdr.opcode() == PStreamHeader::OPEN_FILE) {
            std::lock_guard<std::mutex> l(_rawsegment_finalsegment_map_lock);
            DCHECK(_rawsegment_finalsegment_map.find(_raw_target_segment) ==
                   _rawsegment_finalsegment_map.end());
            TargetSegmentPtr _final_target_segment = std::make_shared<TargetSegment>();
            _final_target_segment->target_rowset = target_rowset;
            uint64_t final_segmentid = get_next_segmentid(target_rowset);
            _final_target_segment->segmentid = final_segmentid;
            _rawsegment_finalsegment_map[_raw_target_segment] = _final_target_segment;
        }
        {
            std::lock_guard<std::mutex> l(_rawsegment_finalsegment_map_lock);
            DCHECK(_rawsegment_finalsegment_map.find(_raw_target_segment) !=
                   _rawsegment_finalsegment_map.end());
            target_segment = _rawsegment_finalsegment_map[_raw_target_segment];
            DCHECK(target_segment.get());
        }

        std::shared_ptr<ThreadPoolToken> token = nullptr;
        {
            std::lock_guard<std::mutex> l(_segment_token_map_lock);
            // serialize OPs on same file: open, write1, write2, ... , close
            if (_segment_token_map.find(target_segment) == _segment_token_map.end()) {
                _segment_token_map[target_segment] =
                        _workers->new_token(ThreadPool::ExecutionMode::SERIAL);
            }

            token = _segment_token_map[target_segment];
        }
        auto s = token->submit_func([this, id, hdr, target_rowset, target_segment, index_stream, rowset_builder, messageBuf]() {
            _handle_message(id, hdr, target_rowset, target_segment, index_stream, rowset_builder, messageBuf);
        });
        if (s != Status::OK()) {
            LOG(WARNING) << "Failed to submit task to threadpool, reason: " << s;
            return -1; // TODO handle it ?
        }
        LOG(INFO) << "OOXXOO target_segment: " << target_segment->to_string()
                  << " submitted to threadpool via token: " << token.get();
    }
    return 0;
}

void LoadStream::on_idle_timeout(StreamId id) {}

void LoadStream::on_closed(StreamId id) {
    auto env = doris::ExecEnv::GetInstance();
    StreamIdPtr id_ptr = std::make_shared<StreamId>(id);
    env->get_sink_stream_mgr()->release_stream_id(id_ptr);
}

} // namespace doris
