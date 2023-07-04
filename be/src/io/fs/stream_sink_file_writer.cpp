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

#include "io/fs/stream_sink_file_writer.h"

#include <gen_cpp/internal_service.pb.h>

#include "olap/olap_common.h"

namespace doris {
namespace io {

StreamSinkFileWriter::StreamSinkFileWriter(brpc::StreamId stream_id) : _stream(stream_id) {}

StreamSinkFileWriter::~StreamSinkFileWriter() {}

void StreamSinkFileWriter::init(PUniqueId load_id, int64_t partition_id, int64_t index_id,
                                int64_t tablet_id, int32_t segment_id) {
    LOG(INFO) << "init stream writer, load id(" << UniqueId(load_id).to_string()
              << "), partition id(" << partition_id << "), index id(" << index_id << "), tablet_id("
              << tablet_id << "), segment_id(" << segment_id << ")";
    _load_id = load_id;
    _partition_id = partition_id;
    _index_id = index_id;
    _tablet_id = tablet_id;
    _segment_id = segment_id;
}

Status StreamSinkFileWriter::appendv(OwnedSlice* data, size_t data_cnt) {
    size_t bytes_req = 0;
    for (int i = 0; i < data_cnt; i++) {
        bytes_req += data[i].slice().get_size();
        _pending_slices.emplace(std::move(data[i]));
    }
    _pending_bytes += bytes_req;

    LOG(INFO) << "writer appendv, load_id: " << UniqueId(_load_id).to_string()
              << ", index_id: " << _index_id << ", tablet_id: " << _tablet_id
              << ", segment_id: " << _segment_id << ", data_length: " << bytes_req;

    if (_pending_bytes >= _max_pending_bytes) {
        RETURN_IF_ERROR(_flush_pending_slices(false));
    }

    LOG(INFO) << "current batched bytes: " << _pending_bytes;
    return Status::OK();
}

Status StreamSinkFileWriter::_flush_pending_slices(bool eos) {
    PStreamHeader header;
    header.set_allocated_load_id(&_load_id);
    header.set_partition_id(_partition_id);
    header.set_index_id(_index_id);
    header.set_tablet_id(_tablet_id);
    header.set_segment_id(_segment_id);
    header.set_segment_eos(eos);
    header.set_opcode(doris::PStreamHeader::APPEND_DATA);
    size_t header_len = header.ByteSizeLong();

    butil::IOBuf buf;
    buf.append(reinterpret_cast<uint8_t*>(&header_len), sizeof(header_len));
    buf.append(header.SerializeAsString());

    size_t bytes_req = 0;
    while (!_pending_slices.empty()) {
        OwnedSlice owend_slice = std::move(_pending_slices.front());
        _pending_slices.pop();
        Slice slice = owend_slice.slice();
        bytes_req += slice.get_size();
        buf.append_user_data(const_cast<void*>(static_cast<const void*>(slice.get_data())),
                             slice.get_size(), deleter);
        owend_slice.release();
    }
    _pending_bytes -= bytes_req;
    _bytes_appended += bytes_req;

    LOG(INFO) << "writer flushing, load_id: " << UniqueId(_load_id).to_string()
              << ", index_id: " << _index_id << ", tablet_id: " << _tablet_id
              << ", segment_id: " << _segment_id << ", data_length: " << bytes_req;

    Status status = _stream_sender(buf);
    header.release_load_id();
    return status;
}

Status StreamSinkFileWriter::finalize() {
    LOG(INFO) << "writer finalize, load_id: " << UniqueId(_load_id).to_string()
              << ", index_id: " << _index_id << ", tablet_id: " << _tablet_id
              << ", segment_id: " << _segment_id;
    return _flush_pending_slices(true);
}

Status StreamSinkFileWriter::send_with_retry(brpc::StreamId stream, butil::IOBuf buf) {
    while (true) {
        int ret = brpc::StreamWrite(stream, buf);
        if (ret == EAGAIN) {
            const timespec time = butil::seconds_from_now(60);
            int wait_result = brpc::StreamWait(stream, &time);
            if (wait_result == 0) {
                continue;
            } else {
                return Status::InternalError("fail to send data when wait stream");
            }
        } else if (ret == EINVAL) {
            return Status::InternalError("fail to send data when stream write");
        } else {
            return Status::OK();
        }
    }
}

Status StreamSinkFileWriter::abort() {
    return Status::OK();
}

Status StreamSinkFileWriter::close() {
    return Status::OK();
}

Status StreamSinkFileWriter::write_at(size_t offset, const Slice& data) {
    return Status::OK();
}
} // namespace io
} // namespace doris
