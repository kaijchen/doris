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

void StreamSinkFileWriter::init(PUniqueId load_id, int64_t index_id, int64_t tablet_id,
                                int32_t segment_id, int32_t schema_hash) {
    LOG(INFO) << "init stream writer, load id(" << UniqueId(load_id).to_string() << "), index id("
              << index_id << "), tablet_id(" << tablet_id << "), segment_id("
              << segment_id << "), schema_hash(" << schema_hash << ")";
    _load_id = load_id;
    _index_id = index_id;
    _tablet_id = tablet_id;
    _segment_id = segment_id;
}

Status StreamSinkFileWriter::appendv(const OwnedSlice* data, size_t data_cnt) {
    size_t bytes_req = 0;
    for (int i = 0; i < data_cnt; i++) {
        bytes_req += data[i].slice().get_size();
    }
    LOG(INFO) << "writer appendv, load_id: " << UniqueId(_load_id).to_string()
              << ", index_id: " << _index_id << ", tablet_id: " << _tablet_id
              << ", segment_id: " << _segment_id << ", data_length: " << bytes_req;
    butil::IOBuf buf;
    PStreamHeader header;
    header.set_allocated_load_id(&_load_id);
    header.set_index_id(_index_id);
    header.set_tablet_id(_tablet_id);
    header.set_segment_id(_segment_id);
    header.set_opcode(doris::PStreamHeader::APPEND_DATA);
    size_t header_len = header.ByteSizeLong();

    buf.append(reinterpret_cast<uint8_t*>(&header_len), sizeof(header_len));
    buf.append(header.SerializeAsString());
    for (int i = 0; i < data_cnt; i++) {
        Slice slice = data[i].slice();
        buf.append_user_data(const_cast<void*>(static_cast<const void*>(slice.get_data())),
                             slice.get_size(), deleter);
        (const_cast<OwnedSlice*>(&data[i]))->release();
    }

    _bytes_appended += bytes_req;
    Status status = _stream_sender(buf);
    header.release_load_id();
    return status;
}

Status StreamSinkFileWriter::finalize() {
    LOG(INFO) << "writer finalize, load_id: " << UniqueId(_load_id).to_string()
              << ", index_id: " << _index_id << ", tablet_id: " << _tablet_id
              << ", segment_id: " << _segment_id;
    butil::IOBuf buf;
    PStreamHeader header;
    header.set_allocated_load_id(&_load_id);
    header.set_index_id(_index_id);
    header.set_tablet_id(_tablet_id);
    header.set_segment_id(_segment_id);
    header.set_opcode(doris::PStreamHeader::APPEND_DATA);
    header.set_segment_eos(true);
    size_t header_len = header.ByteSizeLong();

    LOG(INFO) << "segment_size: " << _bytes_appended;

    buf.append(reinterpret_cast<uint8_t*>(&header_len), sizeof(header_len));
    buf.append(header.SerializeAsString());
    Status status = _stream_sender(buf);
    header.release_load_id();
    return status;
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
