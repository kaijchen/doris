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

#include <brpc/stream.h>
#include <gen_cpp/internal_service.pb.h>

#include <queue>

#include "io/fs/file_writer.h"

namespace doris {

struct RowsetId;

namespace io {
class StreamSinkFileWriter : public FileWriter {
public:
    StreamSinkFileWriter(brpc::StreamId stream);
    ~StreamSinkFileWriter() override;

    static void deleter(void* data) { ::free(data); }

    static Status send_with_retry(brpc::StreamId stream, butil::IOBuf buf);

    void init(PUniqueId load_id, int64_t partition_id, int64_t index_id, int64_t tablet_id,
              int32_t segment_id);

    Status appendv(OwnedSlice* data, size_t data_cnt) override;

    virtual Status appendv(const Slice* data, size_t data_cnt) override {
        CHECK(false);
        return Status::OK();
    }

    Status finalize() override;

    Status close() override;

    Status abort() override;

    Status write_at(size_t offset, const Slice& data) override;

private:
    Status _stream_sender(butil::IOBuf buf) const { return send_with_retry(_stream, buf); }
    Status _flush_pending_slices(bool eos);

private:
    std::queue<OwnedSlice> _pending_slices;
    size_t _max_pending_bytes = config::streamsink_filewriter_batchsize;
    size_t _pending_bytes;

    brpc::StreamId _stream;

    PUniqueId _load_id;
    int64_t _partition_id;
    int64_t _index_id;
    int64_t _tablet_id;
    int32_t _segment_id;
};

} // namespace io
} // namespace doris
