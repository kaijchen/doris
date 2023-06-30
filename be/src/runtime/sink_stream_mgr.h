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
#include <io/fs/local_file_writer.h>

#include <fstream>
#include <iostream>

#include "butil/iobuf.h"
#include "olap/olap_common.h"
#include "util/threadpool.h"

namespace doris {
using namespace brpc;
using StreamIdPtr = std::shared_ptr<StreamId>;

// managing stream_id allocation and release
class SinkStreamMgr {
public:
    SinkStreamMgr();
    ~SinkStreamMgr();

    StreamIdPtr get_free_stream_id();
    void release_stream_id(StreamIdPtr id);

private:
    std::vector<StreamIdPtr> _free_stream_ids;
    std::mutex _lock;
};

} // namespace doris
