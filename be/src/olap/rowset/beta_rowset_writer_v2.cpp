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

#include "olap/rowset/beta_rowset_writer_v2.h"

#include <assert.h>
// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <stdio.h>

#include <ctime> // time
#include <filesystem>
#include <memory>
#include <sstream>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "gutil/integral_types.h"
#include "gutil/strings/substitute.h"
#include "io/fs/file_reader_options.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/stream_sink_file_writer.h"
#include "olap/data_dir.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_schema.h"
#include "util/slice.h"
#include "util/time.h"
#include "vec/common/schema_util.h" // LocalSchemaChangeRecorder
#include "vec/core/block.h"

namespace doris {
using namespace ErrorCode;

BetaRowsetWriterV2::BetaRowsetWriterV2(const std::vector<brpc::StreamId>& streams)
        : _rowset_meta(nullptr),
          _next_segment_id(0),
          _num_segment(0),
          _segment_writer(nullptr),
          _num_rows_written(0),
          _total_data_size(0),
          _total_index_size(0),
          _raw_num_rows_written(0),
          _streams(streams) {}

BetaRowsetWriterV2::~BetaRowsetWriterV2() {
    // TODO(lingbin): Should wrapper exception logic, no need to know file ops directly.
    if (!_already_built) {       // abnormal exit, remove all files generated
        _segment_writer.reset(); // ensure all files are closed

        // TODO: send signal to remote to cleanup segments
    }
}

Status BetaRowsetWriterV2::init(const RowsetWriterContext& rowset_writer_context) {
    _context = rowset_writer_context;
    _rowset_meta.reset(new RowsetMeta);
    _rowset_meta->set_rowset_id(_context.rowset_id);
    _index_id = _context.index_id;
    _rowset_meta->set_partition_id(_context.partition_id);
    _rowset_meta->set_tablet_id(_context.tablet_id);
    _rowset_meta->set_tablet_schema_hash(_context.tablet_schema_hash);
    _rowset_meta->set_rowset_type(_context.rowset_type);
    _rowset_meta->set_rowset_state(_context.rowset_state);
    _rowset_meta->set_segments_overlap(_context.segments_overlap);
    if (_context.rowset_state == PREPARED || _context.rowset_state == COMMITTED) {
        _is_pending = true;
        _rowset_meta->set_txn_id(_context.txn_id);
        _rowset_meta->set_load_id(_context.load_id);
    } else {
        _rowset_meta->set_version(_context.version);
        _rowset_meta->set_newest_write_timestamp(_context.newest_write_timestamp);
    }
    _rowset_meta->set_tablet_uid(_context.tablet_uid);
    _rowset_meta->set_tablet_schema(_context.tablet_schema);

    return Status::OK();
}

Status BetaRowsetWriterV2::_do_add_block(const vectorized::Block* block,
                                         std::unique_ptr<segment_v2::SegmentWriter>* segment_writer,
                                         size_t row_offset, size_t input_row_num) {
    auto s = (*segment_writer)->append_block(block, row_offset, input_row_num);
    if (UNLIKELY(!s.ok())) {
        LOG(WARNING) << "failed to append block: " << s.to_string();
        return Status::Error<WRITER_DATA_WRITE_ERROR>();
    }
    return Status::OK();
}

Status BetaRowsetWriterV2::_add_block(const vectorized::Block* block,
                                      std::unique_ptr<segment_v2::SegmentWriter>* segment_writer,
                                      const FlushContext* flush_ctx) {
    size_t block_size_in_bytes = block->bytes();
    size_t block_row_num = block->rows();
    size_t row_avg_size_in_bytes = std::max((size_t)1, block_size_in_bytes / block_row_num);
    size_t row_offset = 0;

    if (flush_ctx != nullptr && flush_ctx->segment_id.has_value()) {
        // the entire block (memtable) should be flushed into single segment
        RETURN_IF_ERROR(_do_add_block(block, segment_writer, 0, block_row_num));
        _raw_num_rows_written += block_row_num;
        return Status::OK();
    }

    do {
        auto max_row_add = (*segment_writer)->max_row_to_add(row_avg_size_in_bytes);
        if (UNLIKELY(max_row_add < 1)) {
            // no space for another single row, need flush now
            RETURN_IF_ERROR(_flush_segment_writer(segment_writer));
            RETURN_IF_ERROR(_create_segment_writer(segment_writer, flush_ctx));
            max_row_add = (*segment_writer)->max_row_to_add(row_avg_size_in_bytes);
            DCHECK(max_row_add > 0);
        }
        size_t input_row_num = std::min(block_row_num - row_offset, size_t(max_row_add));
        RETURN_IF_ERROR(_do_add_block(block, segment_writer, row_offset, input_row_num));
        row_offset += input_row_num;
    } while (row_offset < block_row_num);

    _raw_num_rows_written += block_row_num;
    return Status::OK();
}

Status BetaRowsetWriterV2::flush() {
    if (_segment_writer != nullptr) {
        RETURN_IF_ERROR(_flush_segment_writer(&_segment_writer));
    }
    return Status::OK();
}

Status BetaRowsetWriterV2::flush_single_memtable(const vectorized::Block* block, int64* flush_size,
                                                 const FlushContext* ctx) {
    if (block->rows() == 0) {
        return Status::OK();
    }

    std::unique_ptr<segment_v2::SegmentWriter> writer;
    RETURN_IF_ERROR(_create_segment_writer(&writer, ctx));
    segment_v2::SegmentWriter* raw_writer = writer.get();
    int32_t segment_id = writer->get_segment_id();
    RETURN_IF_ERROR(_add_block(block, &writer, ctx));
    // if segment_id is present in flush context,
    // the entire memtable should be flushed into a single segment
    if (ctx != nullptr && ctx->segment_id.has_value()) {
        DCHECK_EQ(writer->get_segment_id(), segment_id);
        DCHECK_EQ(writer.get(), raw_writer);
    }
    RETURN_IF_ERROR(_flush_segment_writer(&writer, flush_size));
    return Status::OK();
}

RowsetSharedPtr BetaRowsetWriterV2::build() {
    // make sure all segments are flushed
    DCHECK_EQ(_num_segment, _next_segment_id);
    // TODO(lingbin): move to more better place, or in a CreateBlockBatch?
    for (auto& file_writer : _file_writers) {
        Status status = file_writer->close();
        if (!status.ok()) {
            LOG(WARNING) << "failed to close file writer, path=" << file_writer->path()
                         << " res=" << status;
            return nullptr;
        }
    }

    // When building a rowset, we must ensure that the current _segment_writer has been
    // flushed, that is, the current _segment_writer is nullptr
    DCHECK(_segment_writer == nullptr) << "segment must be null when build rowset";
    _build_rowset_meta(_rowset_meta);

    if (_rowset_meta->newest_write_timestamp() == -1) {
        _rowset_meta->set_newest_write_timestamp(UnixSeconds());
    }

    RowsetSharedPtr rowset;
    auto status = RowsetFactory::create_rowset(_context.tablet_schema, _context.rowset_dir,
                                               _rowset_meta, &rowset);
    if (!status.ok()) {
        LOG(WARNING) << "rowset init failed when build new rowset, res=" << status;
        return nullptr;
    }
    _already_built = true;
    return rowset;
}

bool BetaRowsetWriterV2::_is_segment_overlapping(
        const std::vector<KeyBoundsPB>& segments_encoded_key_bounds) {
    std::string last;
    for (auto segment_encode_key : segments_encoded_key_bounds) {
        auto cur_min = segment_encode_key.min_key();
        auto cur_max = segment_encode_key.max_key();
        if (cur_min <= last) {
            return true;
        }
        last = cur_max;
    }
    return false;
}

void BetaRowsetWriterV2::_build_rowset_meta_with_spec_field(
        RowsetMetaSharedPtr rowset_meta, const RowsetMetaSharedPtr& spec_rowset_meta) {
    rowset_meta->set_num_rows(spec_rowset_meta->num_rows());
    rowset_meta->set_total_disk_size(spec_rowset_meta->total_disk_size());
    rowset_meta->set_data_disk_size(spec_rowset_meta->total_disk_size());
    rowset_meta->set_index_disk_size(spec_rowset_meta->index_disk_size());
    // TODO write zonemap to meta
    rowset_meta->set_empty(spec_rowset_meta->num_rows() == 0);
    rowset_meta->set_creation_time(time(nullptr));
    rowset_meta->set_num_segments(spec_rowset_meta->num_segments());
    rowset_meta->set_segments_overlap(spec_rowset_meta->segments_overlap());
    rowset_meta->set_rowset_state(spec_rowset_meta->rowset_state());

    std::vector<KeyBoundsPB> segments_key_bounds;
    spec_rowset_meta->get_segments_key_bounds(&segments_key_bounds);
    rowset_meta->set_segments_key_bounds(segments_key_bounds);
}

void BetaRowsetWriterV2::_build_rowset_meta(std::shared_ptr<RowsetMeta> rowset_meta) {
    int64_t num_seg = _num_segment;
    int64_t num_rows_written = 0;
    int64_t total_data_size = 0;
    int64_t total_index_size = 0;
    std::vector<KeyBoundsPB> segments_encoded_key_bounds;
    {
        std::lock_guard<std::mutex> lock(_segid_statistics_map_mutex);
        for (const auto& itr : _segid_statistics_map) {
            num_rows_written += itr.second.row_num;
            total_data_size += itr.second.data_size;
            total_index_size += itr.second.index_size;
            segments_encoded_key_bounds.push_back(itr.second.key_bounds);
        }
    }
    for (auto itr = _segments_encoded_key_bounds.begin(); itr != _segments_encoded_key_bounds.end();
         ++itr) {
        segments_encoded_key_bounds.push_back(*itr);
    }
    if (!_is_segment_overlapping(segments_encoded_key_bounds)) {
        rowset_meta->set_segments_overlap(NONOVERLAPPING);
    }

    rowset_meta->set_num_segments(num_seg);
    // TODO(zhangzhengyu): key_bounds.size() should equal num_seg, but currently not always
    rowset_meta->set_num_rows(num_rows_written + _num_rows_written);
    rowset_meta->set_total_disk_size(total_data_size + _total_data_size);
    rowset_meta->set_data_disk_size(total_data_size + _total_data_size);
    rowset_meta->set_index_disk_size(total_index_size + _total_index_size);
    rowset_meta->set_segments_key_bounds(segments_encoded_key_bounds);
    // TODO write zonemap to meta
    rowset_meta->set_empty((num_rows_written + _num_rows_written) == 0);
    rowset_meta->set_creation_time(time(nullptr));

    if (_is_pending) {
        rowset_meta->set_rowset_state(COMMITTED);
    } else {
        rowset_meta->set_rowset_state(VISIBLE);
    }
}

Status BetaRowsetWriterV2::_do_create_segment_writer(
        std::unique_ptr<segment_v2::SegmentWriter>* writer, int64_t begin, int64_t end,
        const FlushContext* flush_ctx) {
    std::string path;
    int32_t segment_id = (flush_ctx != nullptr && flush_ctx->segment_id.has_value())
                                 ? flush_ctx->segment_id.value()
                                 : allocate_segment_id();
    path = BetaRowset::segment_file_path(_context.rowset_dir, _context.rowset_id, segment_id);
    io::FileWriterPtr file_writer;
    auto partition_id = _rowset_meta->partition_id();
    auto index_id = _index_id;
    auto tablet_id = _rowset_meta->tablet_id();
    auto load_id = _rowset_meta->load_id();
    auto stream_id = *_streams.begin();

    auto stream_writer = std::make_unique<io::StreamSinkFileWriter>(stream_id);
    stream_writer->init(load_id, partition_id, index_id, tablet_id, segment_id);
    file_writer = std::move(stream_writer);

    DCHECK(file_writer != nullptr);
    segment_v2::SegmentWriterOptions writer_options;
    writer_options.enable_unique_key_merge_on_write = _context.enable_unique_key_merge_on_write;
    writer_options.rowset_ctx = &_context;
    writer_options.write_type = _context.write_type;

    writer->reset(new segment_v2::SegmentWriter(file_writer.get(), segment_id,
                                                _context.tablet_schema, _context.tablet,
                                                _context.data_dir, _context.max_rows_per_segment,
                                                writer_options, _context.mow_context));
    {
        std::lock_guard<SpinLock> l(_lock);
        _file_writers.push_back(std::move(file_writer));
    }
    auto s = (*writer)->init(flush_ctx);
    if (!s.ok()) {
        LOG(WARNING) << "failed to init segment writer: " << s.to_string();
        writer->reset(nullptr);
        return s;
    }

    return Status::OK();
}

Status BetaRowsetWriterV2::_create_segment_writer(
        std::unique_ptr<segment_v2::SegmentWriter>* writer, const FlushContext* flush_ctx) {
    size_t total_segment_num = _num_segment;
    if (UNLIKELY(total_segment_num > config::max_segment_num_per_rowset)) {
        LOG(WARNING) << "too many segments in rowset."
                     << " tablet_id:" << _context.tablet_id << " rowset_id:" << _context.rowset_id
                     << " max:" << config::max_segment_num_per_rowset
                     << " _num_segment:" << _num_segment;
        return Status::Error<TOO_MANY_SEGMENTS>();
    } else {
        return _do_create_segment_writer(writer, -1, -1, flush_ctx);
    }
}

Status BetaRowsetWriterV2::_flush_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>* writer,
                                                 int64_t* flush_size) {
    uint32_t segid = (*writer)->get_segment_id();
    uint32_t row_num = (*writer)->num_rows_written();

    if ((*writer)->num_rows_written() == 0) {
        return Status::OK();
    }
    uint64_t segment_size;
    uint64_t index_size;
    Status s = (*writer)->finalize(&segment_size, &index_size);
    if (!s.ok()) {
        LOG(WARNING) << "failed to finalize segment: " << s.to_string();
        return Status::Error<WRITER_DATA_WRITE_ERROR>();
    }
    VLOG_DEBUG << "tablet_id:" << _context.tablet_id
               << " flushing filename: " << (*writer)->get_data_dir()->path()
               << " rowset_id:" << _context.rowset_id << " segment num:" << _num_segment;

    KeyBoundsPB key_bounds;
    Slice min_key = (*writer)->min_encoded_key();
    Slice max_key = (*writer)->max_encoded_key();
    DCHECK_LE(min_key.compare(max_key), 0);
    key_bounds.set_min_key(min_key.to_string());
    key_bounds.set_max_key(max_key.to_string());

    Statistics segstat;
    segstat.row_num = row_num;
    segstat.data_size = segment_size + (*writer)->get_inverted_index_file_size();
    segstat.index_size = index_size + (*writer)->get_inverted_index_file_size();
    segstat.key_bounds = key_bounds;
    {
        std::lock_guard<std::mutex> lock(_segid_statistics_map_mutex);
        CHECK_EQ(_segid_statistics_map.find(segid) == _segid_statistics_map.end(), true);
        _segid_statistics_map.emplace(segid, segstat);
        _segment_num_rows.resize(_next_segment_id);
        _segment_num_rows[segid] = row_num;
    }
    VLOG_DEBUG << "_segid_statistics_map add new record. segid:" << segid << " row_num:" << row_num
               << " data_size:" << segment_size << " index_size:" << index_size;

    writer->reset();
    if (flush_size) {
        *flush_size = segment_size + index_size;
    }
    {
        std::lock_guard<std::mutex> lock(_segment_set_mutex);
        _segment_set.add(segid);
        while (_segment_set.contains(_num_segment)) {
            _num_segment++;
        }
    }
    return Status::OK();
}

} // namespace doris
