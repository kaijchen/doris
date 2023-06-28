#pragma once

#include <stdint.h>

#include "common/status.h"
#include "olap/delta_writer.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "util/countdown_latch.h"

namespace doris {
class VOlapTableSinkV2Mgr {
public:
    VOlapTableSinkV2Mgr();
    ~VOlapTableSinkV2Mgr();

    Status init(int64_t process_mem_limit);

    // check if the total mem consumption exceeds limit.
    // If yes, it will flush memtable to try to reduce memory consumption.
    void handle_memtable_flush();

    void register_writer(std::shared_ptr<DeltaWriter> writer);

    void deregister_writer(std::shared_ptr<DeltaWriter> writer);

private:
    void _refresh_mem_tracker();

    std::mutex _lock;
    // If hard limit reached, one thread will trigger load channel flush,
    // other threads should wait on the condition variable.
    bool _should_wait_flush = false;
    std::condition_variable _wait_flush_cond;

    std::unique_ptr<MemTrackerLimiter> _mem_tracker;
    int64_t _load_hard_mem_limit = -1;
    int64_t _load_soft_mem_limit = -1;
    bool _soft_reduce_mem_in_progress = false;

    std::unordered_set<std::shared_ptr<DeltaWriter>> _writers;
};
} // namespace doris