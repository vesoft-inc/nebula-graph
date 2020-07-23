/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTIL_SCOPEDTIMER_H_
#define UTIL_SCOPEDTIMER_H_

#include <functional>

#include "common/base/Logging.h"
#include "common/time/Duration.h"

namespace nebula {
namespace graph {

// This implementation is not thread-safety, please ensure that one scoped timer would NOT be
// used by multi-threads at same time.
class ScopedTimer final {
public:
    explicit ScopedTimer(std::function<void(uint64_t)> callback)
        : duration_(), callback_(std::move(callback)) {
        start();
    }

    explicit ScopedTimer(uint64_t *value)
        : duration_(),
          callback_([value](uint64_t elapsedTime) { *DCHECK_NOTNULL(value) = elapsedTime; }) {
        start();
    }

    ~ScopedTimer() {
        stop();
    }

    void start() {
        duration_.reset();
    }

    void stop() {
        if (stopped_) return;
        stopped_ = true;
        callback_(duration_.elapsedInUSec());
    }

private:
    bool stopped_{false};
    time::Duration duration_;
    std::function<void(uint64_t)> callback_;
};

}   // namespace graph
}   // namespace nebula

#define SCOPED_TIMER(value) ::nebula::graph::ScopedTimer scopedTimer##__LINE__(&value)

#endif   // UTIL_SCOPEDTIMER_H_
