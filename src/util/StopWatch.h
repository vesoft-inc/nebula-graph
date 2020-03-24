/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTIL_STOPWATCH_H_
#define UTIL_STOPWATCH_H_

#include <atomic>
#include <chrono>

namespace nebula {
namespace graph {

// This stop watch is NOT thread safe. Please ensure that you don't use it in multi-threads.
class StopWatch {
public:
    explicit StopWatch(std::chrono::nanoseconds& ns) : ns_(ns) {}

    ~StopWatch() {
        stop();
    }

    void start() {
        if (!started_) {
            started_ = true;
            startPoint_ = ClockType::now();
        }
    }

    void stop() {
        if (!stopped_) {
            stopped_ = true;
            ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(ClockType::now() -
                                                                        startPoint_);
        }
    }

private:
    using ClockType = std::chrono::steady_clock;

    bool started_{false};
    bool stopped_{false};

    std::chrono::nanoseconds& ns_;
    ClockType::time_point startPoint_;
};

class ScopedTimer final : public StopWatch {
public:
    explicit ScopedTimer(std::chrono::nanoseconds& ns) : StopWatch(ns) {
        start();
    }

private:
    using StopWatch::start;
    using StopWatch::stop;
};

}   // namespace graph
}   // namespace nebula

#endif   // UTIL_STOPWATCH_H_
