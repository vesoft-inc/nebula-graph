/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <memory>

#include <folly/Benchmark.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/executors/thread_factory/NamedThreadFactory.h>
#include <folly/init/Init.h>

#include "common/datatypes/DataSet.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Value.h"
#include "util/AsyncDropper.h"

namespace nebula {
namespace graph {

auto ioThreadPool = std::make_unique<folly::IOThreadPoolExecutor>(
    10,
    std::make_shared<folly::NamedThreadFactory>("async-dropper-bm"));

auto dropper = AsyncDropper(ioThreadPool.get());

static DataSet largeData(std::size_t rows) {
    DataSet d({"col1", "col2", "col3", "col4", "col5"});
    d.rows.reserve(rows);
    for (std::size_t i = 0; i < rows; ++i) {
        d.emplace_back(
            Row({1, true, "Nebula Graph", "Vesoft Inc.", List({"qwuhgorhgohg", 1, true})}));
    }
    return d;
}

static inline void syncDrop(DataSet&&) {}

static size_t inlineRelease(size_t iters, std::size_t rows) {
    for (size_t i = 0; i < iters; ++i) {
        DataSet data;
        BENCHMARK_SUSPEND {
            data = largeData(rows);
        }
        syncDrop(std::move(data));
    }
    return iters;
}

static size_t asyncRelease(size_t iters, std::size_t rows) {
    for (size_t i = 0; i < iters; ++i) {
        DataSet data;
        BENCHMARK_SUSPEND {
            data = largeData(rows);
        }
        dropper.drop(std::move(data));
    }
    return iters;
}

BENCHMARK_PARAM_MULTI(inlineRelease, 512)
BENCHMARK_RELATIVE_PARAM_MULTI(asyncRelease, 512)
BENCHMARK_DRAW_LINE();
BENCHMARK_PARAM_MULTI(inlineRelease, 1024)
BENCHMARK_RELATIVE_PARAM_MULTI(asyncRelease, 1024)
BENCHMARK_DRAW_LINE();
BENCHMARK_PARAM_MULTI(inlineRelease, 10240)
BENCHMARK_RELATIVE_PARAM_MULTI(asyncRelease, 10240)
BENCHMARK_DRAW_LINE();

}   // namespace graph
}   // namespace nebula

int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);
    folly::runBenchmarks();

    return 0;
}
