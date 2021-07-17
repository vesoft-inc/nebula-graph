/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <bits/c++config.h>
#include <folly/Benchmark.h>

#include "context/Iterator.h"
#include "context/QueryExpressionContext.h"
#include "context/ExecutionContext.h"
#include "common/expression/PropertyExpression.h"
#include "common/datatypes/DataSet.h"

namespace nebula {
namespace graph {

static ExecutionContext ec;
static QueryExpressionContext qec(&ec);
static ObjectPool pool;
static Expression *expr = InputPropertyExpression::make(&pool, "col1");

static std::size_t inputVarPropEval(std::size_t iters) {
    for (std::size_t i = 0; i < iters; ++i) {
        auto v = expr->eval(qec);
        folly::doNotOptimizeAway(v);
    }
    return iters;
}

BENCHMARK_NAMED_PARAM_MULTI(inputVarPropEval, test)

}  // namespace graph
}  // namespace nebula

int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);
    nebula::DataSet ds({"col0", "col1", "col2", "col4"});
    ds.emplace_back(nebula::Row({0, 1, 2, 3}));
    auto result = nebula::graph::ResultBuilder().value(std::move(ds)).finish();
    nebula::graph::qec(result.iterRef());

    folly::runBenchmarks();
    return 0;
}
