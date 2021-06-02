/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/LimitExecutor.h"
#include "planner/plan/Query.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> LimitExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto* limit = asNode<Limit>(node());
    Result result = ectx_->getResult(limit->inputVar());
    ResultBuilder builder;
    builder.value(result.valuePtr());
    auto offset = limit->offset();
    auto count = limit->count();
    auto size = result.iterRef()->size();
    if (size <= static_cast<size_t>(offset)) {
        result.iterRef()->clear();
    } else if (size > static_cast<size_t>(offset + count)) {
        result.iterRef()->eraseRange(0, offset);
        result.iterRef()->eraseRange(count, size - offset);
    } else if (size > static_cast<size_t>(offset) &&
               size <= static_cast<size_t>(offset + count)) {
        result.iterRef()->eraseRange(0, offset);
    }
    builder.iter(std::move(result).iter());
    return finish(builder.finish());
}

}   // namespace graph
}   // namespace nebula
