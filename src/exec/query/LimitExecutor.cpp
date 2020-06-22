/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/LimitExecutor.h"

#include "context/Iterator.h"
#include "planner/PlanNode.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

folly::Future<Status> LimitExecutor::execute() {
    dumpLog();

    auto limit = asNode<Limit>(node());

    auto &inputResult = ectx_->getResult(limit->inputVar());
    auto iter = inputResult.iter();

    auto offset = limit->offset();
    auto count = limit->count();

    if (iter->size() >= static_cast<size_t>(offset)) {
        for (auto i = 0; i < offset; i++) iter->erase();
        for (auto i = 0; i < count && iter->valid(); i++) iter->next();
    }

    while (iter->valid()) iter->erase();

    auto result = ExecResult::buildSequential(inputResult.valuePtr(), State(State::Stat::kSuccess));
    result.setIter(std::move(iter));
    return finish(std::move(result));
}

}   // namespace graph
}   // namespace nebula
