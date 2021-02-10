/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/MinusExecutor.h"

#include <unordered_set>

#include "planner/Query.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> MinusExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    NG_RETURN_IF_ERROR(checkInputDataSets());

    auto lIter = getLeftInputDataIter();
    auto rIter = getRightInputDataIter();

    std::unordered_set<const LogicalRow *> hashSet;
    for (auto cur = rIter->begin(); rIter->valid(cur); ++cur) {
        hashSet.insert(cur->get());
        // TODO: should test duplicate rows
    }

    if (!hashSet.empty()) {
        auto cur = lIter->begin();
        while (lIter->valid(cur)) {
            auto iter = hashSet.find(cur->get());
            if (iter == hashSet.end()) {
                ++cur;
            } else {
                cur = lIter->unstableErase(cur);
            }
        }
    }

    ResultBuilder builder;
    builder.value(lIter->valuePtr()).iter(std::move(lIter));
    return finish(builder.finish());
}

}   // namespace graph
}   // namespace nebula
