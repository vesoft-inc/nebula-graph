/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/MinusExecutor.h"

#include <unordered_set>

#include "planner/Query.h"

namespace nebula {
namespace graph {

folly::Future<Status> MinusExecutor::execute() {
    dumpLog();

    NG_RETURN_IF_ERROR(checkInputDataSets());

    auto lIter = getLeftInputDataIter();
    auto rIter = getRightInputDataIter();

    std::unordered_set<const LogicalRow *> hashSet;
    for (; rIter->valid(); rIter->next()) {
        auto iter = hashSet.insert(rIter->row());
        if (UNLIKELY(!iter.second)) {
            LOG(ERROR) << "Fail to insert row into hash table in minus executor, row: "
                       << *rIter->row();
        }
    }

    if (!hashSet.empty()) {
        while (lIter->valid()) {
            auto iter = hashSet.find(lIter->row());
            if (iter == hashSet.end()) {
                lIter->next();
            } else {
                lIter->erase();
            }
        }
    }

    ResultBuilder builder;
    builder.value(lIter->valuePtr()).iter(std::move(lIter));
    return finish(builder.finish());
}

}   // namespace graph
}   // namespace nebula
