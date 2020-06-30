/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/IntersectExecutor.h"

#include <list>
#include <unordered_set>

#include "planner/PlanNode.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

folly::Future<Status> IntersectExecutor::execute() {
    dumpLog();
    NG_RETURN_IF_ERROR(validateInputDataSets());

    auto lIter = getLeftInputDataIter();
    auto rIter = getRightInputDataIter();

    std::unordered_set<const Row *> hashSet;
    for (; rIter->valid(); rIter->next()) {
        auto res = hashSet.insert(rIter->row());
        if (UNLIKELY(!res.second)) {
            LOG(ERROR) << "Fail to insert row into hash table in intersect executor, row: "
                       << *rIter->row();
        }
    }

    while (lIter->valid()) {
        auto iter = hashSet.find(lIter->row());
        if (iter == hashSet.end()) {
            lIter->erase();
        } else {
            lIter->next();
        }
    }

    auto result = ExecResult::buildDefault(lIter->valuePtr());
    result.setIter(std::move(lIter));

    return finish(std::move(result));
}

}   // namespace graph
}   // namespace nebula
