/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/IntersectExecutor.h"

#include <unordered_set>

#include "planner/plan/PlanNode.h"
#include "planner/plan/Query.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> IntersectExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    NG_RETURN_IF_ERROR(checkInputDataSets());

    auto left = getLeftInputData();
    auto right = getRightInputData();

    std::unordered_set<const Row *> hashSet;
    for (; right.iterRef()->valid(); right.iterRef()->next()) {
        hashSet.insert(right.iterRef()->row());
        // TODO: should test duplicate rows
    }

    ResultBuilder builder;
    if (hashSet.empty()) {
        auto value = left.iterRef()->valuePtr();
        DataSet ds;
        ds.colNames = value->getDataSet().colNames;
        builder.value(Value(std::move(ds))).iter(Iterator::Kind::kSequential);
        return finish(builder.finish());
    }

    while (left.iterRef()->valid()) {
        auto iter = hashSet.find(left.iterRef()->row());
        if (iter == hashSet.end()) {
            left.iterRef()->unstableErase();
        } else {
            left.iterRef()->next();
        }
    }

    builder.values(left.values()).iter(std::move(left).iter());
    return finish(builder.finish());
}

}   // namespace graph
}   // namespace nebula
