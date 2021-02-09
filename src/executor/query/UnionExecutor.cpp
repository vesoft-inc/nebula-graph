/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/UnionExecutor.h"

#include "context/ExecutionContext.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> UnionExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    NG_RETURN_IF_ERROR(checkInputDataSets());
    auto left = getLeftInputData();
    auto right = getRightInputData();
    auto iter = std::make_unique<SequentialIter>(std::move(left).iter(), std::move(right).iter());
    return finish(ResultBuilder()
        .values(left.values()).values(right.values()).iter(std::move(iter)).finish());
}

}   // namespace graph
}   // namespace nebula
