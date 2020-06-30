/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/UnionExecutor.h"

#include "context/ExecutionContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> UnionExecutor::execute() {
    dumpLog();
    NG_RETURN_IF_ERROR(validateInputDataSets());
    auto left = getLeftInputDataIter();
    auto right = getRightInputDataIter();
    auto result = ExecResult::buildDefault(left->valuePtr());
    auto iter = std::make_unique<UnionIterator>(std::move(left), std::move(right));
    result.setIter(std::move(iter));
    return finish(std::move(result));
}

}   // namespace graph
}   // namespace nebula
