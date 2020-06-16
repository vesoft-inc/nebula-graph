/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/SortExecutor.h"
#include "context/ExpressionContextImpl.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

folly::Future<Status> SortExecutor::execute() {
    dumpLog();
    auto* sort = asNode<Sort>(node());
    auto iter = ectx_->getResult(sort->inputVar()).iter();

    if (iter == nullptr) {
        return finish(ExecResult::buildDefault(Value()));
    }
    auto result = ExecResult::buildDefault(ectx_->getResult(sort->inputVar()).copyValue());
    ExpressionContextImpl ctx(ectx_, iter.get());
    iter->sort(sort->factors());
    result.setIter(std::move(iter));
    return finish(std::move(result));
}

}   // namespace graph
}   // namespace nebula
