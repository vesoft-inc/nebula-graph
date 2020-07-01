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
        return Status::Error("Internal error: nullptr iterator in sort executor");
    }
    if (iter->kind() == Iterator::Kind::kGetNeighbors ||
            iter->kind() == Iterator::Kind::kUnion) {
        return Status::Error("Invalid iterator kind, %d", static_cast<uint8_t>(iter->kind()));
    }
    auto result = ExecResult::buildDefault(iter->valuePtr());
    ExpressionContextImpl ctx(ectx_, iter.get());
    iter->sort(sort->factors());
    result.setIter(std::move(iter));
    return finish(std::move(result));
}

}   // namespace graph
}   // namespace nebula
