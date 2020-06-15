/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/DedupExecutor.h"
#include "planner/Query.h"
#include "context/ExpressionContextImpl.h"

namespace nebula {
namespace graph {
folly::Future<Status> DedupExecutor::execute() {
    dumpLog();
    auto* dedup = asNode<Dedup>(node());
    auto iter = ectx_->getResult(dedup->inputVar()).iter();

    if (iter == nullptr) {
        return finish(ExecResult::buildDefault(Value()));
    }
    auto exprs = dedup->getExprs();
    auto result = ExecResult::buildDefault(iter->valuePtr());
    ExpressionContextImpl ctx(ectx_, iter.get());
    std::unordered_set<ColVals, ColsHasher> unique;
    while (iter->valid()) {
        ColVals colVals;
        for (auto expr : exprs) {
            auto &value = expr->eval(ctx);
            colVals.cols.emplace_back(&value);
        }
        if (unique.find(colVals) != unique.end()) {
            iter->erase();
        } else {
            iter->next();
            unique.emplace(std::move(colVals));
        }
    }
    iter->reset();
    result.setIter(std::move(iter));
    return finish(std::move(result));
}

}   // namespace graph
}   // namespace nebula
