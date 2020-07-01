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
        return Status::Error("Internal Error: iterator is nullptr");
    }

    if (iter->kind() == Iterator::Kind::kGetNeighbors ||
            iter->kind() == Iterator::Kind::kUnion) {
        return Status::Error("Invalid iterator kind, %d", static_cast<uint16_t>(iter->kind()));
    }
    auto result = ExecResult::buildDefault(iter->valuePtr());
    auto &colNames = dedup->getColNames();
    ExpressionContextImpl ctx(ectx_, iter.get());
    std::unordered_set<ValueRefs> unique;
    while (iter->valid()) {
        ValueRefs valRefs;
        for (auto &colName : colNames) {
            auto &value = iter->getColumn(colName);
            valRefs.values.emplace_back(&value);
        }
        if (unique.find(valRefs) != unique.end()) {
            iter->erase();
        } else {
            iter->next();
            unique.emplace(std::move(valRefs));
        }
    }
    iter->reset();
    result.setIter(std::move(iter));
    return finish(std::move(result));
}

}   // namespace graph
}   // namespace nebula
