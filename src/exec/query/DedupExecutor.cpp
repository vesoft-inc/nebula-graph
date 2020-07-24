/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/DedupExecutor.h"
#include "planner/Query.h"
#include "context/QueryExpressionContext.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {
folly::Future<Status> DedupExecutor::execute() {
    SCOPED_TIMER(&execTimes_);
    auto* dedup = asNode<Dedup>(node());
    DCHECK(!dedup->inputVar().empty());
    auto iter = ectx_->getResult(dedup->inputVar()).iter();

    if (UNLIKELY(iter == nullptr)) {
        return Status::Error("Internal Error: iterator is nullptr");
    }

    if (UNLIKELY(iter->isGetNeighborsIter())) {
        auto e = Status::Error("Invalid iterator kind, %d", static_cast<uint16_t>(iter->kind()));
        LOG(ERROR) << e;
        return e;
    }
    ResultBuilder builder;
    builder.value(iter->valuePtr());
    QueryExpressionContext ctx(ectx_, iter.get());
    std::unordered_set<const LogicalRow*> unique;
    while (iter->valid()) {
        if (unique.find(iter->row()) != unique.end()) {
            iter->erase();
        } else {
            unique.emplace(iter->row());
            iter->next();
        }
    }
    iter->reset();
    builder.iter(std::move(iter));
    return finish(builder.finish());
}

}   // namespace graph
}   // namespace nebula
