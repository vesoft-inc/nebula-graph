/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/DedupExecutor.h"
#include "planner/plan/Query.h"
#include "context/QueryExpressionContext.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {
folly::Future<Status> DedupExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto* dedup = asNode<Dedup>(node());
    DCHECK(!dedup->inputVar().empty());
    Result result = ectx_->getResult(dedup->inputVar());

    if (UNLIKELY(result.iterRef() == nullptr)) {
        return Status::Error("Internal Error: iterator is nullptr");
    }

    if (UNLIKELY(result.iterRef()->isGetNeighborsIter())) {
        auto e = Status::Error("Invalid iterator kind, %d",
                               static_cast<uint16_t>(result.iterRef()->kind()));
        LOG(ERROR) << e;
        return e;
    }
    std::unordered_set<const Row*> unique;
    unique.reserve(result.iterRef()->size());
    while (result.iterRef()->valid()) {
        if (!unique.emplace(result.iterRef()->row()).second) {
            result.iterRef()->unstableErase();
        } else {
            result.iterRef()->next();
        }
    }
    result.iterRef()->reset();
    return finish(std::move(result));
}

}   // namespace graph
}   // namespace nebula
