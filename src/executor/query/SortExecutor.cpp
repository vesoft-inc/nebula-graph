/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/SortExecutor.h"
#include "common/expression/PropertyExpression.h"
#include "planner/Query.h"
#include "util/ScopedTimer.h"
#include "common/expression/ColumnExpression.h"

namespace nebula {
namespace graph {

folly::Future<Status> SortExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto* sort = asNode<Sort>(node());
    auto iter = ectx_->getResult(sort->inputVar()).iter();
    if (UNLIKELY(iter == nullptr)) {
        return Status::Error("Internal error: nullptr iterator in sort executor");
    }
    if (UNLIKELY(iter->isDefaultIter())) {
        std::string errMsg = "Internal error: Sort executor does not supported DefaultIter";
        LOG(ERROR) << errMsg;
        return Status::Error(errMsg);
    }
    if (UNLIKELY(iter->isGetNeighborsIter())) {
        std::string errMsg = "Internal error: Sort executor does not supported GetNeighborsIter";
        LOG(ERROR) << errMsg;
        return Status::Error(errMsg);
    }

    QueryExpressionContext qctx(ectx_);
    qctx(iter.get());
    auto &factors = sort->factors();
    auto comparator = [&factors, &qctx](const std::shared_ptr<LogicalRow> lhs,
                                        const std::shared_ptr<LogicalRow> rhs) {
        for (auto &item : factors) {
            auto index = item.first;
            auto expr = std::make_unique<ColumnExpression>(index);
            auto orderType = item.second;
            auto lhsVal = expr->eval(qctx(lhs.get()));
            auto rhsVal = expr->eval(qctx(rhs.get()));
            if (lhsVal == rhsVal) {
                continue;
            }

            if (orderType == OrderFactor::OrderType::ASCEND) {
                return lhsVal < rhsVal;
            } else if (orderType == OrderFactor::OrderType::DESCEND) {
                return lhsVal > rhsVal;
            }
        }
        return false;
    };

    std::sort(iter->begin(), iter->end(), comparator);
    return finish(ResultBuilder().value(iter->valuePtr()).iter(std::move(iter)).finish());
}

}   // namespace graph
}   // namespace nebula
