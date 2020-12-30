/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/SetExecutor.h"

#include <sstream>

#include "common/datatypes/DataSet.h"
#include "context/ExecutionContext.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

Status SetExecutor::checkInputDataSets() {
    auto setNode = asNode<SetOp>(node());
    leftVar_ = setNode->leftInputVar();
    rightVar_ = setNode->rightInputVar();
    const auto& leftVersionExpr = setNode->leftVersionExpr();
    const auto& rightVersionExpr = setNode->rightVersionExpr();

    QueryExpressionContext ctx(ectx_);
    auto leftVersionValue = leftVersionExpr->eval(ctx);
    DCHECK(leftVersionValue.isInt());
    leftVersion_ = leftVersionValue.getInt();

    auto rightVersionValue = rightVersionExpr->eval(ctx);
    DCHECK(rightVersionValue.isInt());
    rightVersion_ = rightVersionValue.getInt();

    VLOG(1) << "lhs hist size: " << ectx_->getHistory(leftVar_).size();
    VLOG(1) << "rhs hist size: " << ectx_->getHistory(rightVar_).size();
    VLOG(1) << "left version: " << leftVersion_ << " right version: " << rightVersion_;

    auto lIter = ectx_->getVersionedResult(leftVar_, leftVersion_).iter();
    auto rIter = ectx_->getVersionedResult(rightVar_, rightVersion_).iter();

    if (UNLIKELY(lIter->kind() == Iterator::Kind::kGetNeighbors ||
                 rIter->kind() == Iterator::Kind::kGetNeighbors)) {
        std::stringstream ss;
        ss << "Invalid iterator kind: " << lIter->kind() << " vs. " << rIter->kind();
        return Status::Error(ss.str());
    }

    auto leftData = lIter->valuePtr();
    auto rightData = rIter->valuePtr();

    if (UNLIKELY(!leftData || !rightData)) {
        return Status::Error("SET related executor failed, %s side input dataset is null",
                             !leftData ? "left" : "right");
    }

    if (UNLIKELY(!leftData->isDataSet() || !rightData->isDataSet())) {
        std::stringstream ss;
        ss << "Invalid data types of dependencies: " << leftData->type() << " vs. "
           << rightData->type() << ".";
        return Status::Error(ss.str());
    }

    auto& lds = leftData->getDataSet();
    auto& rds = rightData->getDataSet();

    if (LIKELY(lds.colNames == rds.colNames)) {
        return Status::OK();
    }

    auto lcols = folly::join(",", lds.colNames);
    auto rcols = folly::join(",", rds.colNames);
    return Status::Error(
        "Datasets have different columns: <%s> vs. <%s>", lcols.c_str(), rcols.c_str());
}

std::unique_ptr<Iterator> SetExecutor::getLeftInputDataIter() const {
    return ectx_->getVersionedResult(leftVar_, leftVersion_).iter();
}

std::unique_ptr<Iterator> SetExecutor::getRightInputDataIter() const {
    return ectx_->getVersionedResult(rightVar_, rightVersion_).iter();
}

}   // namespace graph
}   // namespace nebula
