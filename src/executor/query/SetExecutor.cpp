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

GraphStatus SetExecutor::checkInputDataSets() {
    auto setNode = asNode<SetOp>(node());

    auto lIter = ectx_->getResult(setNode->leftInputVar()).iter();
    auto rIter = ectx_->getResult(setNode->rightInputVar()).iter();

    if (UNLIKELY(lIter->kind() == Iterator::Kind::kGetNeighbors ||
                 rIter->kind() == Iterator::Kind::kGetNeighbors)) {
        std::stringstream ss;
        ss << "Invalid iterator kind: " << lIter->kind() << " vs. " << rIter->kind();
        return GraphStatus::setInternalError(ss.str());
    }

    auto leftData = lIter->valuePtr();
    auto rightData = rIter->valuePtr();

    if (UNLIKELY(!leftData || !rightData)) {
        return GraphStatus::setInternalError(
                folly::stringPrintf("SET related executor failed, %s side input dataset is null",
                                     !leftData ? "left" : "right"));
    }

    if (UNLIKELY(!leftData->isDataSet() || !rightData->isDataSet())) {
        std::stringstream ss;
        ss << "Invalid data types of dependencies: " << leftData->type() << " vs. "
           << rightData->type();
        return GraphStatus::setInternalError(ss.str());
    }

    auto& lds = leftData->getDataSet();
    auto& rds = rightData->getDataSet();

    if (LIKELY(lds.colNames == rds.colNames)) {
        return GraphStatus::OK();
    }

    auto lcols = folly::join(",", lds.colNames);
    auto rcols = folly::join(",", rds.colNames);
    return GraphStatus::setInternalError(folly::stringPrintf(
        "Datasets have different columns: <%s> vs. <%s>", lcols.c_str(), rcols.c_str()));
}

std::unique_ptr<Iterator> SetExecutor::getLeftInputDataIter() const {
    auto left = asNode<SetOp>(node())->leftInputVar();
    return ectx_->getResult(left).iter();
}

std::unique_ptr<Iterator> SetExecutor::getRightInputDataIter() const {
    auto right = asNode<SetOp>(node())->rightInputVar();
    return ectx_->getResult(right).iter();
}

}   // namespace graph
}   // namespace nebula
