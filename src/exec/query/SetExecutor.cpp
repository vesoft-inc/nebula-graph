/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/SetExecutor.h"

#include <sstream>

#include "common/datatypes/DataSet.h"
#include "context/ExecutionContext.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

Status SetExecutor::validateInputDataSets() {
    auto setNode = asNode<SetOp>(node());
    auto& lResult = ectx_->getResult(setNode->leftInputVar());
    auto& rResult = ectx_->getResult(setNode->rightInputVar());

    auto& leftData = lResult.iter()->value();
    auto& rightData = rResult.iter()->value();

    if (UNLIKELY(!leftData.isDataSet() || !rightData.isDataSet())) {
        std::stringstream ss;
        ss << "Invalid data types of dependencies: " << leftData.type() << " vs. "
           << rightData.type() << ".";
        return Status::Error(ss.str());
    }

    auto lds = leftData.getDataSet();
    auto rds = rightData.getDataSet();

    if (UNLIKELY(!(lds.colNames == rds.colNames))) {
        auto lcols = folly::join(",", lds.colNames.begin(), lds.colNames.end());
        auto rcols = folly::join(",", rds.colNames.begin(), rds.colNames.end());
        return Status::Error("The data sets to union have different columns: <%s> vs. <%s>",
                             lcols.c_str(),
                             rcols.c_str());
    }
    return Status::OK();
}

std::unique_ptr<Iterator> SetExecutor::getLeftInputDataIter() const {
    auto left = asNode<SetOp>(node())->leftInputVar();
    auto& result = ectx_->getResult(left);
    return result.iter();
}

std::unique_ptr<Iterator> SetExecutor::getRightInputDataIter() const {
    auto right = asNode<SetOp>(node())->rightInputVar();
    auto& result = ectx_->getResult(right);
    return result.iter();
}

}   // namespace graph
}   // namespace nebula
