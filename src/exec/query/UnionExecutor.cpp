/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/UnionExecutor.h"

#include "context/ExecutionContext.h"
#include "planner/PlanNode.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

folly::Future<Status> UnionExecutor::execute() {
    dumpLog();
    auto unionNode = asNode<Union>(node());
    auto left = unionNode->left();
    auto right = unionNode->right();

    auto leftData = ectx_->getValue(left->varName());
    auto rightData = ectx_->getValue(right->varName());

    DCHECK_EQ(leftData.type(), Value::Type::DATASET);
    DCHECK_EQ(rightData.type(), Value::Type::DATASET);

    auto lds = leftData.getDataSet();
    auto rds = rightData.getDataSet();

    DCHECK(lds.colNames == rds.colNames);

    DataSet ds(lds);
    ds.rows.reserve(lds.rowSize() + rds.rowSize());

    for (auto &row : rds.rows) {
        ds.rows.emplace_back(row);
    }

    return finish(std::move(ds));
}

}   // namespace graph
}   // namespace nebula
