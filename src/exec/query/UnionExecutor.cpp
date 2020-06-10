/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/UnionExecutor.h"

#include <algorithm>

#include <folly/String.h>

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

    if (leftData.type() != Value::Type::DATASET || rightData.type() != Value::Type::DATASET) {
        return Status::Error("Invalid data types of dependencies: %d vs. %d.",
                             static_cast<uint8_t>(leftData.type()),
                             static_cast<uint8_t>(rightData.type()));
    }

    auto lds = leftData.getDataSet();
    auto rds = rightData.getDataSet();

    if (lds.colNames.size() != rds.colNames.size()) {
        auto lcols = folly::join(",", lds.colNames.begin(), lds.colNames.end());
        auto rcols = folly::join(",", rds.colNames.begin(), rds.colNames.end());
        return Status::Error("The data sets to union have different columns: <%s> vs. <%s>",
                             lcols.c_str(),
                             rcols.c_str());
    }

    DataSet ds(lds);
    ds.rows.reserve(lds.rowSize() + rds.rowSize());

    for (auto &row : rds.rows) {
        ds.rows.emplace_back(row);
    }

    if (unionNode->distinct()) {
        doDistinct(&ds);
    }

    return finish(std::move(ds));
}

// static
void UnionExecutor::doDistinct(DataSet *ds) {
    // TODO(yee): use hash table to speed up this step
    std::sort(ds->rows.begin(), ds->rows.end(), [](const Row &lhs, const Row &rhs) -> bool {
        for (size_t i = 0; i < lhs.columns.size(); ++i) {
            if (lhs.columns[i] >= rhs.columns[i]) return false;
        }
        return true;
    });
    auto last = std::unique(ds->rows.begin(), ds->rows.end());
    ds->rows.erase(last, ds->rows.end());
}

}   // namespace graph
}   // namespace nebula
