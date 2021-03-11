/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/ColumnsMergeExecutor.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

folly::Future<Status> ColumnsMergeExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    const auto* cm = asNode<ColumnsMerge>(node());
    auto iter = ectx_->getResult(cm->inputVar()).iter();
    // check column type
    if (iter->valid()) {
        folly::Optional<Value> pre;
        for (const auto &column : cm->columns()) {
            Value current = iter->getColumn(column);
            if (pre.hasValue()) {
                if (pre.value().type() != current.type()) {
                    return Status::Error("Can't merge the columns with different type.");
                }
            }
            pre = std::move(current);
        }
    }

    DataSet ds({cm->columnName()});
    ds.rows.reserve(iter->size() * cm->columns().size());
    while (iter->valid()) {
        for (const auto &column : cm->columns()) {
            ds.emplace_back(Row({iter->getColumn(column)}));
        }
        iter->next();
    }
    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}

}   // namespace graph
}   // namespace nebula
