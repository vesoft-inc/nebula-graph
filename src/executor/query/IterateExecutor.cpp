/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/IterateExecutor.h"
#include "context/QueryExpressionContext.h"
#include "parser/Clauses.h"
#include "planner/Query.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> IterateExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *iterate = asNode<Iterate>(node());
    auto &input = ectx_->getValue(iterate->inputVar());
    DCHECK(input.isDataSet());
    auto &inputDS = input.getDataSet();

    DataSet ds;
    ds.colNames = iterate->colNames();
    DCHECK_LT(idx_, inputDS.size());
    ds.rows.emplace_back(inputDS.rows[idx_++]);

    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}

}   // namespace graph
}   // namespace nebula
