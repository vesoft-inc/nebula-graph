/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/AssignExecutor.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

folly::Future<Status> AssignExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto* assign = asNode<Assign>(node());
    auto varName = assign->varName();
    auto* valueExpr = assign->valueExpr();

    QueryExpressionContext ctx(ectx_);
    auto value = valueExpr->eval(ctx);
    VLOG(1) << "VarName is: " << varName << " value is : " << value;
    ectx_->setValue(varName, std::move(value));
    DataSet ds;
    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}

}   // namespace graph
}   // namespace nebula
