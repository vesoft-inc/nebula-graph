/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/logic/PassThroughExecutor.h"

#include "planner/Query.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> PassThroughExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto &result = const_cast<Result&>(ectx_->getResult(node()->inputVar()));
    auto iter = result.iter();
    if (!iter->isDefaultIter() && !iter->empty()) {
        return finish(std::move(result));
    }

    DataSet ds;
    ds.colNames = node()->colNames();
    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}

}   // namespace graph
}   // namespace nebula
