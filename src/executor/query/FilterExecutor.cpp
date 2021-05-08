/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/FilterExecutor.h"

#include "planner/plan/Query.h"

#include "context/QueryExpressionContext.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> FilterExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto* filter = asNode<Filter>(node());
    Result result = ectx_->getResult(filter->inputVar());
    if (result.iterRef() == nullptr || result.iterRef()->isDefaultIter()) {
        LOG(ERROR) << "Internal Error: iterator is nullptr or DefaultIter";
        return Status::Error("Internal Error: iterator is nullptr or DefaultIter");
    }

    VLOG(2) << "Get input var: " << filter->inputVar()
            << ", iterator type: " << static_cast<int16_t>(result.iterRef()->kind())
            << ", input data size: " << result.iterRef()->size();

    ResultBuilder builder;
    builder.values(result.values());
    QueryExpressionContext ctx(ectx_);
    auto condition = filter->condition();
    while (result.iterRef()->valid()) {
        auto val = condition->eval(ctx(result.iterRef()));
        if (val.isBadNull() || (!val.empty() && !val.isBool() && !val.isNull())) {
            return Status::Error("Internal Error: Wrong type result, "
                                 "the type should be NULL,EMPTY or BOOL");
        }
        if (val.empty() || val.isNull() || !val.getBool()) {
            if (UNLIKELY(filter->needStableFilter())) {
                result.iterRef()->erase();
            } else {
                result.iterRef()->unstableErase();
            }
        } else {
            result.iterRef()->next();
        }
    }

    result.iterRef()->reset();
    builder.iter(std::move(result).iter());
    return finish(builder.finish());
}

}   // namespace graph
}   // namespace nebula
