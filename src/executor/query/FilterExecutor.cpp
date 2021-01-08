/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/FilterExecutor.h"

#include "planner/Query.h"

#include "context/QueryExpressionContext.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> FilterExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto* filter = asNode<Filter>(node());
    auto iter = ectx_->getResult(filter->inputVar()).iter();
    if (iter == nullptr || iter->isDefaultIter()) {
        LOG(ERROR) << "Internal Error: iterator is nullptr or DefaultIter";
        return Status::Error("Internal Error: iterator is nullptr or DefaultIter");
    }

    VLOG(2) << "Get input var: " << filter->inputVar()
            << ", iterator type: " << static_cast<int16_t>(iter->kind())
            << ", input data size: " << iter->size();

    ResultBuilder builder;
    builder.value(iter->valuePtr());
    QueryExpressionContext ctx(ectx_);
    auto condition = filter->condition();
    while (iter->valid()) {
        auto val = condition->eval(ctx(iter.get()));
        if (!val.empty() && !val.isBool() && !val.isNull()) {
            return Status::Error("Internal Error: Wrong type result, "
                                 "the type should be NULL,EMPTY or BOOL");
        }
        if (val.empty() || val.isNull() || !val.getBool()) {
            iter->unstableErase();
        } else {
            iter->next();
        }
    }

    iter->reset();
    builder.iter(std::move(iter));
    return finish(builder.finish());
}

}   // namespace graph
}   // namespace nebula
