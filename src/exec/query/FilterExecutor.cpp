/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/FilterExecutor.h"

#include "planner/Query.h"

#include "context/ExpressionContextImpl.h"

namespace nebula {
namespace graph {

folly::Future<Status> FilterExecutor::execute() {
    dumpLog();
    auto* filter = asNode<Filter>(node());
    auto iter = ectx_->getResult(filter->inputVar()).iter();

    auto result = ExecResult::buildDefault(iter->valuePtr());
    ExpressionContextImpl ctx(ectx_, iter.get());
    auto condition = filter->condition();
    while (iter->valid()) {
        auto val = condition->eval(ctx);
        if (val.isBool() && !val.getBool()) {
            iter->erase();
        } else {
            iter->next();
        }
    }

    iter->reset();
    result.setIter(std::move(iter));
    return finish(std::move(result));
}

}   // namespace graph
}   // namespace nebula
