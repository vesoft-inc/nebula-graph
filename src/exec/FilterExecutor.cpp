/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/FilterExecutor.h"

#include "planner/Query.h"

namespace nebula {
namespace graph {

folly::Future<Status> FilterExecutor::exec() {
    auto* filter = static_cast<const Filter*>(node());
    auto* expr = filter->condition();
    Getters getters;
    auto res = expr->eval(getters);
    return res.status();
}

}   // namespace graph
}   // namespace nebula
