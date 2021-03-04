/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/algo/CartesianProductExecutor.h"

#include "planner/Algo.h"

namespace nebula {
namespace graph {

folly::Future<Status> CartesianProductExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto* cartesianProduct = asNode<CartesianProduct>(node());
    colNames_ = cartesianProduct->allColNames();
    auto vars = cartesianProduct->inputVars();
    if (vars.size() < 2) {
        return Status::Error("vars's size : %zu, must be greater than 2", vars.size());
    }
    std::vector<std::string> emptyCol;
    return finish(ResultBuilder().value(DataSet()).finish());
}

void CartesianProductExecutor::initJoinIter(Iterator* rightIter) {
    UNUSED(rightIter);
}

void CartesianProductExecutor::doCartesianProduct(Iterator* leftIter,
                                                  Iterator* rightIter) {
    UNUSED(leftIter);
    UNUSED(rightIter);
}

}   // namespace graph
}   // namespace nebula
