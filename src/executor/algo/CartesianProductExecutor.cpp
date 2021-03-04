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
    auto leftIter = std::make_unique<JoinIter>(emptyCol);
    return finish(ResultBuilder().value(DataSet()).iter(std::move(leftIter)).finish());
}

void CartesianProductExecutor::initJoinIter(JoinIter* joinIter, Iterator* rightIter) {
    UNUSED(joinIter);
    UNUSED(rightIter);
}

void CartesianProductExecutor::doCartesianProduct(Iterator* leftIter,
                                                  Iterator* rightIter,
                                                  JoinIter* joinIter) {
    UNUSED(joinIter);
    UNUSED(rightIter);
    UNUSED(leftIter);
}

}   // namespace graph
}   // namespace nebula
