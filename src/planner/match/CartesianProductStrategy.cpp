/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/CartesianProductStrategy.h"

#include "planner/Algo.h"
#include "planner/Query.h"
#include "planner/match/MatchSolver.h"
#include "util/ExpressionUtils.h"

namespace nebula {
namespace graph {
PlanNode* CartesianProductStrategy::connect(const PlanNode* left, const PlanNode* right) {
    return joinDataSet(left, right);
}

PlanNode* CartesianProductStrategy::joinDataSet(const PlanNode* left, const PlanNode* right) {
    DCHECK(left->outputVar() != right->outputVar());

    auto* cartesianProduct = CartesianProduct::make(qctx_, const_cast<PlanNode*>(left));
    cartesianProduct->addVar(left->outputVar());
    cartesianProduct->addVar(right->outputVar());
    auto colNames = std::move(combineColNames(cartesianProduct->allColNames()));
    cartesianProduct->setColNames(colNames);

    return cartesianProduct;
}

std::vector<std::string> CartesianProductStrategy::combineColNames(
    const std::vector<std::vector<std::string>>& allColNames) {
    std::vector<std::string> ret;
    for (auto& colNames : allColNames) {
        ret.reserve(ret.size() + colNames.size());
        ret.insert(ret.end(), colNames.begin(), colNames.end());
    }

    return ret;
}

}   // namespace graph
}   // namespace nebula
