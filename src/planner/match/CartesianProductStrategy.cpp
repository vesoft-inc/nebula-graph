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

PlanNode* CartesianProductStrategy::connect(const PlanNode::Dependency& left,
                                            const PlanNode::Dependency& right) {
    return joinDataSet(left, right);
}

PlanNode* CartesianProductStrategy::joinDataSet(const PlanNode::Dependency& left,
                                                const PlanNode::Dependency& right) {
    DCHECK_NE(left.node->outputVar(), right.node->outputVar());

    auto* cartesianProduct = CartesianProduct::make(qctx_, nullptr);
    cartesianProduct->addVar(left.node->outputVar());
    cartesianProduct->addVar(right.node->outputVar());

    return cartesianProduct;
}

}   // namespace graph
}   // namespace nebula
