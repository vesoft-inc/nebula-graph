/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/InnerJoinStrategy.h"

#include "common/expression/AttributeExpression.h"
#include "planner/Query.h"
#include "util/ExpressionUtils.h"
#include "planner/match/MatchSolver.h"

namespace nebula {
namespace graph {

PlanNode* InnerJoinStrategy::connect(const PlanNode::Dependency& left,
                                     const PlanNode::Dependency& right) {
    return joinDataSet(left, right);
}

PlanNode* InnerJoinStrategy::joinDataSet(const PlanNode::Dependency& left,
                                         const PlanNode::Dependency& right) {
    Expression* buildExpr = nullptr;
    if (leftPos_ == JoinPos::kStart) {
        auto& leftKey = left.node->colNamesRef().front();
        buildExpr = MatchSolver::getStartVidInPath(leftKey);
    } else {
        auto& leftKey = left.node->colNamesRef().back();
        buildExpr = MatchSolver::getEndVidInPath(leftKey);
    }

    Expression* probeExpr = nullptr;
    if (rightPos_ == JoinPos::kStart) {
        auto& rightKey = right.node->colNamesRef().front();
        probeExpr = MatchSolver::getStartVidInPath(rightKey);
    } else {
        auto& rightKey = right.node->colNamesRef().back();
        probeExpr = MatchSolver::getEndVidInPath(rightKey);
    }

    qctx_->objPool()->add(buildExpr);
    qctx_->objPool()->add(probeExpr);
    // TODO(yee): dynamic cast
    Join::DepParam leftParam = static_cast<const Join::DepParam&>(left);
    Join::DepParam rightParam = static_cast<const Join::DepParam&>(right);
    auto join = InnerJoin::make(qctx_, leftParam, rightParam, {buildExpr}, {probeExpr});
    std::vector<std::string> colNames = left.node->colNames();
    const auto& rightColNames = right.node->colNamesRef();
    colNames.insert(colNames.end(), rightColNames.begin(), rightColNames.end());
    join->setColNames(std::move(colNames));
    return join;
}

}  // namespace graph
}  // namespace nebula
