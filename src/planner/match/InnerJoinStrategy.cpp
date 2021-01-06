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
PlanNode* InnerJoinStrategy::connect(const PlanNode* left, const PlanNode* right) {
    return joinDataSet(left, right);
}

PlanNode* InnerJoinStrategy::joinDataSet(const PlanNode* left, const PlanNode* right) {
    Expression* buildExpr = nullptr;
    if (leftPos_ == JoinPos::kStart) {
        auto& leftKey = left->colNamesRef().front();
        buildExpr = MatchSolver::getStartVidInPath(leftKey);
    } else {
        auto& leftKey = left->colNamesRef().back();
        buildExpr = MatchSolver::getEndVidInPath(leftKey);
    }

    Expression* probeExpr = nullptr;
    if (rightPos_ == JoinPos::kStart) {
        auto& rightKey = right->colNamesRef().front();
        probeExpr = MatchSolver::getStartVidInPath(rightKey);
    } else {
        auto& rightKey = right->colNamesRef().back();
        probeExpr = MatchSolver::getEndVidInPath(rightKey);
    }

    qctx_->objPool()->add(buildExpr);
    qctx_->objPool()->add(probeExpr);
    auto join = DataJoin::make(qctx_,
                               const_cast<PlanNode*>(right),
                               {left->outputVar(), 0},
                               {right->outputVar(), 0},
                               {buildExpr},
                               {probeExpr});
    std::vector<std::string> colNames = left->colNames();
    const auto& rightColNames = right->colNamesRef();
    colNames.insert(colNames.end(), rightColNames.begin(), rightColNames.end());
    join->setColNames(std::move(colNames));
    return join;
}
}  // namespace graph
}  // namespace nebula
