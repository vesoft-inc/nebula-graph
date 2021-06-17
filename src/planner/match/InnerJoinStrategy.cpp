/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/InnerJoinStrategy.h"
#include <vector>

#include "common/expression/AttributeExpression.h"
#include "planner/plan/Query.h"
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
        auto& leftKey = left->colNames().front();
        buildExpr = MatchSolver::getStartVidInPath(leftKey);
    } else {
        auto& leftKey = left->colNames().back();
        buildExpr = MatchSolver::getEndVidInPath(leftKey);
    }
    std::vector<Expression*> buildExprs{buildExpr};
    if (dstNodeId_ != nullptr) {
        buildExprs.emplace_back(dstNodeId_);
    }

    std::vector<Expression*> probeExprs;
    Expression* probeExpr = nullptr;
    if (rightPos_ == JoinPos::kStart) {
        auto& rightKey = right->colNames().front();
        probeExpr = MatchSolver::getStartVidInPath(rightKey);
        probeExprs.emplace_back(probeExpr);
        if (dstNodeId_ != nullptr) {
            auto& dstKey = right->colNames().back();
            auto dstIdExpr = MatchSolver::getEndVidInPath(dstKey);
            probeExprs.emplace_back(dstIdExpr);
        }
    } else {
        auto& rightKey = right->colNames().back();
        probeExpr = MatchSolver::getEndVidInPath(rightKey);
        probeExprs.emplace_back(probeExpr);
        if (dstNodeId_ != nullptr) {
            auto& dstKey = right->colNames().front();
            auto dstIdExpr = MatchSolver::getStartVidInPath(dstKey);
            probeExprs.emplace_back(dstIdExpr);
        }
    }
    for (const auto &eje : extraJoinExprs_) {
        buildExprs.emplace_back(eje.first);
        probeExprs.emplace_back(eje.second);
    }

    for (const auto &expr : buildExprs) {
        qctx_->objPool()->add(expr);
    }
    for (const auto &expr : probeExprs) {
        qctx_->objPool()->add(expr);
    }
    auto join = InnerJoin::make(qctx_,
                               const_cast<PlanNode*>(right),
                               {left->outputVar(), 0},
                               {right->outputVar(), 0},
                               std::move(buildExprs),
                               std::move(probeExprs));
    std::vector<std::string> colNames = left->colNames();
    const auto& rightColNames = right->colNames();
    colNames.insert(colNames.end(), rightColNames.begin(), rightColNames.end());
    join->setColNames(std::move(colNames));
    return join;
}
}  // namespace graph
}  // namespace nebula
