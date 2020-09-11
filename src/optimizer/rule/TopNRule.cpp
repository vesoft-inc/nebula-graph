/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/TopNRule.h"

#include "common/expression/BinaryExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/Expression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/UnaryExpression.h"
#include "optimizer/OptGroup.h"
#include "planner/PlanNode.h"
#include "planner/Query.h"
#include "visitor/ExtractFilterExprVisitor.h"

using nebula::graph::Limit;
using nebula::graph::Sort;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> TopNRule::kInstance =
    std::unique_ptr<TopNRule>(new TopNRule());

TopNRule::TopNRule() {
    RuleSet::queryRules().addRule(this);
}

bool TopNRule::match(const OptGroupExpr *groupExpr) const {
    auto pair = findMatchedGroupExpr(groupExpr);
    if (!pair.first) {
        return false;
    }

    return true;
}

Status TopNRule::transform(QueryContext *qctx,
                                            const OptGroupExpr *groupExpr,
                                            TransformResult *result) const {
    auto pair = findMatchedGroupExpr(groupExpr);
    auto limit = static_cast<const Limit *>(groupExpr->node());
    auto sort = static_cast<const Sort *>(pair.second->node());

    auto topn = TopN::make(qctx, nullptr, sort->factors(), limit->offset(), limit->count());
    topn->setOutputVar(limit->varName());
    topn->setInputVar(sort->inputVar());
    topnExpr = OptGroupExpr::create(qctx, topn, groupExpr->group());
    for (auto dep : pair.second->dependencies()) {
        topnExpr->dependsOn(dep);
    }

    result->newGroupExprs.emplace_back(topnExpr);
    result->eraseAll = true;
    result->eraseCurr = true;

    return Status::OK();
}

std::string TopNRule::toString() const {
    return "TopNRule";
}

std::pair<bool, const OptGroupExpr *> TopNRule::findMatchedGroupExpr(
    const OptGroupExpr *groupExpr) const {
    auto node = groupExpr->node();
    if (node->kind() != PlanNode::Kind::kLimit) {
        return std::make_pair(false, nullptr);
    }

    auto limit = static_cast<const Limit *>(groupExpr->node());
    // Currently, we cannot know the total amount of input data,
    // so only apply topn rule when offset of limit is 0
    if (limit->offset() != 0) {
        return std::make_pair(false, nullptr);
    }

    for (auto dep : groupExpr->dependencies()) {
        for (auto expr : dep->groupExprs()) {
            if (expr->node()->kind() == PlanNode::Kind::kSort) {
                return std::make_pair(true, expr);
            }
        }
    }
    return std::make_pair(false, nullptr);
}

}   // namespace opt
}   // namespace nebula
