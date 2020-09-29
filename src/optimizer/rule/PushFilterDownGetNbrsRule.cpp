/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/PushFilterDownGetNbrsRule.h"

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

using nebula::graph::Filter;
using nebula::graph::GetNeighbors;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushFilterDownGetNbrsRule::kInstance =
    std::unique_ptr<PushFilterDownGetNbrsRule>(new PushFilterDownGetNbrsRule());

PushFilterDownGetNbrsRule::PushFilterDownGetNbrsRule() {
    RuleSet::QueryRules().addRule(this);
}

const Pattern &PushFilterDownGetNbrsRule::pattern() const {
    static Pattern pattern = Pattern::create(
        graph::PlanNode::Kind::kFilter, {Pattern::create(graph::PlanNode::Kind::kGetNeighbors)});
    return pattern;
}

bool PushFilterDownGetNbrsRule::match(const MatchedResult &matched) const {
    UNUSED(matched);
    return true;
}

StatusOr<OptRule::TransformResult> PushFilterDownGetNbrsRule::transform(
    QueryContext *qctx,
    const MatchedResult &matched) const {
    auto filterGroupExpr = matched.node;
    auto gnGroupExpr = matched.dependencies.front().node;
    auto filter = static_cast<const Filter *>(filterGroupExpr->node());
    auto gn = static_cast<const GetNeighbors *>(gnGroupExpr->node());

    auto condition = filter->condition()->clone();
    graph::ExtractFilterExprVisitor visitor;
    condition->accept(&visitor);
    if (!visitor.ok()) {
        return TransformResult{false, false, {}};
    }

    auto pool = qctx->objPool();
    auto remainedExpr = std::move(visitor).remainedExpr();
    OptGroupExpr *newFilterGroupExpr = nullptr;
    if (remainedExpr != nullptr) {
        auto newFilter = Filter::make(qctx, nullptr, pool->add(remainedExpr.release()));
        newFilter->setOutputVar(filter->outputVar());
        newFilter->setInputVar(filter->inputVar());
        newFilterGroupExpr = OptGroupExpr::create(qctx, newFilter, filterGroupExpr->group());
    }

    auto newGNFilter = condition->encode();
    if (!gn->filter().empty()) {
        auto filterExpr = Expression::decode(gn->filter());
        LogicalExpression logicExpr(
            Expression::Kind::kLogicalAnd, condition.release(), filterExpr.release());
        newGNFilter = logicExpr.encode();
    }

    auto newGN = gn->clone(qctx);
    newGN->setFilter(newGNFilter);

    OptGroupExpr *newGnGroupExpr = nullptr;
    if (newFilterGroupExpr != nullptr) {
        // Filter(A&&B)->GetNeighbors(C) => Filter(A)->GetNeighbors(B&&C)
        auto newGroup = OptGroup::create(qctx);
        newGnGroupExpr = OptGroupExpr::create(qctx, newGN, newGroup);
        newFilterGroupExpr->dependsOn(newGroup);
    } else {
        // Filter(A)->GetNeighbors(C) => GetNeighbors(A&&C)
        newGnGroupExpr = OptGroupExpr::create(qctx, newGN, filterGroupExpr->group());
        newGN->setOutputVar(filter->outputVar());
    }

    for (auto dep : gnGroupExpr->dependencies()) {
        newGnGroupExpr->dependsOn(dep);
    }

    TransformResult result;
    result.newGroupExprs.emplace_back(newFilterGroupExpr ? newFilterGroupExpr : newGnGroupExpr);
    result.eraseCurr = true;
    return result;
}

std::string PushFilterDownGetNbrsRule::toString() const {
    return "PushFilterDownGetNbrsRule";
}

}   // namespace opt
}   // namespace nebula
