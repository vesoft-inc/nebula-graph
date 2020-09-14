/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/LimitPushDownRule.h"

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
using nebula::graph::Project;
using nebula::graph::GetNeighbors;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> LimitPushDownRule::kInstance =
    std::unique_ptr<LimitPushDownRule>(new LimitPushDownRule());

LimitPushDownRule::LimitPushDownRule() {
    RuleSet::queryRules().addRule(this);
}

bool LimitPushDownRule::match(const OptGroupExpr *groupExpr) const {
    auto pair = findMatchedGroupExpr(groupExpr);
    if (!pair.first) {
        return false;
    }

    return true;
}

Status LimitPushDownRule::transform(QueryContext *qctx,
                                    const OptGroupExpr *groupExpr,
                                    TransformResult *result) const {
    auto pair = findMatchedGroupExpr(groupExpr);
    auto projExpr = pair.second[0];
    auto gnExpr = pair.second[1];

    const auto limit = static_cast<const Limit *>(groupExpr->node());
    const auto proj = static_cast<const Project *>(projExpr->node());
    const auto gn = static_cast<const GetNeighbors *>(gnExpr->node());

    int64_t limitRows = limit->offset() + limit->count();

    auto newLimit = cloneLimit(qctx, limit);
    auto newLimitExpr = OptGroupExpr::create(qctx, newLimit, groupExpr->group());

    auto newProj = cloneProj(qctx, proj);
    auto newProjGroup = OptGroup::create(qctx);
    auto newProjExpr = newProjGroup->makeGroupExpr(qctx, newProj);

    auto newGn = cloneGetNbrs(qctx, gn);
    newGn->setLimit(limitRows);
    auto newGnGroup = OptGroup::create(qctx);
    auto newGnExpr = newGnGroup->makeGroupExpr(qctx, newGn);

    newLimitExpr->dependsOn(newProjGroup);
    newProjExpr->dependsOn(newGnGroup);
    for (auto dep : gnExpr->dependencies()) {
       newGnExpr->dependsOn(dep);
    }

    result->eraseAll = true;
    result->eraseCurr = true;
    result->newGroupExprs.emplace_back(newLimitExpr);
    return Status::OK();
}

std::string LimitPushDownRule::toString() const {
    return "LimitPushDownRule";
}

std::pair<bool, std::vector<const OptGroupExpr *>> LimitPushDownRule::findMatchedGroupExpr(
    const OptGroupExpr *groupExpr) const {
    std::vector<const OptGroupExpr *> matched;

    auto node = groupExpr->node();
    if (node->kind() != PlanNode::Kind::kLimit) {
        return std::make_pair(false, matched);
    }

    const auto limit = static_cast<const Limit *>(node);
    int64_t limitRows = limit->offset() + limit->count();

    bool projFound = false;
    const OptGroupExpr *projExpr = nullptr;
    for (auto dep : groupExpr->dependencies()) {
        for (auto expr : dep->groupExprs()) {
            if (expr->node()->kind() == PlanNode::Kind::kProject) {
                projFound = true;
                projExpr = expr;
                break;
            }
        }
    }

    if (!projFound) {
        return std::make_pair(false, matched);
    }

    bool gnFound = false;
    const OptGroupExpr *gnExpr = nullptr;
    for (auto dep : projExpr->dependencies()) {
        for (auto expr : dep->groupExprs()) {
            if (expr->node()->kind() == PlanNode::Kind::kGetNeighbors) {
                gnFound = true;
                gnExpr = expr;
                break;
            }
        }
    }

    if (!gnFound) {
        return std::make_pair(false, matched);
    }

    const auto gn = static_cast<const GetNeighbors *>(gnExpr->node());
    if (limitRows >= gn->limit()) {
        return std::make_pair(false, matched);
    }
    matched.push_back(projExpr);
    matched.push_back(gnExpr);
    return std::make_pair(true, matched);
}

Limit *LimitPushDownRule::cloneLimit(QueryContext *qctx,
                                     const Limit *limit) const {
    auto newLimit = Limit::make(qctx, nullptr, limit->offset(), limit->count());
    newLimit->setInputVar(limit->inputVar());
    newLimit->setOutputVar(limit->outputVar());
    return newLimit;
}

Project *LimitPushDownRule::cloneProj(QueryContext *qctx,
                                      const Project *proj) const {
    auto cols = qctx->objPool()->add(new YieldColumns());
    for (auto col : proj->columns()->columns()) {
        cols->addColumn((col->clone()).release());
    }

    auto newProj = Project::make(qctx, nullptr, cols);
    newProj->setInputVar(proj->inputVar());
    newProj->setOutputVar(proj->outputVar());
    return newProj;
}

GetNeighbors *LimitPushDownRule::cloneGetNbrs(QueryContext *qctx,
                                              const GetNeighbors *gn) const {
    auto newGn = GetNeighbors::make(qctx, nullptr, gn->space());
    newGn->setSrc(gn->src());
    newGn->setEdgeTypes(gn->edgeTypes());
    newGn->setEdgeDirection(gn->edgeDirection());
    newGn->setDedup(gn->dedup());
    newGn->setRandom(gn->random());
    newGn->setFilter(gn->filter());
    newGn->setLimit(gn->limit());
    newGn->setInputVar(gn->inputVar());
    newGn->setOutputVar(gn->outputVar());

    if (gn->vertexProps()) {
        auto vertexProps = *gn->vertexProps();
        auto vertexPropsPtr = std::make_unique<decltype(vertexProps)>(std::move(vertexProps));
        newGn->setVertexProps(std::move(vertexPropsPtr));
    }

    if (gn->edgeProps()) {
        auto edgeProps = *gn->edgeProps();
        auto edgePropsPtr = std::make_unique<decltype(edgeProps)>(std::move(edgeProps));
        newGn->setEdgeProps(std::move(edgePropsPtr));
    }

    if (gn->statProps()) {
        auto statProps = *gn->statProps();
        auto statPropsPtr = std::make_unique<decltype(statProps)>(std::move(statProps));
        newGn->setStatProps(std::move(statPropsPtr));
    }

    if (gn->exprs()) {
        auto exprs = *gn->exprs();
        auto exprsPtr = std::make_unique<decltype(exprs)>(std::move(exprs));
        newGn->setExprs(std::move(exprsPtr));
    }
    return newGn;
}

}   // namespace opt
}   // namespace nebula
