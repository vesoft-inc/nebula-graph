/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/PushFilterDownEdgeIndexScanRule.h"

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "common/expression/LogicalExpression.h"
#include "optimizer/OptGroup.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Query.h"

using nebula::Expression;
using nebula::graph::EdgeIndexFullScan;
using nebula::graph::EdgeIndexPrefixScan;
using nebula::graph::EdgeIndexRangeScan;
using nebula::graph::Filter;

using Kind = nebula::graph::PlanNode::Kind;
using TransformResult = nebula::opt::OptRule::TransformResult;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushFilterDownEdgeIndexScanRule::kInstance =
    std::unique_ptr<PushFilterDownEdgeIndexScanRule>(new PushFilterDownEdgeIndexScanRule());

PushFilterDownEdgeIndexScanRule::PushFilterDownEdgeIndexScanRule() {
    RuleSet::QueryRules().addRule(this);
}

const Pattern& PushFilterDownEdgeIndexScanRule::pattern() const {
    static Pattern pattern =
        Pattern::create(Kind::kFilter, {Pattern::create(Kind::kEdgeIndexFullScan)});
    return pattern;
}

bool PushFilterDownEdgeIndexScanRule::match(OptContext* ctx, const MatchedResult& matched) const {
    UNUSED(ctx);
    auto filter = static_cast<const Filter*>(matched.planNode());
    // auto edgeIndexScan = static_cast<const EdgeIndexFullScan*>(matched.planNode({0, 0}));

    auto condition = filter->condition();
    if (condition->isRelExpr()) {
        return checkRelExpr(condition);
    }

    if (condition->isLogicalExpr()) {
        return checkLogicalExpr(condition);
    }

    return true;
}

bool PushFilterDownEdgeIndexScanRule::checkRelExpr(const Expression* expr) const {
    UNUSED(expr);
    return false;
}

bool PushFilterDownEdgeIndexScanRule::checkLogicalExpr(const Expression* expr) const {
    if (expr->kind() != Expression::Kind::kLogicalAnd) {
        return false;
    }
    auto logicExpr = static_cast<const LogicalExpression*>(expr);
    for (auto& op : logicExpr->operands()) {
        UNUSED(op);
    }
    return true;
}

StatusOr<TransformResult> PushFilterDownEdgeIndexScanRule::transform(
    OptContext* ctx,
    const MatchedResult& matched) const {
    UNUSED(ctx);
    UNUSED(matched);
    return Status::OK();
}

std::string PushFilterDownEdgeIndexScanRule::toString() const {
    return "PushFilterDownEdgeIndexScanRule";
}

}   // namespace opt
}   // namespace nebula
