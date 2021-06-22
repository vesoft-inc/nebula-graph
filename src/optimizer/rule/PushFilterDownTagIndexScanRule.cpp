/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/PushFilterDownTagIndexScanRule.h"

#include "common/expression/Expression.h"
#include "optimizer/OptGroup.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Query.h"

using nebula::graph::Filter;

using Kind = nebula::graph::PlanNode::Kind;
using TransformResult = nebula::opt::OptRule::TransformResult;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushFilterDownTagIndexScanRule::kInstance =
    std::unique_ptr<PushFilterDownTagIndexScanRule>(new PushFilterDownTagIndexScanRule());

PushFilterDownTagIndexScanRule::PushFilterDownTagIndexScanRule() {
    RuleSet::QueryRules().addRule(this);
}

const Pattern& PushFilterDownTagIndexScanRule::pattern() const {
    static Pattern pattern =
        Pattern::create(Kind::kFilter, {Pattern::create(Kind::kTagIndexFullScan)});
    return pattern;
}

bool PushFilterDownTagIndexScanRule::match(OptContext* ctx, const MatchedResult& matched) const {
    UNUSED(ctx);
    auto filter = static_cast<const Filter*>(matched.node->node());
    auto condition = filter->condition();
    if (condition->isRelExpr()) {
        // TODO(yee)
    }
    if (condition->isLogicalExpr()) {
        // TODO(yee):
    }
    return false;
}

StatusOr<TransformResult> PushFilterDownTagIndexScanRule::transform(
    OptContext* ctx,
    const MatchedResult& matched) const {
    UNUSED(ctx);
    UNUSED(matched);
    return Status::OK();
}

std::string PushFilterDownTagIndexScanRule::toString() const {
    return "PushFilterDownTagIndexScanRule";
}

}   // namespace opt
}   // namespace nebula
