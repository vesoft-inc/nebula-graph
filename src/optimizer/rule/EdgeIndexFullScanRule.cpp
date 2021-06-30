/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/EdgeIndexFullScanRule.h"

using Kind = nebula::graph::PlanNode::Kind;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> EdgeIndexFullScanRule::kInstance =
    std::unique_ptr<EdgeIndexFullScanRule>(new EdgeIndexFullScanRule());

EdgeIndexFullScanRule::EdgeIndexFullScanRule() {
    RuleSet::QueryRules().addRule(this);
}

const Pattern& EdgeIndexFullScanRule::pattern() const {
    static Pattern pattern = Pattern::create(Kind::kEdgeIndexFullScan);
    return pattern;
}

std::string EdgeIndexFullScanRule::toString() const {
    return "EdgeIndexFullScanRule";
}

}   // namespace opt
}   // namespace nebula
