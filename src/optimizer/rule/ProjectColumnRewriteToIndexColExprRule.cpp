/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/ProjectColumnRewriteToIndexColExprRule.h"

#include "optimizer/OptGroup.h"

namespace nebula {
namespace opt {
std::unique_ptr<OptRule> ProjectColumnRewriteToIndexColExprRule::kInstance =
    std::unique_ptr<ProjectColumnRewriteToIndexColExprRule>(
        new ProjectColumnRewriteToIndexColExprRule());

ProjectColumnRewriteToIndexColExprRule::ProjectColumnRewriteToIndexColExprRule() {
    RuleSet::QueryRules().addRule(this);
}

const Pattern& ProjectColumnRewriteToIndexColExprRule::pattern() const {
    static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kProject);
    return pattern;
}

bool ProjectColumnRewriteToIndexColExprRule::match(const MatchedResult &matched) const {
    DCHECK(!matched.dependencies.empty());
    if (matched.dependencies.empty()) {
        return false;
    }
    auto depKind = matched.dependencies.front().node->node()->kind();
    if (depKind == graph::PlanNode::Kind::kGetNeighbors
            || depKind == graph::PlanNode::Kind::kGetVertices
            || depKind == graph::PlanNode::Kind::kGetEdges
            || depKind == graph::PlanNode::Kind::kIndexScan) {
        return false;
    }
    return true;
}

StatusOr<OptRule::TransformResult> ProjectColumnRewriteToIndexColExprRule::transform(
    graph::QueryContext *qctx,
    const MatchedResult &matched) const {
    UNUSED(qctx);
    UNUSED(matched);
    return Status::Error();
}

std::string ProjectColumnRewriteToIndexColExprRule::toString() const {
    return "ProjectColumnRewriteToIndexColExprRule";
}
}  // namespace opt
}  // namespace nebula
