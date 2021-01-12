/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/ProjectColumnRewriteToIndexColExprRule.h"

#include "optimizer/OptGroup.h"
#include "visitor/RewritePropExprToIndexColExprVisitor.h"
#include "planner/Query.h"

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
    auto proGroupNode = matched.node;
    DCHECK(!proGroupNode->dependencies().empty());
    if (proGroupNode->dependencies().empty()) {
        return false;
    }
    for (auto depGroup : proGroupNode->dependencies()) {
        if (depGroup->groupNodes().empty()) {
            return false;
        }
        for (auto dep : depGroup->groupNodes()) {
            auto depKind = dep->node()->kind();
            if (depKind == graph::PlanNode::Kind::kGetNeighbors
                    || depKind == graph::PlanNode::Kind::kGetVertices
                    || depKind == graph::PlanNode::Kind::kGetEdges
                    || depKind == graph::PlanNode::Kind::kIndexScan) {
                return false;
            }
        }
    }
    return true;
}

StatusOr<OptRule::TransformResult> ProjectColumnRewriteToIndexColExprRule::transform(
    graph::QueryContext *qctx,
    const MatchedResult &matched) const {
    auto proGroupNode = matched.node;
    auto project = static_cast<const graph::Project*>(proGroupNode->node());
    // All the candidate dependencies of the project should have same output column names.
    auto depColNames =
        proGroupNode->dependencies().front()->groupNodes().front()->node()->colNames();

    auto cols = project->columns()->columns();
    auto newYields = qctx->objPool()->makeAndAdd<YieldColumns>();
    for (auto col : cols) {
        graph::RewritePropExprToIndexColExprVisitor visitor(depColNames);
        auto newCol = col->clone();
        newCol->expr()->accept(&visitor);
        if (!visitor.ok()) {
            return visitor.status();
        }
        auto rewritedExpr = std::move(visitor).rewritedExpr();
        if (rewritedExpr != nullptr) {
            newCol->setExpr(rewritedExpr.release());
        }
        newYields->addColumn(newCol.release());
    }

    TransformResult result;
    auto newProject = project->clone(qctx);
    newProject->setYieldColumns(newYields);
    auto newProGroupNode = OptGroupNode::create(qctx, newProject, proGroupNode->group());
    if (proGroupNode->dependencies().size() != 1) {
        return Status::Error("Project has only one dependency.");
    }
    newProGroupNode->dependsOn(proGroupNode->dependencies().front());
    result.newGroupNodes.emplace_back(newProGroupNode);
    result.eraseAll = true;
    return result;
}

std::string ProjectColumnRewriteToIndexColExprRule::toString() const {
    return "ProjectColumnRewriteToIndexColExprRule";
}
}  // namespace opt
}  // namespace nebula
