/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/PushFilterDownProjectRule.h"

#include "optimizer/OptContext.h"
#include "optimizer/OptGroup.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Query.h"
#include "util/ExpressionUtils.h"

using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushFilterDownProjectRule::kInstance =
    std::unique_ptr<PushFilterDownProjectRule>(new PushFilterDownProjectRule());

PushFilterDownProjectRule::PushFilterDownProjectRule() {
    RuleSet::QueryRules().addRule(this);
}

const Pattern& PushFilterDownProjectRule::pattern() const {
    static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kFilter,
                                             {Pattern::create(graph::PlanNode::Kind::kProject)});
    return pattern;
}

StatusOr<OptRule::TransformResult> PushFilterDownProjectRule::transform(
    OptContext* octx,
    const MatchedResult& matched) const {
    const auto* filterGroupNode = matched.node;
    const auto* oldFilterNode = filterGroupNode->node();
    const auto* projGroupNode = matched.dependencies.front().node;
    const auto* oldProjNode = projGroupNode->node();

    auto* newFilterNode = static_cast<graph::Filter*>(oldFilterNode->clone());
    auto* newProjNode = static_cast<graph::Project*>(oldProjNode->clone());
    const auto* condition = newFilterNode->condition();

    auto varProps = graph::ExpressionUtils::collectAll(condition, {Expression::Kind::kVarProperty});
    if (varProps.empty()) {
        return TransformResult::noTransform();
    }

    std::vector<std::string> propNames;
    for (auto expr : varProps) {
        DCHECK_EQ(expr->kind(), Expression::Kind::kVarProperty);
        propNames.emplace_back(*static_cast<const VariablePropertyExpression*>(expr)->prop());
    }

    auto projColNames = newProjNode->colNames();
    auto projColumns = newProjNode->columns()->columns();
    std::unordered_map<std::string, Expression*> rewriteMap;
    for (size_t i = 0; i < projColNames.size(); ++i) {
        auto colName = projColNames[i];
        auto iter = std::find_if(propNames.begin(), propNames.end(), [&colName](const auto& name) {
            return !colName.compare(name);
        });
        if (iter == propNames.end()) continue;
        rewriteMap[colName] = projColumns[i]->expr();
    }

    // Rewrite VariablePropertyExpr in filter's condition
    auto matcher = [&rewriteMap](const Expression* e) -> bool {
        if (e->kind() != Expression::Kind::kVarProperty) {
            return false;
        }
        auto* propName = static_cast<const VariablePropertyExpression*>(e)->prop();
        return rewriteMap[*propName];
    };
    auto rewriter = [&rewriteMap](const Expression* e) -> Expression* {
        DCHECK_EQ(e->kind(), Expression::Kind::kVarProperty);
        auto* propName = static_cast<const VariablePropertyExpression*>(e)->prop();
        return rewriteMap[*propName]->clone().release();
    };
    auto* newCondition =
        graph::RewriteVisitor::transform(condition, std::move(matcher), std::move(rewriter));
    octx->qctx()->objPool()->add(newCondition);
    newFilterNode->setCondition(newCondition);

    // Exchange planNode
    newProjNode->setOutputVar(oldFilterNode->outputVar());
    newFilterNode->setInputVar(oldProjNode->inputVar());
    newProjNode->setInputVar(oldProjNode->outputVar());
    newFilterNode->setOutputVar(oldProjNode->outputVar());

    // Push down filter's optGroup and embed newProjGroupNode into old filter's Group
    auto newProjGroupNode = OptGroupNode::create(octx, newProjNode, filterGroupNode->group());
    auto newFilterGroup = OptGroup::create(octx);
    auto newFilterGroupNode = newFilterGroup->makeGroupNode(newFilterNode);
    newProjGroupNode->dependsOn(newFilterGroup);
    for (auto dep : projGroupNode->dependencies()) {
        newFilterGroupNode->dependsOn(dep);
    }

    TransformResult result;
    result.eraseAll = true;
    result.newGroupNodes.emplace_back(newProjGroupNode);
    return result;
}

std::string PushFilterDownProjectRule::toString() const {
    return "PushFilterDownProjectRule";
}

}   // namespace opt
}   // namespace nebula

