/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/PushFilterDownInnerJoinRule.h"

#include "optimizer/OptContext.h"
#include "optimizer/OptGroup.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Query.h"
#include "util/ExpressionUtils.h"

using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushFilterDownInnerJoinRule::kInstance =
    std::unique_ptr<PushFilterDownInnerJoinRule>(new PushFilterDownInnerJoinRule());

PushFilterDownInnerJoinRule::PushFilterDownInnerJoinRule() {
    RuleSet::QueryRules().addRule(this);
}

const Pattern& PushFilterDownInnerJoinRule::pattern() const {
    static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kFilter,
                                             {Pattern::create(graph::PlanNode::Kind::kInnerJoin)});
    return pattern;
}

StatusOr<OptRule::TransformResult> PushFilterDownInnerJoinRule::transform(
    OptContext* octx,
    const MatchedResult& matched) const {
    auto* filterGroupNode = matched.node;
    auto* oldFilterNode = filterGroupNode->node();
    auto deps = matched.dependencies;
    DCHECK_EQ(deps.size(), 1);
    auto innerJoinGroupNode = deps.front().node;
    auto* innerJoinNode = innerJoinGroupNode->node();
    DCHECK_EQ(oldFilterNode->kind(), PlanNode::Kind::kFilter);
    DCHECK_EQ(innerJoinNode->kind(), PlanNode::Kind::kInnerJoin);
    auto* oldInnerJoinNode = static_cast<graph::InnerJoin*>(innerJoinNode);
    const auto* condition = static_cast<graph::Filter*>(oldFilterNode)->condition();
    DCHECK(condition);
    auto symTable = octx->qctx()->symTable();
    const std::pair<std::string, int64_t>& leftVar = oldInnerJoinNode->leftVar();
    std::vector<std::string> leftVarColNames = symTable->getVar(leftVar.first)->colNames;
    const std::pair<std::string, int64_t>& rightVar = oldInnerJoinNode->rightVar();
    std::vector<std::string> rightVarColNames = symTable->getVar(rightVar.first)->colNames;

    auto pickerHelper = [](std::vector<std::string> cols, const Expression* e) -> bool {
        auto varProps = graph::ExpressionUtils::collectAll(e, {Expression::Kind::kVarProperty});
        if (varProps.empty()) {
            return false;
        }
        std::vector<std::string> propNames;
        for (auto* expr : varProps) {
            DCHECK(expr->kind() == Expression::Kind::kVarProperty);
            propNames.emplace_back(static_cast<const VariablePropertyExpression*>(expr)->prop());
        }
        for (auto prop : propNames) {
            auto iter = std::find_if(cols.begin(), cols.end(), [&prop](std::string item) {
                return !item.compare(prop);
            });
            if (iter == cols.end()) {
                return false;
            }
        }
        return true;
    };
    auto leftPicker = [&leftVarColNames, pickerHelper](const Expression* e) -> bool {
        return pickerHelper(leftVarColNames, e);
    };
    auto rightPicker = [&rightVarColNames, pickerHelper](const Expression* e) -> bool {
        return pickerHelper(rightVarColNames, e);
    };
    Expression* leftFilterPicked = nullptr;
    Expression* rightFilterPicked = nullptr;
    Expression* tmp = nullptr;
    Expression* filterUnpicked = nullptr;
    graph::ExpressionUtils::splitFilter(condition, leftPicker, &leftFilterPicked, &tmp);
    if (tmp) {
        graph::ExpressionUtils::splitFilter(tmp, rightPicker, &rightFilterPicked, &filterUnpicked);
    }
    if (!leftFilterPicked && !rightFilterPicked) {
        return TransformResult::noTransform();
    }

    graph::Filter* newLeftFilterNode = nullptr;
    graph::Filter* newRightFilterNode = nullptr;
    std::string newLeftFilterOutputVar;
    std::string newRightFilterOutputVar;
    auto joinDeps = innerJoinGroupNode->dependencies();
    DCHECK_EQ(joinDeps.size(), 2);
    OptGroup* newLeftFilterGroup = nullptr;
    OptGroup* newRightFilterGroup = nullptr;
    OptGroupNode* newLeftFilterGroupNode = nullptr;
    OptGroupNode* newRightFilterGroupNode = nullptr;
    if (!leftFilterPicked) {
        // produce new left Filter node
        newLeftFilterNode = graph::Filter::make(
            octx->qctx(),
            const_cast<graph::PlanNode*>(oldInnerJoinNode->dep(0)),
            graph::ExpressionUtils::rewriteInnerVar(leftFilterPicked, leftVar.first));
        newLeftFilterNode->setInputVar(leftVar.first);
        newLeftFilterNode->setColNames(leftVarColNames);
        newLeftFilterGroup = OptGroup::create(octx);
        newLeftFilterGroupNode = newLeftFilterGroup->makeGroupNode(newLeftFilterNode);
        newLeftFilterGroupNode->dependsOn(joinDeps[0]);
        newLeftFilterOutputVar = newLeftFilterNode->outputVar();
    }
    if (!rightFilterPicked) {
        // produce new right Filter node
        newRightFilterNode = graph::Filter::make(
            octx->qctx(),
            const_cast<graph::PlanNode*>(oldInnerJoinNode->dep(1)),
            graph::ExpressionUtils::rewriteInnerVar(rightFilterPicked, rightVar.first));
        newRightFilterNode->setInputVar(rightVar.first);
        newRightFilterNode->setColNames(rightVarColNames);
        newRightFilterGroup = OptGroup::create(octx);
        newRightFilterGroupNode = newRightFilterGroup->makeGroupNode(newRightFilterNode);
        newRightFilterGroupNode->dependsOn(joinDeps[1]);
        newRightFilterOutputVar = newRightFilterNode->outputVar();
    }

    // produce new InnerJoin node
    auto* newInnerJoinNode = static_cast<graph::InnerJoin*>(oldInnerJoinNode->clone());
    if (!newLeftFilterNode) {
        newInnerJoinNode->setLeftVar({newLeftFilterOutputVar, 0});
    }
    if (!newRightFilterNode) {
        newInnerJoinNode->setRightVar({newRightFilterOutputVar, 0});
    }
    // TODO: update hash keys
    // const std::vector<Expression*>& hashKeys = oldInnerJoinNode->hashKeys();
    // std::vector<Expression*> newHashKeys;
    // for (auto* k : hashKeys) {
    //     newHashKeys.emplace_back(
    //         graph::ExpressionUtils::rewriteInnerVar(k, newLeftFilterOutputVar));
    // }
    // newInnerJoinNode->setHashKeys(newHashKeys);

    TransformResult result;
    result.eraseAll = true;
    if (filterUnpicked) {
        auto* newAboveFilterNode = graph::Filter::make(octx->qctx(), newInnerJoinNode);
        newAboveFilterNode->setOutputVar(oldFilterNode->outputVar());
        newAboveFilterNode->setCondition(filterUnpicked);
        auto newAboveFilterGroupNode =
            OptGroupNode::create(octx, newAboveFilterNode, filterGroupNode->group());

        auto newInnerJoinGroup = OptGroup::create(octx);
        auto newInnerJoinGroupNode = newInnerJoinGroup->makeGroupNode(newInnerJoinNode);
        newAboveFilterGroupNode->setDeps({newInnerJoinGroup});
        newInnerJoinGroupNode->setDeps({newLeftFilterGroup, newRightFilterGroup});
        result.newGroupNodes.emplace_back(newAboveFilterGroupNode);
    } else {
        newInnerJoinNode->setOutputVar(oldFilterNode->outputVar());
        newInnerJoinNode->setColNames(oldInnerJoinNode->colNames());
        auto newInnerJoinGroupNode =
            OptGroupNode::create(octx, newInnerJoinNode, filterGroupNode->group());
        newInnerJoinGroupNode->setDeps({newLeftFilterGroup, newRightFilterGroup});
        result.newGroupNodes.emplace_back(newInnerJoinGroupNode);
    }

    return result;
}

std::string PushFilterDownInnerJoinRule::toString() const {
    return "PushFilterDownInnerJoinRule";
}

}   // namespace opt
}   // namespace nebula
