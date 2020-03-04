/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/rule/FilterPushDownRule.h"

#include "planner/PlanNode.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

// static
const std::shared_ptr<PlanNode> FilterPushDownRule::FILTER_PUSH_DOWN_RULE(new FilterPushDownRule());

bool FilterPushDownRule::match(std::shared_ptr<PlanNode> node) const {
    if (node->kind() != PlanNode::Kind::kFilter) {
        return false;
    }

    auto& next = node->table();
    if (next.empty() || next.size() > 1) {
        // Don't handle multiple successors case now
        return false;
    }

    std::shared_ptr<PlanNode> successor = next[0];
    if (successor->kind() == PlanNode::Kind::kGetNeighbors) {
        // Always push filter operand to GetNeighbors
        return true;
    }

    // TODO: Check the expression of successor and filter
    return false;
}

std::shared_ptr<PlanNode> FilterPushDownRule::transform(std::shared_ptr<PlanNode> node) const {
    Filter* filter = static_cast<Filter*>(node.get());
    std::shared_ptr<PlanNode> next = node->table().first();
    if (next->kind() == PlanNode::Kind::kGetNeighbors) {
        GetNeighbors* getNbrs = static_cast<GetNeighbors*>(next.get());
        auto* exprs = filter->condition();
        // TODO: Merge expressions of Filter and GetNeighbors operands
        UNUSED(expres);
        UNUSED(getNbrs);
    } else {
        // TODO: Process other kind operands, for example: agg
    }

    // Reset relationship of previous plan nodes
    StateTransition trans;
    trans.setTable(node->prevTable());
    next->setPreTrans(std::move(trans));
    for (auto& prev : node->prevTable()) {
        prev->replace(node, next);
    }

    return next;
}

}   // namespace graph
}   // namespace nebula
