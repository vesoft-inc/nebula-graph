/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "scheduler/PlanFragment.h"

#include "base/Logging.h"
#include "exec/ExecutionContext.h"
#include "exec/Executor.h"
#include "planner/PlanNode.h"

namespace nebula {
namespace graph {

// static
std::shared_ptr<PlanFragment> PlanFragment::createPlanFragmentFromPlanNode(
        const std::shared_ptr<PlanNode> &start) {
    auto last = getLastPlanNode(start);
    // Actually, the last exec node would be yield or project node
    if (!last || last->kind() != PlanNode::Kind::kProject) {
        LOG(FATAL) << "invalid plan node";
    }

    DCHECK_EQ(last->prevTable().size(), 1U);

    // Private constructor
    std::shared_ptr<PlanFragment> root(new PlanFragment);
    traversePlanNodeReversely(last, root.get());
    return root;
}

// static
std::shared_ptr<PlanNode> PlanFragment::getLastPlanNode(const std::shared_ptr<PlanNode> &start) {
    std::vector<PlanNode *> visited;
    return getLastPlanNodeInner(start, visited);
}

// static
std::shared_ptr<PlanNode> PlanFragment::getLastPlanNodeInner(const std::shared_ptr<PlanNode> &node,
                                                             std::vector<PlanNode *> &visited) {
    auto table = node->table();
    if (table.empty()) {
        return node;
    }

    // TODO(yee): There may be some loops in the plan
    for (auto it = table.begin(); it != table.end(); ++it) {
        auto found = std::find(visited.begin(), visited.end(), it->get());
        if (found == visited.end()) {
            visited.push_back(it->get());
            return getLastPlanNodeInner(*it, visited);
        }
    }

    LOG(FATAL) << "This method can not find a valid plan node";
    return nullptr;
}

// static
void PlanFragment::traversePlanNodeReversely(std::shared_ptr<PlanNode> node,
                                             PlanFragment *currPlan) {
    auto table = node->prevTable();
    if (table.empty()) {
        currPlan->setStartNode(node);
        return;
    }

    // TODO(yee): FindShortestPath don't match this pattern
    DCHECK_EQ(table.size(), 1U);
    for (auto &prev : table) {
        if (prev->kind() == PlanNode::Kind::kUnknown) {
            LOG(FATAL) << "Invalid plan prev kind";
        }

        if (prev->kind() == PlanNode::Kind::kUnion) {
            makeUnionPlanFragment(node, currPlan);
            continue;
        }

        if (prev->kind() == PlanNode::Kind::kIntersect) {
            // makeIntersectExecNode(node, currPlan);
            continue;
        }

        if (prev->kind() == PlanNode::Kind::kMinus) {
            // makeMinusExecNode(node, currPlan);
            continue;
        }

        traversePlanNodeReversely(prev, currPlan);
    }
}

// static
void PlanFragment::makeUnionPlanFragment(std::shared_ptr<PlanNode> node, PlanFragment *currPlan) {
    DCHECK(node->kind() == PlanNode::Kind::kUnion);

    // current plan start from union
    currPlan->setStartNode(node);

    auto prevTable = node->prevTable();
    DCHECK_EQ(prevTable.size(), 2U);

    // left side plan fragment
    std::shared_ptr<PlanFragment> leftPlan(new PlanFragment);
    currPlan->dependsOn(leftPlan.get());
    // break plan node dependent relationship
    auto leftNode = prevTable.front();
    leftNode->clearStatTransitionTable();
    traversePlanNodeReversely(leftNode, leftPlan.get());

    // right side plan fragment
    std::shared_ptr<PlanFragment> rightPlan(new PlanFragment);
    currPlan->dependsOn(rightPlan.get());
    // break plan node dependent relationship
    auto rightNode = prevTable.back();
    rightNode->clearStatTransitionTable();
    traversePlanNodeReversely(rightNode, rightPlan.get());
}

void PlanFragment::dependsOn(PlanFragment *dep) {
    depends_.push_back(dep);
}

std::shared_ptr<PlanFragment> PlanFragment::clone() const {
    // TODO
    return nullptr;
}

std::shared_ptr<Executor> PlanFragment::convertPlan(ExecutionContext *ectx) const {
    auto root = Executor::makeExecutor(*start_, ectx);
    for (auto &trans : start_->table()) {
        auto exec = Executor::makeExecutor(*trans, ectx);
        root->dependsOn(exec.get());
    }
    return root;
}

}   // namespace graph
}   // namespace nebula
