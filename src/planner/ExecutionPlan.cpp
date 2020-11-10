/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/ExecutionPlan.h"

#include "common/graph/Response.h"
#include "planner/Logic.h"
#include "planner/PlanNode.h"
#include "planner/Query.h"
#include "util/IdGenerator.h"

namespace nebula {
namespace graph {

ExecutionPlan::ExecutionPlan(PlanNode* root) : id_(EPIdGenerator::instance().id()), root_(root) {}

ExecutionPlan::~ExecutionPlan() {}

static size_t makePlanNodeDesc(const PlanNode* node, PlanDescription* planDesc) {
    auto found = planDesc->node_index_map.find(node->id());
    if (found != planDesc->node_index_map.end()) {
        return found->second;
    }

    size_t planNodeDescPos = planDesc->plan_node_descs.size();
    planDesc->node_index_map.emplace(node->id(), planNodeDescPos);
    planDesc->plan_node_descs.emplace_back(std::move(*node->explain()));
    auto& planNodeDesc = planDesc->plan_node_descs.back();

    switch (node->kind()) {
        case PlanNode::Kind::kStart: {
            break;
        }
        case PlanNode::Kind::kUnion:
        case PlanNode::Kind::kIntersect:
        case PlanNode::Kind::kMinus:
        case PlanNode::Kind::kConjunctPath: {
            auto bNode = static_cast<const BiInputNode*>(node);
            makePlanNodeDesc(bNode->left(), planDesc);
            makePlanNodeDesc(bNode->right(), planDesc);
            break;
        }
        case PlanNode::Kind::kSelect: {
            auto select = static_cast<const Select*>(node);
            planNodeDesc.dependencies.reset(new std::vector<int64_t>{select->dep()->id()});
            auto thenPos = makePlanNodeDesc(select->then(), planDesc);
            PlanNodeBranchInfo thenInfo;
            thenInfo.is_do_branch = true;
            thenInfo.condition_node_id = select->id();
            planDesc->plan_node_descs[thenPos].branch_info =
                std::make_unique<PlanNodeBranchInfo>(std::move(thenInfo));
            auto otherwisePos = makePlanNodeDesc(select->otherwise(), planDesc);
            PlanNodeBranchInfo elseInfo;
            elseInfo.is_do_branch = false;
            elseInfo.condition_node_id = select->id();
            planDesc->plan_node_descs[otherwisePos].branch_info =
                std::make_unique<PlanNodeBranchInfo>(std::move(elseInfo));
            makePlanNodeDesc(select->dep(), planDesc);
            break;
        }
        case PlanNode::Kind::kLoop: {
            auto loop = static_cast<const Loop*>(node);
            planNodeDesc.dependencies.reset(new std::vector<int64_t>{loop->dep()->id()});
            auto bodyPos = makePlanNodeDesc(loop->body(), planDesc);
            PlanNodeBranchInfo info;
            info.is_do_branch = true;
            info.condition_node_id = loop->id();
            planDesc->plan_node_descs[bodyPos].branch_info =
                std::make_unique<PlanNodeBranchInfo>(std::move(info));
            makePlanNodeDesc(loop->dep(), planDesc);
            break;
        }
        default: {
            // Other plan nodes have single dependency
            DCHECK_EQ(node->dependencies().size(), 1U);
            auto singleDepNode = static_cast<const SingleDependencyNode*>(node);
            makePlanNodeDesc(singleDepNode->dep(), planDesc);
            break;
        }
    }
    return planNodeDescPos;
}

void ExecutionPlan::fillPlanDescription(PlanDescription* planDesc) const {
    DCHECK(planDesc != nullptr);
    makePlanNodeDesc(root_, planDesc);
}

}   // namespace graph
}   // namespace nebula
