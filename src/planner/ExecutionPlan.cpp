/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/ExecutionPlan.h"

#include <folly/String.h>

#include "common/interface/gen-cpp2/graph_types.h"
#include "exec/Executor.h"
#include "planner/PlanNode.h"
#include "planner/Query.h"
#include "scheduler/Scheduler.h"
#include "util/IdGenerator.h"
#include "util/ObjectPool.h"

using folly::stringPrintf;

namespace nebula {
namespace graph {

ExecutionPlan::ExecutionPlan(ObjectPool* objectPool)
    : id_(EPIdGenerator::instance().id()),
      objPool_(objectPool),
      nodeIdGen_(std::make_unique<IdGenerator>(0)) {}

ExecutionPlan::~ExecutionPlan() {}

PlanNode* ExecutionPlan::addPlanNode(PlanNode* node) {
    node->setId(nodeIdGen_->id());
    return objPool_->add(node);
}

static cpp2::PlanNodeDescription* makePlanNodeDesc(
    const PlanNode* node,
    cpp2::PlanDescription* planDesc,
    std::unordered_map<int64_t, size_t>* nodeIdxMap) {
    auto found = nodeIdxMap->find(node->id());
    if (found != nodeIdxMap->end()) {
        return &planDesc->plan_node_descs[found->second];
    }

    cpp2::PlanNodeDescription planNodeDesc;
    planNodeDesc.set_id(node->id());
    planNodeDesc.set_name(PlanNode::toString(node->kind()));
    planNodeDesc.set_output_var(node->varName());
    planNodeDesc.set_arguments({node->explain()});

    switch (node->kind()) {
        case PlanNode::Kind::kStart: {
            break;
        }
        case PlanNode::Kind::kUnion:
        case PlanNode::Kind::kIntersect:
        case PlanNode::Kind::kMinus: {
            auto bNode = static_cast<const BiInputNode*>(node);
            planNodeDesc.set_dependencies({bNode->left()->id(), bNode->right()->id()});
            makePlanNodeDesc(bNode->left(), planDesc, nodeIdxMap);
            makePlanNodeDesc(bNode->right(), planDesc, nodeIdxMap);
            break;
        }
        case PlanNode::Kind::kSelect: {
            auto select = static_cast<const Select*>(node);
            planNodeDesc.set_dependencies({select->dep()->id()});
            auto then = makePlanNodeDesc(select->then(), planDesc, nodeIdxMap);
            auto otherwise = makePlanNodeDesc(select->otherwise(), planDesc, nodeIdxMap);
            then->set_condition(true);
            then->get_arguments()->emplace_back(stringPrintf("Select Node Id: %ld", select->id()));
            otherwise->set_condition(false);
            then->get_arguments()->emplace_back(stringPrintf("Select Node Id: %ld", select->id()));
            makePlanNodeDesc(select->dep(), planDesc, nodeIdxMap);
            break;
        }
        case PlanNode::Kind::kLoop: {
            auto loop = static_cast<const Loop*>(node);
            planNodeDesc.set_dependencies({loop->dep()->id()});
            auto body = makePlanNodeDesc(loop->body(), planDesc, nodeIdxMap);
            body->set_condition(true);
            body->get_arguments()->emplace_back(stringPrintf("Loop Node Id: %ld", loop->id()));
            makePlanNodeDesc(loop->dep(), planDesc, nodeIdxMap);
            break;
        }
        default: {
            // Other plan nodes have single dependency
            auto singleDepNode = static_cast<const SingleDependencyNode*>(node);
            makePlanNodeDesc(singleDepNode->dep(), planDesc, nodeIdxMap);
            break;
        }
    }
    nodeIdxMap->emplace(node->id(), planDesc->plan_node_descs.size());
    planDesc->plan_node_descs.emplace_back(std::move(planNodeDesc));
    return &planDesc->plan_node_descs.back();
}

void ExecutionPlan::fillPlanDescription(cpp2::PlanDescription* planDesc,
                                        std::unordered_map<int64_t, size_t>* nodeIdxMap) const {
    makePlanNodeDesc(root_, DCHECK_NOTNULL(planDesc), DCHECK_NOTNULL(nodeIdxMap));
}

}   // namespace graph
}   // namespace nebula
