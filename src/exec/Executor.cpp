/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/Executor.h"

#include "exec/ExecutionContext.h"
#include "planner/PlanNode.h"

namespace nebula {
namespace graph {

// static
std::shared_ptr<Executor> Executor::makeExecutor(const PlanNode &node, ExecutionContext *ectx) {
    UNUSED(ectx);
    // TODO: derived executors
    // TODO: add some memory or thread reservation
    switch (node.kind()) {
        case PlanNode::Kind::kAggregate:
            break;
        case PlanNode::Kind::kSort:
            break;
        case PlanNode::Kind::kFilter:
            break;
        case PlanNode::Kind::kGetEdges:
            break;
        case PlanNode::Kind::kGetVertices:
            break;
        case PlanNode::Kind::kGetNeighbors:
            break;
        case PlanNode::Kind::kLimit:
            break;
        case PlanNode::Kind::kProject:
            break;
        case PlanNode::Kind::kReadIndex:
            break;
        case PlanNode::Kind::kStart:
            break;
        case PlanNode::Kind::kUnion:
            break;
        case PlanNode::Kind::kIntersect:
            break;
        case PlanNode::Kind::kMinus:
            break;
        case PlanNode::Kind::kUnknown:
        default:
            LOG(FATAL) << "Unknown plan node kind.";
            break;
    }
    return nullptr;
}

Status Executor::finish(std::list<cpp2::Row> dataset) {
    return ectx_->addExecutor(id_, std::move(dataset));
}

}   // namespace graph
}   // namespace nebula
