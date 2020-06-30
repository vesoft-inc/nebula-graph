/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/PlanNode.h"
#include "planner/ExecutionPlan.h"

namespace nebula {
namespace graph {

PlanNode::PlanNode(ExecutionPlan* plan, Kind kind) : kind_(kind), plan_(plan) {
    DCHECK_NOTNULL(plan_);
    plan_->addPlanNode(this);
}

// static
const char* PlanNode::toString(Kind kind) {
    switch (kind) {
        case PlanNode::Kind::kUnknown:
            return "Unkonwn";
        case PlanNode::Kind::kStart:
            return "Start";
        case PlanNode::Kind::kGetNeighbors:
            return "GetNeighbors";
        case PlanNode::Kind::kGetVertices:
            return "GetVertices";
        case PlanNode::Kind::kGetEdges:
            return "GetEdges";
        case PlanNode::Kind::kReadIndex:
            return "ReadIndex";
        case PlanNode::Kind::kFilter:
            return "Filter";
        case PlanNode::Kind::kUnion:
            return "Union";
        case PlanNode::Kind::kIntersect:
            return "Intersect";
        case PlanNode::Kind::kMinus:
            return "Minus";
        case PlanNode::Kind::kProject:
            return "Project";
        case PlanNode::Kind::kSort:
            return "Sort";
        case PlanNode::Kind::kLimit:
            return "Limit";
        case PlanNode::Kind::kAggregate:
            return "Aggregate";
        case PlanNode::Kind::kSelect:
            return "Select";
        case PlanNode::Kind::kLoop:
            return "Loop";
        case PlanNode::Kind::kDedup:
            return "Dedup";
        case PlanNode::Kind::kMultiOutputs:
            return "MultiOutputs";
        case Kind::kSwitchSpace:
            return "RegisterSpaceToSession";
        case Kind::kCreateSpace:
            return "CreateSpace";
        case Kind::kCreateTag:
            return "CreateTag";
        case Kind::kCreateEdge:
            return "CreateEdge";
        case Kind::kDescSpace:
            return "DescSpace";
        case Kind::kDescTag:
            return "DescTag";
        case Kind::kDescEdge:
            return "DescEdge";
        case Kind::kInsertVertices:
            return "InsertVertices";
        case Kind::kInsertEdges:
            return "InsertEdges";
        case PlanNode::Kind::kDataCollect:
            return "DataCollect";
        // no default so the compiler will warning when lack one enumerate
    }
    LOG(FATAL) << "Impossible kind plan node " << static_cast<int>(kind);
}

std::ostream& operator<<(std::ostream& os, PlanNode::Kind kind) {
    os << PlanNode::toString(kind);
    return os;
}

}   // namespace graph
}   // namespace nebula
