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
        case Kind::kStart:
            return "Start";
        case Kind::kUnion:
            return "Union";
        case Kind::kLoop:
            return "Loop";
        case Kind::kSort:
            return "Sort";
        case Kind::kDedup:
            return "Dedup";
        case Kind::kMinus:
            return "Minus";
        case Kind::kIntersect:
            return "Intersect";
        case Kind::kAggregate:
            return "Aggregate";
        case Kind::kFilter:
            return "Filter";
        case Kind::kGetEdges:
            return "GetEdges";
        case Kind::kGetVertices:
            return "GetVertices";
        case Kind::kGetNeighbors:
            return "GetNeighbors";
        case Kind::kLimit:
            return "Limit";
        case Kind::kProject:
            return "Project";
        case Kind::kSelector:
            return "Selector";
        case Kind::kReadIndex:
            return "ReadIndex";
        case Kind::kMultiOutputs:
            return "MultiOutputs";
        case Kind::kSwitchSpace:
            return "RegisterSpaceToSession";
        case Kind::kUnknown:
        default:
            return "kUnknown";
    }
}

std::ostream& operator<<(std::ostream& os, PlanNode::Kind kind) {
    switch (kind) {
        case PlanNode::Kind::kUnknown:
            os << "Unkonwn";
            break;
        case PlanNode::Kind::kStart:
            os << "Start";
            break;
        case PlanNode::Kind::kGetNeighbors:
            os << "GetNeighbors";
            break;
        case PlanNode::Kind::kGetVertices:
            os << "GetVertices";
            break;
        case PlanNode::Kind::kGetEdges:
            os << "GetEdges";
            break;
        case PlanNode::Kind::kReadIndex:
            os << "ReadIndex";
            break;
        case PlanNode::Kind::kFilter:
            os << "Filter";
            break;
        case PlanNode::Kind::kUnion:
            os << "Union";
            break;
        case PlanNode::Kind::kIntersect:
            os << "Intersect";
            break;
        case PlanNode::Kind::kMinus:
            os << "Minus";
            break;
        case PlanNode::Kind::kProject:
            os << "Project";
            break;
        case PlanNode::Kind::kSort:
            os << "Sort";
            break;
        case PlanNode::Kind::kLimit:
            os << "Limit";
            break;
        case PlanNode::Kind::kAggregate:
            os << "Aggregate";
            break;
        case PlanNode::Kind::kSelector:
            os << "Selector";
            break;
        case PlanNode::Kind::kLoop:
            os << "Loop";
            break;
        case PlanNode::Kind::kSwitchSpace:
            os << "SwitchSpace";
            break;
        case PlanNode::Kind::kDedup:
            os << "Dedup";
            break;
        case PlanNode::Kind::kMultiOutputs:
            os << "MultiOutputs";
            break;
        case PlanNode::Kind::kCreateSpace:
            os << "CreateSpace";
            break;
        case PlanNode::Kind::kCreateTag:
            os << "CreateTag";
            break;
        case PlanNode::Kind::kCreateEdge:
            os << "CreateEdge";
            break;
        case PlanNode::Kind::kDescSpace:
            os << "DescSpace";
            break;
        case PlanNode::Kind::kDescTag:
            os << "DescTag";
            break;
        case PlanNode::Kind::kDescEdge:
            os << "DescEdge";
            break;
        case PlanNode::Kind::kInsertVertices:
            os << "InsertVertices";
            break;
        case PlanNode::Kind::kInsertEdges:
            os << "InsertEdges";
            break;
        default:
            LOG(FATAL) << "Unkown PlanNode: " << static_cast<int64_t>(kind);
    }
    return os;
}
}   // namespace graph
}   // namespace nebula
