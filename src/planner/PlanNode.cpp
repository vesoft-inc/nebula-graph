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
        case Kind::kEnd:
            return "End";
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
        case Kind::kUnknown:
            return "Unknown";
            // no default so the compiler will warning when lack one enumerate
    }
    return "Unkown";
}

std::ostream& operator<<(std::ostream& os, PlanNode::Kind kind) {
    os << PlanNode::toString(kind);
    return os;
}

}   // namespace graph
}   // namespace nebula
