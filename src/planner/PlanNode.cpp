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
            return "EndNode";
        case Kind::kUnion:
            return "UnionNode";
        case Kind::kLoop:
            return "LoopNode";
        case Kind::kSort:
            return "SortNode";
        case Kind::kDedup:
            return "DedupNode";
        case Kind::kMinus:
            return "MinusNode";
        case Kind::kIntersect:
            return "IntersectNode";
        case Kind::kAggregate:
            return "AggregateNode";
        case Kind::kFilter:
            return "FilterNode";
        case Kind::kGetEdges:
            return "GetEdgesNode";
        case Kind::kGetVertices:
            return "GetVerticesNode";
        case Kind::kGetNeighbors:
            return "GetNeighborsNode";
        case Kind::kLimit:
            return "LimitNode";
        case Kind::kProject:
            return "ProjectNode";
        case Kind::kSelector:
            return "SelectorNode";
        case Kind::kReadIndex:
            return "ReadIndexNode";
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
    os << PlanNode::toString(kind);
    return os;
}

}   // namespace graph
}   // namespace nebula
