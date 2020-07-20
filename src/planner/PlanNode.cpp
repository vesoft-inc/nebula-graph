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
const char* PlanNode::toString(PlanNode::Kind kind) {
    switch (kind) {
        case Kind::kUnknown:
            return "Unkonwn";
        case Kind::kStart:
            return "Start";
        case Kind::kGetNeighbors:
            return "GetNeighbors";
        case Kind::kGetVertices:
            return "GetVertices";
        case Kind::kGetEdges:
            return "GetEdges";
        case Kind::kReadIndex:
            return "ReadIndex";
        case Kind::kFilter:
            return "Filter";
        case Kind::kUnion:
            return "Union";
        case Kind::kIntersect:
            return "Intersect";
        case Kind::kMinus:
            return "Minus";
        case Kind::kProject:
            return "Project";
        case Kind::kSort:
            return "Sort";
        case Kind::kLimit:
            return "Limit";
        case Kind::kAggregate:
            return "Aggregate";
        case Kind::kSelect:
            return "Select";
        case Kind::kLoop:
            return "Loop";
        case Kind::kDedup:
            return "Dedup";
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
        case Kind::kAlterTag:
            return "AlterTag";
        case Kind::kAlterEdge:
            return "AlterEdge";
        case Kind::kInsertVertices:
            return "InsertVertices";
        case Kind::kInsertEdges:
            return "InsertEdges";
        case Kind::kDataCollect:
            return "DataCollect";
        // acl
        case PlanNode::Kind::kCreateUser:
            return "CreateUser";
        case PlanNode::Kind::kDropUser:
            return "DropUser";
        case PlanNode::Kind::kUpdateUser:
            return "UpdateUser";
        case PlanNode::Kind::kGrantRole:
            return "GrantRole";
        case PlanNode::Kind::kRevokeRole:
            return "RevokeRole";
        case PlanNode::Kind::kChangePassword:
            return "ChangePassword";
        case PlanNode::Kind::kListUserRoles:
            return "ListUserRoles";
        case PlanNode::Kind::kListUsers:
            return "ListUsers";
        case PlanNode::Kind::kListRoles:
            return "ListRoles";
        case PlanNode::Kind::kShowCreateSpace:
            return "ShowCreateSpace";
        case Kind::kShowCreateTag:
            return "ShowCreateTag";
        case Kind::kShowCreateEdge:
            return "ShowCreateEdge";
        case Kind::kDropSpace:
            return "DropSpace";
        case Kind::kDropTag:
            return "DropTag";
        case Kind::kDropEdge:
            return "DropEdge";
        case Kind::kShowSpaces:
            return "kShowSpaces";
        case Kind::kShowTags:
            return "kShowTags";
        case Kind::kShowEdges:
            return "kShowEdges";
        case Kind::kCreateSnapshot:
            return "CreateSnapshot";
        case Kind::kDropSnapshot:
            return "DropSnapshot";
        case Kind::kShowSnapshots:
            return "ShowSnapshots";
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
