/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/Utils.h"

namespace nebula {
namespace graph {

/*static*/ bool PlanNodeUtils::isSingleDependencyNode(const PlanNode* node) {
    switch (DCHECK_NOTNULL(node)->kind()) {
        case PlanNode::Kind::kFilter:
        case PlanNode::Kind::kProject:
        case PlanNode::Kind::kSort:
        case PlanNode::Kind::kLimit:
        case PlanNode::Kind::kAggregate:
        case PlanNode::Kind::kSelect:
        case PlanNode::Kind::kLoop:
        case PlanNode::Kind::kMultiOutputs:
        case PlanNode::Kind::kSwitchSpace:
        case PlanNode::Kind::kCreateSpace:
        case PlanNode::Kind::kCreateTag:
        case PlanNode::Kind::kCreateEdge:
        case PlanNode::Kind::kDescSpace:
        case PlanNode::Kind::kDescTag:
        case PlanNode::Kind::kDescEdge:
        case PlanNode::Kind::kInsertVertices:
        case PlanNode::Kind::kInsertEdges:
        case PlanNode::Kind::kGetNeighbors:
        case PlanNode::Kind::kAlterTag:
        case PlanNode::Kind::kAlterEdge:
        case PlanNode::Kind::kShowCreateSpace:
        case PlanNode::Kind::kShowCreateTag:
        case PlanNode::Kind::kShowCreateEdge:
        case PlanNode::Kind::kDropSpace:
        case PlanNode::Kind::kDropTag:
        case PlanNode::Kind::kDropEdge:
        case PlanNode::Kind::kShowSpaces:
        case PlanNode::Kind::kShowTags:
        case PlanNode::Kind::kShowEdges:
        case PlanNode::Kind::kCreateSnapshot:
        case PlanNode::Kind::kDropSnapshot:
        case PlanNode::Kind::kShowSnapshots:
            DCHECK_NOTNULL(dynamic_cast<const SingleInputNode*>(node));
            return true;
        default:
            DCHECK_NOTNULL(dynamic_cast<const SingleInputNode*>(node));
            return false;
    }
}

}  // namespace graph
}  // namespace nebula
