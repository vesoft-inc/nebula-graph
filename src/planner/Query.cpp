/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "Query.h"

#include <folly/String.h>

using folly::stringPrintf;

namespace nebula {
namespace graph {
std::string GetNeighborsNode::explain() const {
    // TODO:
    return "GetNeighborsNode";
}

std::string GetVerticesNode::explain() const {
    // TODO:
    return "GetVerticesNode";
}

std::string GetEdgesNode::explain() const {
    // TODO:
    return "GetEdgesNode";
}

std::string ReadIndexNode::explain() const {
    // TODO:
    return "ReadIndexNode";
}

std::string FilterNode::explain() const {
    // TODO:
    return "FilterNode";
}

std::string UnionNode::explain() const {
    // TODO:
    return "UnionNode";
}

std::string IntersectNode::explain() const {
    // TODO:
    return "IntersectNode";
}

std::string MinusNode::explain() const {
    // TODO:
    return "MinusNode";
}

std::string ProjectNode::explain() const {
    // TODO:
    return "ProjectNode";
}

std::string SortNode::explain() const {
    // TODO:
    return "SortNode";
}

std::string LimitNode::explain() const {
    std::string buf;
    buf.reserve(256);
    buf += "LimitNode: ";
    buf += folly::stringPrintf("offset %ld, count %ld", offset_, count_);
    return buf;
}

std::string AggregateNode::explain() const {
    // TODO:
    return "AggregateNode";
}

std::string SelectorNode::explain() const {
    return "SelectorNode";
}

LoopNode::LoopNode(ExecutionPlan* plan, PlanNode* input, PlanNode* body, Expression* condition)
    : BinarySelect(plan, Kind::kLoop, input, condition), body_(body) {}

std::string LoopNode::explain() const {
    return "LoopNode";
}

std::string SwitchSpaceNode::explain() const {
    return "SwitchSpaceNode";
}

std::string DedupNode::explain() const {
    return "DedupNode";
}
}  // namespace graph
}  // namespace nebula
