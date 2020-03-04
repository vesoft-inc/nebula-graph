/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/OptRule.h"

#include "planner/PlanNode.h"

namespace nebula {
namespace graph {

OptRule::OptRule(const std::string &name) : name_(name) {
    void(name_);
}

std::shared_ptr<PlanNode> OptRule::transform(std::shared_ptr<PlanNode> node) const {
    // Do nothing
    return node;
}

}   // namespace graph
}   // namespace nebula
