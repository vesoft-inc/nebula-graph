/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/OptRule.h"
namespace nebula {
namespace graph {

// Transform plan based on the plan node
StatusOr<std::shared_ptr<PlanNode>> OptRule::transform(std::shared_ptr<PlanNode>) {
    return nullptr;
}

// Check whether this rule will match on the plan node
bool OptRule::match(const std::shared_ptr<PlanNode>&) const {
    return false;
}

QueryContext* OptRule::queryContext() {
    return qctx_;
}

}   // namespace graph
}   // namespace nebula
