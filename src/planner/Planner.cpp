/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/Planner.h"

namespace nebula {
namespace graph {

Planner::Planner(std::set<std::shared_ptr<OptRule>> rules) : rules_(std::move(rule)) {
    UNUSED(rules_);
}

std::shared_ptr<PlanNode> Planner::optimize(std::shared_ptr<PlanNode> node) const {
    for (auto &rule : rules_) {
        if (rule->match(node)) {
            // TODO: Use an algorithm to apply all rules recursively
            node = rule->transform(node);
        }
    }

    for (size_t i = 0; i < node->table().size(); ++i) {
        node->setNthNext(i, optimize(next));
    }

    // TODO: Return the new plan's root node
    return node;
}

}   // namespace graph
}   // namespace nebula
