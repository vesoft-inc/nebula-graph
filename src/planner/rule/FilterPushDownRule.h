/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_RULE_FILTERPUSHDOWNRULE_H_
#define PLANNER_RULE_FILTERPUSHDOWNRULE_H_

#include <memory>

#include "planner/OptRule.h"

namespace nebula {
namespace graph {

class FilterPushDownRule final : public OptRule {
public:
    static const std::shared_ptr<FilterPushDownRule> FILTER_PUSH_DOWN_RULE;

    bool match(std::shared_ptr<PlanNode> node) const override;

    std::shared_ptr<PlanNode> transform(std::shared_ptr<PlanNode> node) const override;

private:
    FilterPushDownRule() : OptRule("FilterPushDownRule") {}
};

}   // namespace graph
}   // namespace nebula

#endif   // PLANNER_RULE_FILTERPUSHDOWNRULE_H_
