/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/RuleSet.h"

#include "planner/rule/FilterPushDownRule.h"

namespace nebula {
namespace graph {

DefaultRuleSet::DefaultRuleSet() {
    addRule(FilterPushDownRule::FILTER_PUSH_DOWN_RULE);
}

}   // namespace graph
}   // namespace nebula
