/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_RULESET_H_
#define PLANNER_RULESET_H_

#include <memory>
#include <set>

namespace nebula {
namespace graph {

class OptRule;

class RuleSet {
public:
    std::set<std::shared_ptr<OptRule>> getRules() const {
        return rules_;
    }

    void addRule(std::shared_ptr<OptRule> rule) {
        rules_.insert(rule);
    }

private:
    std::set<std::shared_ptr<OptRule>> rules_;
};

class DefaultRuleSet final : public RuleSet {
public:
    DefaultRuleSet();
};

}   // namespace graph
}   // namespace nebula

#endif   // PLANNER_RULESET_H_
