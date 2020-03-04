/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_OPTRULE_H_
#define PLANNER_OPTRULE_H_

#include <memory>
#include <string>

namespace nebula {
namespace graph {

class PlanNode;

class OptRule {
public:
    // Transform plan based on the plan node
    virtual std::shared_ptr<PlanNode> transform(std::shared_ptr<PlanNode> node) const;

    // Check whether this rule will match on the plan node
    virtual bool match(const std::shared_ptr<PlanNode>& node) const {
        return false;
    }

    std::string name() const {
        return name_;
    }

protected:
    explicit OptRule(const std::string& name);
};

}   // namespace graph
}   // namespace nebula

#endif   // PLANNER_OPTRULE_H_
