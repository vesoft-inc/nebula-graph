/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLAN_H
#define PLANNER_PLAN_H

#include <memory>
#include <string>

namespace nebula {
namespace graph {

class PlanNode;
class ExecutionContext;

class Plan final {
public:
    explicit Plan(const std::string& id);

    void setRoot(std::shared_ptr<PlanNode> root) {
        root_ = root;
    }

    ExecutionContext* ectx() const {
        return ectx_.get();
    }

private:
    // Unique id to represent this plan. Maybe used to cache plan
    std::string id_;
    std::shared_ptr<PlanNode> root_;
    std::unique_ptr<ExecutionContext> ectx_;
};

}   // namespace graph
}   // namespace nebula

#endif   // PLANNER_PLAN_H
