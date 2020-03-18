/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef SCHEDULER_PLANFRAGMENT_H_
#define SCHEDULER_PLANFRAGMENT_H_

#include <memory>
#include <vector>

namespace nebula {
namespace graph {

class PlanNode;
class Executor;
class ExecutionContext;

class PlanFragment final {
public:
    // The factory method for building plan fragment dependencies. For scheduling execution plans
    // parallelly, Split following plan node to different execution node which shares data by
    // memory:
    //  - Union
    //  - Intersect
    //  - Minus
    static std::shared_ptr<PlanFragment> createPlanFragmentFromPlanNode(
            const std::shared_ptr<PlanNode> &start);

    std::vector<PlanFragment *> depends() const {
        return depends_;
    }

    // Make the plan executable and return the first executor
    std::shared_ptr<Executor> convertPlan(ExecutionContext *ectx) const;

    uint64_t id() const {
        return id_;
    }

private:
    // Disable constructors since the static factory method
    PlanFragment() = default;

    // Find the last plan node from start point. The dummy node just for implementation convenience.
    static std::shared_ptr<PlanNode> getLastPlanNode(const std::shared_ptr<PlanNode> &start);

    // Implementation of getLastPlanNode function
    static std::shared_ptr<PlanNode> getLastPlanNodeInner(const std::shared_ptr<PlanNode> &node,
                                                          std::vector<PlanNode *> &visited);

    // Traverse forward from the start plan node, and find the start plan node of current plan
    // fragment .
    static void traversePlanNodeReversely(std::shared_ptr<PlanNode> node, PlanFragment *currPlan);

    // Make plan fragment start from union node
    static void makeUnionPlanFragment(std::shared_ptr<PlanNode> node, PlanFragment *currPlan);

    // Add a new dependency
    void dependsOn(PlanFragment *dep);

    // Clone a new plan fragment based on this one
    std::shared_ptr<PlanFragment> clone() const;

    void setStartNode(std::shared_ptr<PlanNode> start) {
        start_ = start;
    }

    // Plan fragment unique id
    uint64_t id_;

    // All dependent plan fragments
    std::vector<PlanFragment *> depends_;

    // The plan node started from here of each plan fragment
    std::shared_ptr<PlanNode> start_;
};

}   // namespace graph
}   // namespace nebula

#endif   // SCHEDULER_PLANFRAGMENT_H_
