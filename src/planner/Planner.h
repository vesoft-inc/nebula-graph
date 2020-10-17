/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLANNERS_PLANNER_H_
#define PLANNER_PLANNERS_PLANNER_H_

#include "common/base/Base.h"
#include "planner/PlanNode.h"

namespace nebula {
namespace graph {
class Validator;

struct SubPlan {
    // root and tail of a subplan.
    PlanNode*   root{nullptr};
    PlanNode*   tail{nullptr};
};

class Planner {
public:
    virtual ~Planner() = default;

    static StatusOr<SubPlan> toPlan(Validator* validator);

    virtual bool match(Validator* validator) = 0;

    virtual SubPlan transform() = 0;

    auto& plannersMap() {
        return plannersMap_;
    }

protected:
    Planner() = default;

private:
    static std::unordered_map<Sentence::Kind, std::vector<Planner*>> plannersMap_;
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_PLANNERS_PLANNER_H_
