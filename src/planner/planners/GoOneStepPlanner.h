/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLANNERS_GOONESTEPPLANNER_H_
#define PLANNER_PLANNERS_GOONESTEPPLANNER_H_

#include "planner/planners/GoPlanner.h"

namespace nebula {
namespace graph {
class GoOneStepPlanner final : public GoPlanner {
public:
    bool match(AstContext* astCtx) override;

    StatusOr<SubPlan> transform(AstContext* astCtx) override;

private:
    GoOneStepPlanner();

    static std::unique_ptr<GoOneStepPlanner>   instance_;
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_PLANNERS_GOONESTEPPLANNER_H_
