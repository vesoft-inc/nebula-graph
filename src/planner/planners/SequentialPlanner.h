/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLANNERS_SEQUENTIALPLANNER_H_
#define PLANNER_PLANNERS_SEQUENTIALPLANNER_H_

#include "planner/Planner.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {
class SequentialPlanner final : public Planner {
public:
    bool match(AstContext* astCtx) override;

    StatusOr<SubPlan> transform(AstContext* astCtx) override;

    void ifBuildDataCollect(SubPlan& subPlan, QueryContext* qctx);

private:
    SequentialPlanner();

    static std::unique_ptr<SequentialPlanner>    instance_;
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_PLANNERS_SEQUENTIALPLANNER_H_
