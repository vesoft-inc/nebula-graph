/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLANNERS_MATCHVERTEXINDEXSEEKPLANNER_H_
#define PLANNER_PLANNERS_MATCHVERTEXINDEXSEEKPLANNER_H_

#include "planner/Planner.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {
class MatchVertexIndexSeekPlanner final : public Planner {
public:
    static std::unique_ptr<MatchVertexIndexSeekPlanner> make() {
        return std::unique_ptr<MatchVertexIndexSeekPlanner>(new MatchVertexIndexSeekPlanner());
    }

    static bool match(AstContext* astCtx);

    StatusOr<SubPlan> transform(AstContext* astCtx) override;

private:
    MatchVertexIndexSeekPlanner() = default;
};

class MatchVertexIndexSeekPlannerRegister final {
private:
    MatchVertexIndexSeekPlannerRegister() {
        auto& planners = Planner::plannersMap()[Sentence::Kind::kMatch];
        planners.emplace_back(&MatchVertexIndexSeekPlanner::match,
                              &MatchVertexIndexSeekPlanner::make);
    }

    static MatchVertexIndexSeekPlannerRegister instance_;
};
}  // namespace graph
}  // namespace nebula
#endif
