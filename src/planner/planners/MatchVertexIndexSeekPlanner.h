/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLANNERS_MATCHVERTEXINDEXSEEKPLANNER_H_
#define PLANNER_PLANNERS_MATCHVERTEXINDEXSEEKPLANNER_H_

#include "context/QueryContext.h"
#include "planner/Planner.h"

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
    static Expression* makeIndexFilter(const std::string& label,
                                       const MapExpression* map,
                                       QueryContext* qctx);

    static Expression* makeIndexFilter(const std::string& label,
                                       const std::string& alias,
                                       const Expression* filter,
                                       QueryContext* qctx);

    MatchVertexIndexSeekPlanner() = default;
};
}  // namespace graph
}  // namespace nebula
#endif
