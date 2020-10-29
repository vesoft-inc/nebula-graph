/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLANNERS_MATCHVERTEXIDSEEK_H_
#define PLANNER_PLANNERS_MATCHVERTEXIDSEEK_H_

#include "context/QueryContext.h"
#include "planner/Planner.h"

namespace nebula {
namespace graph {
class MatchVertexIdSeekPlanner final : public Planner {
public:
    static std::unique_ptr<MatchVertexIdSeekPlanner> make() {
        return std::unique_ptr<MatchVertexIdSeekPlanner>(new MatchVertexIdSeekPlanner());
    }

    static bool match(AstContext* astCtx);

    static StatusOr<std::pair<std::string, Expression*>> extractVids(const Expression* filter,
                                                                     QueryContext* qctx);

    static std::pair<std::string, Expression*> listToAnnoVarVid(const List& list,
                                                                QueryContext* qctx);

    static std::pair<std::string, Expression*> constToAnnoVarVid(const Value& v,
                                                                 QueryContext* qctx);

    StatusOr<SubPlan> transform(AstContext* astCtx) override;

private:
    MatchVertexIdSeekPlanner() = default;
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_PLANNERS_MATCHVERTEXIDSEEK_H_
