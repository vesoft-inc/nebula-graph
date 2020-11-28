/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLANNERS_MATCH_MATCHCLAUSEPLANNER_H_
#define PLANNER_PLANNERS_MATCH_MATCHCLAUSEPLANNER_H_

#include "planner/planners/match/CypherClausePlanner.h"

namespace nebula {
namespace graph {
/*
 * The MatchClausePlanner was designed to generate plan for match clause;
 */
class MatchClausePlanner final : public CypherClausePlanner {
public:
    MatchClausePlanner() = default;

    StatusOr<SubPlan> transform(CypherClauseContextBase* clauseCtx) override;

private:
    Status findStartVids();

    Status expand(const std::vector<NodeInfo>& nodeInfos,
                  const std::vector<EdgeInfo>& edgeInfos,
                  MatchClauseContext* matchClauseCtx,
                  int64_t startIndex,
                  SubPlan& subplan);

    Status appendFetchVertexPlan(const Expression* nodeFilter,
                                 QueryContext* qctx,
                                 SpaceInfo& space,
                                 SubPlan* plan);

    Status appendFilterPlan(SubPlan& plan);

private:
    Expression* initialExpr_;
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_PLANNERS_MATCH_MATCHCLAUSE_H_
