/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MATCH_SEGMENTSCONNECTOR_H_
#define PLANNER_MATCH_SEGMENTSCONNECTOR_H_

#include "context/QueryContext.h"
#include "context/ast/QueryAstContext.h"
#include "planner/PlanNode.h"
#include "planner/Planner.h"
#include "planner/match/InnerJoinStrategy.h"

namespace nebula {
namespace graph {
/**
 * The SegmentsConnector was designed to be a util to help connecting the
 * plan segment.
 */
class SegmentsConnector final {
public:
    enum class ConnectStrategy : int8_t {
        kAddDependency,
        kInnerJoin,
        kLeftOuterJoin,
        kCartesianProduct,
        kUnion,
        kApply,
    };

    SegmentsConnector() = default;

    // Analyse the relation of two segments and connect them.
    StatusOr<SubPlan> connectSegments(CypherClauseContextBase* leftCtx,
                                      CypherClauseContextBase* rightCtx,
                                      SubPlan& left,
                                      SubPlan& right,
                                      QueryContext* qctx = nullptr);

    static PlanNode* innerJoinSegments(
        QueryContext* qctx,
        const PlanNode* left,
        const PlanNode* right,
        InnerJoinStrategy::JoinPos leftPos = InnerJoinStrategy::JoinPos::kEnd,
        InnerJoinStrategy::JoinPos rightPos = InnerJoinStrategy::JoinPos::kStart);

    static PlanNode* cartesianProductSegments(QueryContext* qctx,
                                              const PlanNode* left,
                                              const PlanNode* right);

    static PlanNode* applySegments(QueryContext* qctx,
                                   const PlanNode* left,
                                   const PlanNode* right,
                                   const std::string& rowIndex);

    static void addDependency(const PlanNode* left, const PlanNode* right);

    static void addInput(const PlanNode* left, const PlanNode* right, bool copyColNames = false);

private:
    static PlanNode* iterateDataSet(QueryContext* qctx,
                                    PlanNode* input,
                                    const std::string& rowIndex);

    static PlanNode* transformDataSet(QueryContext* qctx, PlanNode* input);

    static Status rewriteMatchClause(QueryContext* qctx,
                                     MatchClauseContext* mctx,
                                     SubPlan& plan,
                                     PlanNode* input,
                                     const std::string& rowIndex = "");

    static void collectPlanNodes(const PlanNode* root,
                                 const PlanNode* tail,
                                 PlanNode::Kind kind,
                                 std::vector<PlanNode*>& result);

private:
    bool isUnwinding_{false};
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_MATCH_SEGMENTSCONNECTOR_H_
