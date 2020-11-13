/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLANNERS_MATCHVARIABLELENGTHPATTERNINDEXSCANPLANNER_H_
#define PLANNER_PLANNERS_MATCHVARIABLELENGTHPATTERNINDEXSCANPLANNER_H_

#include "planner/Planner.h"
#include "validator/MatchValidator.h"

namespace nebula {
namespace graph {

class PlanNode;
struct SubPlan;

class MatchVariableLengthPatternIndexScanPlanner final : public Planner {
public:
    static std::unique_ptr<MatchVariableLengthPatternIndexScanPlanner> make();

    static bool match(AstContext* astCtx);

    StatusOr<SubPlan> transform(AstContext* astCtx) override;

private:
    MatchVariableLengthPatternIndexScanPlanner() = default;

    Status scanIndex(SubPlan* plan);

    // Generate plan:
    //  (v)-[e:et*m..n]- + (n)-[e1:et2*k..l]-
    Status composePlan(SubPlan* finalPlan);

    // Generate data join plan node
    PlanNode* joinDataSet(const PlanNode* right, const PlanNode* left);

    // Generate fetch final vertex props sub-plan
    Status appendFetchVertexPlan(const PlanNode* input, SubPlan* plan);

    // (v)-[e:et*m..n]- plan + Filter + PassThroughNode
    Status filterFinalDataset(const MatchValidator::EdgeInfo& edge,
                              const PlanNode* input,
                              SubPlan* plan);

    // Generate subplan for pattern which is like:
    //   (v)-[e:edgetype*m..n{prop: value}]-
    Status composeSubPlan(const MatchValidator::EdgeInfo& edge,
                          const PlanNode* input,
                          SubPlan* plan);

    // Generate subplan composite of following nodes:
    //   Project -> Dedup -> GetNeighbors -> [Filter ->] [Filter ->] Project
    Status expandStep(const MatchValidator::EdgeInfo& edge, const PlanNode* input, SubPlan* plan);

    // Generate subplan composite of following nodes:
    //   DataJoin -> Project -> PassThroughNode -> Union
    Status collectData(const PlanNode* joinLeft,
                       const PlanNode* joinRight,
                       const PlanNode* inUnionNode,
                       PlanNode** passThrough,
                       SubPlan* plan);

    template <typename T>
    T* saveObject(T* obj) const {
        return matchCtx_->qctx->objPool()->add(obj);
    }

    MatchAstContext* matchCtx_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif   // PLANNER_PLANNERS_MATCHVARIABLELENGTHPATTERNINDEXSCANPLANNER_H_
