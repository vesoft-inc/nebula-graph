/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_GOPLANNER_H_
#define PLANNER_GOPLANNER_H_

#include "context/QueryContext.h"
#include "context/ast/QueryAstContext.h"
#include "planner/Planner.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {
class GoPlanner final : public Planner {
public:
    static std::unique_ptr<GoPlanner> make() {
        return std::unique_ptr<GoPlanner>(new GoPlanner());
    }

    static bool match(AstContext* astCtx);

    StatusOr<SubPlan> transform(AstContext* astCtx) override;


    std::vector<std::string> buildDstVertexColNames();

    GetNeighbors::VertexProps buildSrcVertexProps();

    std::vector<storage::cpp2::VertexProp> buildDstVertexProps();

    GetNeighbors::EdgeProps buildEdgeProps();

    GetNeighbors::EdgeProps buildEdgeDst();

    void buildEdgeProps(GetNeighbors::EdgeProps& edgeProps, bool isInEdge);

    PlanNode* buildJoinPipeOrVariableInput(PlanNode* projectFromJoin,
                                           PlanNode* dependencyForJoinInput);

    PlanNode* buildProjectSrcEdgePropsForGN(std::string gnVar,
                                            PlanNode* dependency,
                                            bool needJoinInput,
                                            bool needJoinDst);

    PlanNode* buildJoinDstProps(PlanNode* projectSrcDstProps);

    PlanNode* buildTraceProjectForGN(std::string gnVar,
                                     PlanNode* dependency,
                                     bool needJoinInput,
                                     bool needJoinDst);

    void buildConstantInput(Starts& starts, std::string& startVidsVar);

    PlanNode* buildRuntimeInput(Starts& starts, PlanNode*& project);

private:
     GoPlanner() = default;

     GoAstContext*  goCtx_{nullptr};
};
}  // namespace graph
}  // namespace nebula
#endif
