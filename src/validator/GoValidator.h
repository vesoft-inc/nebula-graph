/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_GOVALIDATOR_H_
#define VALIDATOR_GOVALIDATOR_H_

#include "common/base/Base.h"
#include "validator/Validator.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {
class GoValidator final : public Validator {
public:
    GoValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {}

private:
    Status validateImpl() override;

    Status toPlan() override;

    Status validateStep(const StepClause* step);

    Status validateFrom(const FromClause* from);

    Status validateOver(const OverClause* over);

    Status validateWhere(const WhereClause* where);

    Status validateYield(const YieldClause* yield);

    void extractPropExprs(const Expression* expr);

    std::unique_ptr<Expression> rewriteToInputProp(Expression* expr);

    Status buildColumns();

    Status buildOneStepPlan();

    Status buildNStepsPlan();

    Status oneStep(PlanNode* dependencyForGn, const std::string& inputVarNameForGN,
                   PlanNode* projectFromJoin);

    std::string buildConstantInput();

    PlanNode* buildRuntimeInput();

    GetNeighbors::VertexProps buildSrcVertexProps();

    std::vector<storage::cpp2::VertexProp> buildDstVertexProps();

    GetNeighbors::EdgeProps buildEdgeProps();

    GetNeighbors::EdgeProps buildNStepLoopEdgeProps();

    Expression* buildNStepLoopCondition(int64_t steps) const;

    Project* ifBuildLeftVarForTraceJoin(PlanNode* projectStartVid);

    Project* projectDstVidsFromGN(PlanNode* gn, const std::string& outputVar);

    Project* ifTraceToStartVid(Project* projectLeftVarForJoin,
                               Project* projectDstFromGN);

    PlanNode* ifBuildJoinPipeInput(PlanNode* gn,
                                   PlanNode* projectFromJoin);

    PlanNode* ifBuildProjectSrcEdgePropsForGN(PlanNode* gn);

    PlanNode* ifBuildJoinDstProps(PlanNode* projectSrcDstProps);

    enum FromType {
        kConstantExpr,
        kVariable,
        kPipe,
    };

private:
    int64_t                                                 steps_;
    FromType                                                fromType_{kConstantExpr};
    Expression*                                             srcRef_{nullptr};
    Expression*                                             src_{nullptr};
    std::vector<Value>                                      starts_;
    bool                                                    isOverAll_{false};
    std::vector<EdgeType>                                   edgeTypes_;
    storage::cpp2::EdgeDirection                            direction_;
    Expression*                                             filter_{nullptr};
    std::vector<std::string>                                colNames_;
    YieldColumns*                                           yields_{nullptr};
    bool                                                    distinct_{false};

    // Generated by validator if needed, and the lifecycle of raw pinters would
    // be managed by object pool
    YieldColumns*                                           srcAndEdgePropCols_{nullptr};
    YieldColumns*                                           dstPropCols_{nullptr};
    YieldColumns*                                           inputPropCols_{nullptr};
    std::unordered_map<std::string, YieldColumn*>           propExprColMap_;
    Expression*                                             newFilter_{nullptr};
    YieldColumns*                                           newYieldCols_{nullptr};
    // Used for n steps to trace the path
    std::string                                             srcVidColName_;
    std::string                                             dstVidColName_;
    std::string                                             firstBeginningSrcVidColName_;
    // Used for get dst props
    std::string                                             joinDstVidColName_;
};
}  // namespace graph
}  // namespace nebula
#endif
