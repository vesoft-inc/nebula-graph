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
#include "util/AnnoColGenerator.h"

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

    Status buildOneStepPlan();

    Status buildNStepsPlan();

    Status oneStep(PlanNode* input, const std::string& inputVarName);

    std::string buildInput();

    PlanNode* buildRuntimeInput();

    GetNeighbors::VertexProps buildSrcVertexProps();

    GetNeighbors::VertexProps buildDstVertexProps();

    GetNeighbors::EdgeProps buildEdgeProps();

    GetNeighbors::EdgeProps buildNStepLoopEdgeProps();

    Expression* buildNStepLoopCondition(int64_t steps) const;

    enum FromType {
        kConstantExpr,
        kVariable,
        kPipe,
    };

    void extractPropExprs(const Expression* expr);

    std::unique_ptr<Expression> rewriteToInputProp(Expression* expr);

    Status buildColumns();

private:
    int64_t                                                 steps_;
    FromType                                                fromType_{kConstantExpr};
    Expression*                                             src_{nullptr};
    std::vector<Value>                                      starts_;
    bool                                                    isOverAll_{false};
    std::vector<EdgeType>                                   edgeTypes_;
    storage::cpp2::EdgeDirection                            direction_;
    Expression*                                             filter_{nullptr};
    std::vector<std::string>                                colNames_;
    YieldColumns*                                           yields_{nullptr};
    bool                                                    distinct_{false};
    std::unique_ptr<YieldColumns>                           srcAndEdgePropCols_;
    std::unique_ptr<YieldColumns>                           dstPropCols_;
    std::unique_ptr<YieldColumns>                           inputPropCols_;
    std::unordered_map<const Expression*, YieldColumn*>     propExprColMap_;
    std::unique_ptr<AnnoColGenerator>                       annoColGen_;
    std::unique_ptr<Expression>                             newFilter_;
    std::unique_ptr<YieldColumns>                           newYieldCols_;
};
}  // namespace graph
}  // namespace nebula
#endif
