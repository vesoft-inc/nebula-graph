/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_TRAVERSALVALIDATOR_H_
#define VALIDATOR_TRAVERSALVALIDATOR_H_

#include "common/base/Base.h"
#include "context/ast/QueryAstContext.h"
#include "planner/plan/Query.h"
#include "validator/Validator.h"

namespace nebula {
namespace graph {

// some utils for the validator to traverse the graph
class TraversalValidator : public Validator {
public:
    static void buildConstantInput(QueryContext* qctx,
                                   Starts& starts,
                                   std::string& startVidsVar);

    static PlanNode* buildRuntimeInput(QueryContext* qctx,
                                       Starts& starts,
                                       PlanNode*& project);

protected:
    TraversalValidator(Sentence* sentence, QueryContext* qctx) : Validator(sentence, qctx) {
        startVidList_.reset(new ExpressionList());
        loopSteps_ = vctx_->anonVarGen()->getVar();
    }

    Status validateStarts(const VerticesClause* clause, Starts& starts);

    Status validateOver(const OverClause* clause, Over& over);

    Status validateStep(const StepClause* clause, StepClause& step);

    PlanNode* projectDstVidsFromGN(PlanNode* gn, const std::string& outputVar);

    Expression* buildNStepLoopCondition(uint32_t steps) const;

protected:
    Starts                from_;
    StepClause            steps_;
    std::string           srcVidColName_;
    PlanNode*             projectStartVid_{nullptr};
    std::string           loopSteps_;

    std::unique_ptr<ExpressionList>  startVidList_;
};

}  // namespace graph
}  // namespace nebula

#endif
