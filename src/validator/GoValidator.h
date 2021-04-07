/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_GOVALIDATOR_H_
#define VALIDATOR_GOVALIDATOR_H_

#include "planner/plan/Query.h"
#include "validator/TraversalValidator.h"

namespace nebula {
namespace graph {
class GoValidator final : public TraversalValidator {
public:
    GoValidator(Sentence* sentence, QueryContext* context)
        : TraversalValidator(sentence, context) {
    }

private:
    Status validateImpl() override;

    Status validateWhere(WhereClause* where);

    Status validateYield(YieldClause* yield);

    void extractPropExprs(const Expression* expr);

    Expression* rewriteToInputProp(const Expression* expr);

    Status buildColumns();

    AstContext* getAstContext() override {
        return goCtx_.get();
    }

private:
    std::unique_ptr<GoAstContext>   goCtx_;
};
}   // namespace graph
}   // namespace nebula
#endif
