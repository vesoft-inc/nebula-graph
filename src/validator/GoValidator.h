/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_GOVALIDATOR_H_
#define VALIDATOR_GOVALIDATOR_H_

#include "planner/plan/Query.h"
#include "validator/TraversalValidator.h"
#include "context/ast/QueryAstContext.h"

namespace nebula {
namespace graph {
class GoValidator final : public TraversalValidator {
public:
    using VertexProp = nebula::storage::cpp2::VertexProp;
    using EdgeProp = nebula::storage::cpp2::EdgeProp;
    GoValidator(Sentence* sentence, QueryContext* context)
        : TraversalValidator(sentence, context) {}

private:
    Status validateImpl() override;

    Status toPlan() override;

    Status validateWhere(WhereClause* where);

    Status validateYield(YieldClause* yield);

    void extractPropExprs(const Expression* expr);

    Expression* rewrite2VarProp(const Expression* expr);

    Status buildColumns();

    std::vector<std::string> buildDstVertexColNames();

private:
    YieldColumns* yields() const {
        return newYieldCols_ ? newYieldCols_ : yields_;
    }

    Expression* filter() const {
        return newFilter_ ? newFilter_ : filter_;
    }

    std::unique_ptr<GoContext> goCtx_;

    std::vector<std::string> colNames_;
    YieldColumns* yields_{nullptr};

    YieldColumns* inputPropCols_{nullptr};
    std::unordered_map<std::string, YieldColumn*> propExprColMap_;
    Expression* newFilter_{nullptr};
    YieldColumns* newYieldCols_{nullptr};
};
}   // namespace graph
}   // namespace nebula
#endif
