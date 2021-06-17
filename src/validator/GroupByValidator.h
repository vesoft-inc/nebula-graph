/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_GROUPBY_VALIDATOR_H_
#define VALIDATOR_GROUPBY_VALIDATOR_H_

#include "common/base/Base.h"
#include "planner/plan/Query.h"
#include "validator/Validator.h"

namespace nebula {
namespace graph {

class GroupByValidator final : public Validator {
public:
    GroupByValidator(Sentence *sentence, QueryContext *context) : Validator(sentence, context) {}

    Status validateImpl() override;

    Status toPlan() override;

private:
    Status validateGroup(const GroupClause *groupClause);

    Status validateYield(const YieldClause *yieldClause);

    Status groupClauseSemanticCheck();

private:
    std::vector<Expression *> yieldCols_;

    // key: alias, value: input name
    std::unordered_map<std::string, YieldColumn *> aliases_;

    bool needGenProject_{false};
    std::vector<std::string> outputColumnNames_;

    // used to generate Project node when there is an internally nested aggregateExpression
    YieldColumns *projCols_;

    std::vector<Expression *> groupKeys_;
    std::vector<Expression *> groupItems_;
};

}   // namespace graph
}   // namespace nebula
#endif
