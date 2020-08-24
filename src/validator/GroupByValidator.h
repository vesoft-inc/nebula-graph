/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_GROUPBY_VALIDATOR_H_
#define VALIDATOR_GROUPBY_VALIDATOR_H_

#include "common/base/Base.h"
#include "validator/Validator.h"
#include "planner/Query.h"


namespace nebula {
namespace graph {

class GroupByValidator final : public Validator {
public:
    GroupByValidator(Sentence *sentence, QueryContext *context)
        : Validator(sentence, context) {}

private:
    GraphStatus validateImpl() override;

    GraphStatus toPlan() override;

    GraphStatus validateGroup(const GroupClause *groupClause);

    GraphStatus validateYield(const YieldClause *yieldClause);

    GraphStatus checkInputProps() const;

    GraphStatus checkVarProps() const;

private:
    std::vector<YieldColumn*>                         groupCols_;
    std::vector<YieldColumn*>                         yieldCols_;

    // key: alias, value: input name
    std::unordered_map<std::string, YieldColumn*>     aliases_;

    std::vector<std::string>                          outputColumnNames_;

    ExpressionProps                                   exprProps_;

    std::vector<Expression*>                          groupKeys_;
    std::vector<Aggregate::GroupItem>                 groupItems_;
};


}  // namespace graph
}  // namespace nebula
#endif
