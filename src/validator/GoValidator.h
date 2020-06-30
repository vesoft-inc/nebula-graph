/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_GOVALIDATOR_H_
#define VALIDATOR_GOVALIDATOR_H_

#include "common/base/Base.h"
#include "validator/Validator.h"

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

    Status deduceProps(const Expression* expr);
    bool evaluableExpr(const Expression* expr) const;

private:
    Expression*                     src_{nullptr};
    std::vector<Value>              starts_;
    bool                            isOverAll_{false};
    std::vector<EdgeType>           edgeTypes_;
    storage::cpp2::EdgeDirection    direction_;
    Expression*                     filter_{nullptr};
    std::unordered_map<TagID, std::vector<std::string>>     srcTagProps_;
    std::unordered_map<TagID, std::vector<std::string>>     dstTagProps_;
    std::unordered_map<EdgeType, std::vector<std::string>>  edgeProps_;
    std::vector<std::string>                                colNames_;
};
}  // namespace graph
}  // namespace nebula
#endif
