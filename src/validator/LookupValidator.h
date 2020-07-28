/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef VALIDATOR_LOOKUPVALIDATOR_H_
#define VALIDATOR_LOOKUPVALIDATOR_H_

#include "common/base/Base.h"
#include "validator/Validator.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {
class LookupValidator final : public Validator {
public:
    LookupValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {}

private:
    Status validateImpl() override;

    Status toPlan() override;

    Status validateFrom(const std::string* from);

    Status validateWhere(const WhereClause* where);

    Status validateYield(const YieldClause* yield);

    bool isEdgeIndex();

    int32_t schemaId();

    Status getSchema();

    Status validateFilter(const Expression* expr);

    Status checkPropExpression(const SymbolPropertyExpression* expr);

    Lookup::IndexQueryCtx buildIndexQueryCtx();

    Lookup::IndexReturnCols buildIndexReturnCols();

private:
    meta::cpp2::SchemaID                                    schemaId_;
    std::string                                             from_;
    std::vector<std::string>                                returnCols_{};
    YieldColumns*                                           yields_{nullptr};
    Expression*                                             filter_{nullptr};
    std::shared_ptr<const meta::NebulaSchemaProvider>       schema_{nullptr};
};
}  // namespace graph
}  // namespace nebula
#endif  // VALIDATOR_LOOKUPVALIDATOR_H_
