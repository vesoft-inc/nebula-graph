/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef _VALIDATOR_INDEXSCAN_VALIDATOR_H_
#define _VALIDATOR_INDEXSCAN_VALIDATOR_H_

#include <memory>

#include "common/base/Base.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "common/plugin/fulltext/elasticsearch/ESGraphAdapter.h"
#include "parser/TraverseSentences.h"
#include "planner/plan/Query.h"
#include "validator/Validator.h"

namespace nebula {

namespace meta {
class NebulaSchemaProvider;
}   // namespace meta

namespace graph {

struct LookupContext;

class LookupValidator final : public Validator {
public:
    LookupValidator(Sentence* sentence, QueryContext* context);

    AstContext* getAstContext() override;

private:
    Status validateImpl() override;

    Status prepareFrom();
    Status prepareYield();
    Status prepareFilter();

    StatusOr<Expression*> checkFilter(Expression* expr);
    Status checkRelExpr(RelationalExpression* expr);
    StatusOr<std::string> checkTSExpr(Expression* expr);
    StatusOr<Expression*> checkConstExpr(Expression* expr,
                                         ObjectPool* pool,
                                         const std::string& prop,
                                         const Expression::Kind kind);

    StatusOr<Expression*> rewriteRelExpr(RelationalExpression* expr);
    Expression* rewriteInExpr(const Expression* expr);
    Expression* reverseRelKind(RelationalExpression* expr);

    const LookupSentence* sentence() const;
    int32_t schemaId() const;
    GraphSpaceID spaceId() const;

private:
    Status getSchemaProvider(std::shared_ptr<const meta::NebulaSchemaProvider>* provider) const;
    StatusOr<Expression*> genTsFilter(Expression* filter);
    StatusOr<Expression*> handleLogicalExprOperands(LogicalExpression* lExpr);

    std::unique_ptr<LookupContext> lookupCtx_;
    std::vector<nebula::plugin::HttpClient> tsClients_;
};

}   // namespace graph
}   // namespace nebula

#endif   // _VALIDATOR_INDEXSCAN_VALIDATOR_H_
