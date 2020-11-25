/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef _VALIDATOR_INDEXSCAN_VALIDATOR_H_
#define _VALIDATOR_INDEXSCAN_VALIDATOR_H_

#include <planner/Query.h>
#include "common/base/Base.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "parser/TraverseSentences.h"
#include "validator/Validator.h"

namespace nebula {
namespace graph {

class IndexScanValidator final : public Validator {
public:
    IndexScanValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {}

private:
    Status validateImpl() override;

    Status toPlan() override;

    Status prepareTSService();

    Status prepareFrom();

    Status prepareYield();

    Status prepareFilter();

    StatusOr<std::unique_ptr<Expression>> rewriteTSFilter(Expression* expr);

    std::unique_ptr<Expression> genOrFilterFromList(
        TextSearchExpression* expr, const std::vector<std::string>& values);

    StatusOr<std::vector<std::string>> textSearch(TextSearchExpression* expr);

    bool needTextSearch(Expression* expr);

    Status checkFilter(Expression* expr, const std::string& from);

    Status checkRelExpr(RelationalExpression* expr, const std::string& from);

    Status rewriteRelExpr(RelationalExpression* expr, const std::string& from);

    StatusOr<Value> checkConstExpr(Expression* expr, const std::string& prop);

private:
    GraphSpaceID                      spaceId_{0};
    IndexScan::IndexQueryCtx          contexts_{};
    IndexScan::IndexReturnCols        returnCols_{};
    bool                              isEdge_{false};
    int32_t                           schemaId_;
    bool                              isEmptyResultSet_;
    bool                              textSearchReady{false};
    std::vector<meta::cpp2::FTClient> tsClients_;
};

}   // namespace graph
}   // namespace nebula


#endif   // _VALIDATOR_INDEXSCAN_VALIDATOR_H_
