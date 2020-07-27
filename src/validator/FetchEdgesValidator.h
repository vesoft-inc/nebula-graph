/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/base/Base.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "parser/TraverseSentences.h"
#include "validator/Validator.h"

namespace nebula {
namespace graph {

class FetchEdgesValidator final : public Validator {
public:
    FetchEdgesValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {}

private:
    Status validateImpl() override;

    Status toPlan() override;

    Status check();

    Status prepareEdges();

    Status prepareProperties();

    static const Expression* findInvalidYieldExpression(const Expression* root);

    static const std::unordered_set<std::string> reservedProperties;

private:
    GraphSpaceID spaceId_;
    std::vector<nebula::Row> edges_;
    Expression* src_{nullptr};
    Expression* ranking_{nullptr};
    Expression* dst_{nullptr};
    std::string edgeTypeName_;
    EdgeType edgeType_{0};
    std::shared_ptr<const meta::SchemaProviderIf> schema_;
    std::vector<storage::cpp2::EdgeProp> props_;
    std::vector<storage::cpp2::Expr>     exprs_;
    bool dedup_{false};
    int64_t limit_{std::numeric_limits<int64_t>::max()};
    std::vector<storage::cpp2::OrderBy> orderBy_{};
    std::string filter_{""};
    // valid when yield expression not require storage
    // So expression like these will be evaluate in Project Executor
    bool withProject_{false};
    // outputs
    std::vector<std::string> colNames_;
    // new yield to inject reserved properties for compatible with 1.0
    YieldClause* newYield_{nullptr};
    // input
    std::string inputVar_;
};

}   // namespace graph
}   // namespace nebula
