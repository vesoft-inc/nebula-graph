/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "base/Base.h"
#include "interface/gen-cpp2/storage_types.h"
#include "parser/TraverseSentences.h"
#include "validator/Validator.h"

namespace nebula {
namespace graph {

class FetchEdgesValidator final : public Validator {
public:
    FetchEdgesValidator(Sentence* sentence, ValidateContext* context)
        : Validator(sentence, context) {
        sentence_ = static_cast<FetchEdgesSentence*>(sentence);
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

    Status check();

    Status prepareEdges();

    Status prepareProperties();

private:
    FetchEdgesSentence* sentence_{nullptr};
    GraphSpaceID spaceId_;
    std::vector<nebula::Row> edges_;
    Expression* src_{nullptr};
    Expression* ranking_{nullptr};
    Expression* dst_{nullptr};
    std::string edgeTypeName_;
    EdgeType edgeType_{0};
    std::shared_ptr<const meta::SchemaProviderIf> schema_;
    std::vector<storage::cpp2::PropExp> props_;
    bool dedup_{false};
    int64_t limit_{std::numeric_limits<int64_t>::max()};
    std::vector<storage::cpp2::OrderBy> orderBy_{};
    std::string filter_{""};
};

}   // namespace graph
}   // namespace nebula
