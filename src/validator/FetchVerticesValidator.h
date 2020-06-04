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

class FetchVerticesValidator final : public Validator {
public:
    FetchVerticesValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
        sentence_ = static_cast<FetchVerticesSentence*>(sentence);
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

    Status check();

    Status prepareVertices();

    Status prepareProperties();

private:
    FetchVerticesSentence* sentence_{nullptr};
    GraphSpaceID spaceId_{0};
    std::vector<nebula::Row> vertices_;
    Expression* src_{nullptr};
    std::string tagName_;
    // none if not specified tag
    folly::Optional<TagID> tagId_;
    std::shared_ptr<const meta::SchemaProviderIf> schema_;
    std::vector<storage::cpp2::PropExp> props_;
    bool dedup_{false};
    std::vector<storage::cpp2::OrderBy> orderBy_{};
    int64_t limit_{std::numeric_limits<int64_t>::max()};
    std::string filter_{};
};

}   // namespace graph
}   // namespace nebula
