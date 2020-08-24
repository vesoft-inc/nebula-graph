/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/ExplainValidator.h"

#include <algorithm>

#include <folly/String.h>

#include "common/base/StatusOr.h"
#include "common/interface/gen-cpp2/graph_types.h"
#include "parser/ExplainSentence.h"
#include "planner/PlanNode.h"
#include "validator/SequentialValidator.h"

namespace nebula {
namespace graph {

static const std::vector<std::string> kAllowedFmtType = {"row", "dot", "dot:struct"};

ExplainValidator::ExplainValidator(Sentence* sentence, QueryContext* context)
    : Validator(sentence, context) {
    DCHECK_EQ(sentence->kind(), Sentence::Kind::kExplain);
}

static StatusOr<std::string> toExplainFormatType(const std::string& formatType) {
    if (formatType.empty()) {
        return kAllowedFmtType.front();
    }

    std::string fmtType = formatType;
    std::transform(formatType.cbegin(), formatType.cend(), fmtType.begin(), [](char c) {
        return std::tolower(c);
    });

    auto found = std::find(kAllowedFmtType.cbegin(), kAllowedFmtType.cend(), fmtType);
    if (found != kAllowedFmtType.cend()) {
        return fmtType;
    }
    auto allowedStr = folly::join(",", kAllowedFmtType);
    return Status::Error(
        "Invalid explain/profile format type: \"%s\", only following values are supported: %s",
        fmtType.c_str(),
        allowedStr.c_str());
}

GraphStatus ExplainValidator::validateImpl() {
    auto explain = static_cast<ExplainSentence*>(sentence_);

    auto status = toExplainFormatType(explain->formatType());
    if (!status.ok()) {
        return GraphStatus::setInvalidParam(explain->formatType());
    }
    auto planDesc = std::make_unique<cpp2::PlanDescription>();
    planDesc->set_format(std::move(status).value());
    qctx_->setPlanDescription(std::move(planDesc));

    auto sentences = explain->seqSentences();
    validator_ = std::make_unique<SequentialValidator>(sentences, qctx_);
    auto gStatus = validator_->validate();
    if (!gStatus.ok()) {
        return gStatus;
    }

    outputs_ = validator_->outputCols();
    return GraphStatus::OK();
}

GraphStatus ExplainValidator::toPlan() {
    auto gStatus = validator_->toPlan();
    if (!gStatus.ok()) {
        return gStatus;
    }
    root_ = validator_->root();
    tail_ = validator_->tail();
    return GraphStatus::OK();
}

}   // namespace graph
}   // namespace nebula
