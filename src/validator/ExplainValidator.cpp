/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/ExplainValidator.h"

#include <algorithm>

#include "common/base/StatusOr.h"
#include "common/interface/gen-cpp2/graph_types.h"
#include "parser/ExplainSentence.h"
#include "planner/PlanNode.h"
#include "validator/SequentialValidator.h"

namespace nebula {
namespace graph {

using ExplainFmtType = cpp2::PlanFormat;

ExplainValidator::ExplainValidator(Sentence* sentence, QueryContext* context)
    : Validator(sentence, context) {
    DCHECK_EQ(sentence->kind(), Sentence::Kind::kExplain);
}

static StatusOr<ExplainFmtType> toExplainFormatType(const std::string& formatType) {
    if (formatType.empty()) {
        return ExplainFmtType::ROW;
    }

    std::string fmtType = formatType;
    std::transform(formatType.cbegin(), formatType.cend(), fmtType.begin(), [](char c) {
        return std::tolower(c);
    });

    if (fmtType == "row") {
        return ExplainFmtType::ROW;
    }

    if (fmtType == "dot") {
        return ExplainFmtType::DOT;
    }

    return Status::SyntaxError(
        "Invalid explain/profile format type: \"%s\", only `row' and `dot' values supported",
        formatType.c_str());
}

Status ExplainValidator::validateImpl() {
    auto explain = static_cast<ExplainSentence*>(sentence_);

    auto status = toExplainFormatType(explain->formatType());
    NG_RETURN_IF_ERROR(status);
    auto planDesc = std::make_unique<cpp2::PlanDescription>();
    planDesc->set_format(std::move(status).value());
    qctx_->setPlanDescription(std::move(planDesc));

    auto sentences = explain->seqSentences();
    validator_ = std::make_unique<SequentialValidator>(sentences, qctx_);
    return validator_->validate();
}

Status ExplainValidator::toPlan() {
    return validator_->toPlan();
}

}   // namespace graph
}   // namespace nebula
