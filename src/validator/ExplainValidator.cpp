/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/ExplainValidator.h"

#include <algorithm>

#include "common/base/StatusOr.h"
#include "parser/ExplainSentence.h"
#include "planner/PlanNode.h"
#include "validator/SequentialValidator.h"

namespace nebula {
namespace graph {

using QStmtType = QueryContext::StmtType;
using QExplainFmtType = QueryContext::ExplainFormatType;

ExplainValidator::ExplainValidator(Sentence* sentence, QueryContext* context)
    : Validator(sentence, context) {
    DCHECK_EQ(sentence->kind(), Sentence::Kind::kExplain);
}

static StatusOr<QExplainFmtType> toExplainFormatType(const std::string& formatType) {
    if (formatType.empty()) {
        return QExplainFmtType::kRow;
    }

    std::string fmtType = formatType;
    std::transform(formatType.cbegin(), formatType.cend(), fmtType.begin(), [](char c) {
        return std::tolower(c);
    });

    if (fmtType == "row") {
        return QExplainFmtType::kRow;
    }

    if (fmtType == "dot") {
        return QExplainFmtType::kDot;
    }

    return Status::Error(
        "Invalid explain/profile format type: \"%s\", only `row' and `dot' values supported",
        formatType.c_str());
}

Status ExplainValidator::validateImpl() {
    auto explain = static_cast<ExplainSentence*>(sentence_);

    auto stmtType = explain->isProfile() ? QStmtType::kProfile : QStmtType::kExplain;
    qctx_->setStmtType(stmtType);

    auto status = toExplainFormatType(explain->formatType());
    NG_RETURN_IF_ERROR(status);
    qctx_->setExplainFormatType(std::move(status).value());

    auto sentences = explain->seqSentences();
    validator_ = std::make_unique<SequentialValidator>(sentences, qctx_);
    return validator_->validate();
}

Status ExplainValidator::toPlan() {
    return validator_->toPlan();
}

}   // namespace graph
}   // namespace nebula
