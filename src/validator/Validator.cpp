/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/Validator.h"
#include "parser/Sentence.h"
#include "ReportError.h"
#include "PipeValidator.h"
#include "GoValidator.h"

namespace nebula {
namespace graph {
std::unique_ptr<Validator> makeValidator(Sentence* sentence) {
    CHECK(!!sentence);
    auto kind = sentence->kind();
    switch (kind) {
        case Sentence::Kind::kGo:
            return std::make_unique<GoValidator>(sentence);
        case Sentence::Kind::kPipe:
            return std::make_unique<PipeValidator>(sentence);
        default:
            return std::make_unique<ReportError>(sentence);
    }
}

Status Validator::validate() {
    auto status = validateImpl();
    if (!status.ok()) {
        return status;
    }

    status = toPlan();
    if (!status.ok()) {
        return status;
    }

    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
