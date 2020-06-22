/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/LimitValidator.h"

#include "parser/TraverseSentences.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

LimitValidator::LimitValidator(Sentence *sentence, QueryContext *qctx)
    : Validator(sentence, qctx) {}

Status LimitValidator::validateImpl() {
    auto limit = static_cast<LimitSentence *>(sentence_);

    auto offset = limit->offset();
    if (offset < 0) {
        return Status::SyntaxError("skip `%ld' is illegal", offset);
    }
    auto count = limit->count();
    if (count < 0) {
        return Status::SyntaxError("count `%ld' is illegal", count);
    }

    return Status::OK();
}

Status LimitValidator::toPlan() {
    auto limit = static_cast<LimitSentence *>(sentence_);

    root_ = Limit::make(qctx_->plan(), nullptr, limit->offset(), limit->count());
    tail_ = root_;

    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
