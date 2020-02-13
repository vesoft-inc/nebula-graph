/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/SetValidator.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {
Status SetValidator::validateImpl() {
    auto setSentence = static_cast<SetSentence*>(sentence_);
    lValidator_ = Validator::makeValidator(setSentence->left(), validateContext_);
    auto status = lValidator_->validate();
    if (!status.ok()) {
        return status;
    }
    rValidator_ = Validator::makeValidator(setSentence->right(), validateContext_);
    status = rValidator_->validate();
    if (!status.ok()) {
        return status;
    }
    return Status::OK();
}

Status SetValidator::toPlan() {
    start_ = lValidator_->start();
    start_->merge(rValidator_->start());

    switch (op_) {
        case SetSentence::Operator::UNION:
            end_ = std::make_shared<Union>(distinct_);
            break;
        case SetSentence::Operator::INTERSECT:
            end_ = std::make_shared<Intersect>();
            break;
        case SetSentence::Operator::MINUS:
            end_ = std::make_shared<Minus>();
            break;
        default:
            return Status::Error("Unkown operator: %ld", static_cast<int64_t>(op_));
    }

    lValidator_->end()->append(end_);
    rValidator_->end()->append(end_);
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
