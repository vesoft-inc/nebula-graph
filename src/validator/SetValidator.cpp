/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/SetValidator.h"

namespace nebula {
namespace graph {
Status SetValidator::validateImpl() {
    auto setSentence = static_cast<SetSentence*>(sentence_);
    lValidator_ = Validator::makeValidator(setSentence->left());
    auto status = lValidator_->validate();
    if (!status.ok()) {
        return status;
    }
    rValidator_ = Validator::makeValidator(setSentence->right());
    status = rValidator_->validate();
    if (!status.ok()) {
        return status;
    }
    return Status::OK();
}

Status SetValidator::toPlan() {
    auto leftStart = lValidator_->start();
    auto rightStart = rValidator_->start();
    start_ = std::make_shared<StartNode>();
    start_->merge(leftStart);
    start_->merge(rightStart);

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
