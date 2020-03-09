/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "SequentialValidator.h"

namespace nebula {
namespace graph {
Status SequentialValidator::validateImpl() {
    Status status;
    if (sentence_->kind() != Sentence::Kind::kSequential) {
        return Status::Error(
                "Sequential validator validates a SequentialSentences, but %ld is given.",
                static_cast<int64_t>(sentence_->kind()));
    }
    auto seqSentence = static_cast<SequentialSentences*>(sentence_);
    auto sentences = seqSentence->sentences();
    for (auto* sentence : sentences) {
        auto validator = Validator::makeValidator(sentence, validateContext_);
        status = validator->validate();
        if (!status.ok()) {
            return status;
        }
        validators_.emplace_back(std::move(validator));
    }

    return Status::OK();
}

Status SequentialValidator::toPlan() {
    start_ = validators_[validators_.size() - 1]->start();
    for (decltype(validators_.size()) i = 0; i < (validators_.size() - 1); ++i) {
        auto status = PlanNode::append(validators_[i + 1]->end(), validators_[i]->start());
        if (!status.ok()) {
            return status;
        }
    }
    end_ = validators_[0]->end();
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
