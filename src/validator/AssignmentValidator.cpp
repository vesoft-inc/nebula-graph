/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/AssignmentValidator.h"
#include "parser/TraverseSentences.h"

namespace nebula {
namespace graph {
Status AssignmentValidator::validateImpl() {
    auto* assignSentence = static_cast<AssignmentSentence*>(sentence_);
    validator_ = Validator::makeValidator(assignSentence->sentence(), validateContext_);
    auto status = validator_->validate();
    if (!status.ok()) {
        return status;
    }
    validateContext_->registerVariable(*assignSentence->var());
    return Status::OK();
}

Status AssignmentValidator::toPlan() {
    start_ = validator_->start();
    // TODO: need a node to register variable in execution phase.
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
