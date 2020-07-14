/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/ASTValidator.h"

#include "parser/Sentence.h"
#include "planner/ExecutionPlan.h"
#include "validator/Validator.h"

namespace nebula {
namespace graph {

Status ASTValidator::validate() {
    // Check if space chosen from session. if chosen, add it to context.
    auto session = qctx_->rctx()->session();
    if (session->space() > -1) {
        qctx_->vctx()->switchToSpace(session->spaceName(), session->space());
    }

    validator_ = Validator::makeValidator(sentences_, qctx_);
    auto status = validator_->validate();
    if (!status.ok()) {
        return status;
    }

    auto root = validator_->root();
    if (!root) {
        return Status::Error("Get null plan from sequantial validator.");
    }

    qctx_->plan()->setRoot(root);
    return Status::OK();
}

}  // namespace graph
}  // namespace nebula
