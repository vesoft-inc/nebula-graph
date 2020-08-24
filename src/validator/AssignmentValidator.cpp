/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/AssignmentValidator.h"

#include "parser/TraverseSentences.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

GraphStatus AssignmentValidator::validateImpl() {
    auto* assignSentence = static_cast<AssignmentSentence*>(sentence_);
    validator_ = makeValidator(assignSentence->sentence(), qctx_);
    auto status = validator_->validate();
    if (!status.ok()) {
        return status;
    }

    auto outputs = validator_->outputCols();
    var_ = *assignSentence->var();
    vctx_->registerVariable(var_, std::move(outputs));
    return GraphStatus::OK();
}

GraphStatus AssignmentValidator::toPlan() {
    root_ = validator_->root();
    root_->setOutputVar(var_);
    tail_ = validator_->tail();
    return GraphStatus::OK();
}

}   // namespace graph
}   // namespace nebula
