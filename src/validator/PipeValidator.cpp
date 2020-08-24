/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "validator/PipeValidator.h"
#include "parser/TraverseSentences.h"
#include "planner/PlanNode.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

GraphStatus PipeValidator::validateImpl() {
    auto pipeSentence = static_cast<PipedSentence*>(sentence_);
    auto left = pipeSentence->left();
    lValidator_ = makeValidator(left, qctx_);
    lValidator_->setInputCols(std::move(inputs_));
    lValidator_->setInputVarName(inputVarName_);
    auto gStatus = lValidator_->validate();
    if (!gStatus.ok()) {
        return gStatus;
    }

    auto right = pipeSentence->right();
    rValidator_ = makeValidator(right, qctx_);
    rValidator_->setInputCols(lValidator_->outputCols());
    rValidator_->setInputVarName(lValidator_->root()->varName());
    gStatus = rValidator_->validate();
    if (!gStatus.ok()) {
        return gStatus;
    }

    outputs_ = rValidator_->outputCols();
    return GraphStatus::OK();
}

GraphStatus PipeValidator::toPlan() {
    root_ = rValidator_->root();
    tail_ = lValidator_->tail();
    auto gStatus = rValidator_->appendPlan(lValidator_->root());
    if (!gStatus.ok()) {
        return gStatus;
    }
    auto node = static_cast<SingleInputNode*>(rValidator_->tail());
    if (node->inputVar().empty()) {
        // If the input variable was not set, set it dynamically.
        node->setInputVar(lValidator_->root()->varName());
    }
    return GraphStatus::OK();
}

}  // namespace graph
}  // namespace nebula
