/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/FindPathValidator.h"
#include "planner/Logic.h"

namespace nebula {
namespace graph {
GraphStatus FindPathValidator::validateImpl() {
    auto fpSentence = static_cast<FindPathSentence*>(sentence_);
    isShortest_ = fpSentence->isShortest();

    auto gStatus = validateStarts(fpSentence->from(), from_);
    if (!gStatus.ok()) {
        return gStatus;
    }
    gStatus = validateStarts(fpSentence->to(), to_);
    if (!gStatus.ok()) {
        return gStatus;
    }
    gStatus = validateOver(fpSentence->over(), over_);
    if (!gStatus.ok()) {
        return gStatus;
    }
    gStatus = validateStep(fpSentence->step(), steps_);
    if (!gStatus.ok()) {
        return gStatus;
    }
    return GraphStatus::OK();
}

GraphStatus FindPathValidator::toPlan() {
    // TODO: Implement the path plan.
    auto* passThrough = PassThroughNode::make(qctx_, nullptr);
    tail_ = passThrough;
    root_ = tail_;
    return GraphStatus::OK();
}
}  // namespace graph
}  // namespace nebula
