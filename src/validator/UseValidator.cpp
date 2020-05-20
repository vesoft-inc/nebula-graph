/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/UseValidator.h"
#include "parser/TraverseSentences.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {
Status UseValidator::validateImpl() {
    auto useSentence = static_cast<UseSentence*>(sentence_);
    auto* spaceName = useSentence->space();
    auto ret = validateContext_->schemaMng()->toGraphSpaceID(*spaceName);
    if (!ret.ok()) {
        LOG(ERROR) << "Unkown space: " << *spaceName;
        return ret.status();
    }

    validateContext_->switchToSpace(*spaceName, ret.value());
    return Status::OK();
}

Status UseValidator::toPlan() {
    auto space = validateContext_->whichSpace();
    // The input will be set by father validator later.
    auto plan = validateContext_->plan();
    auto *start = StartNode::make(plan);
    auto reg = SwitchSpaceNode::make(plan, start, space.name, space.id);
    root_ = reg;
    tail_ = root_;
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
