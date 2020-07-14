/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "validator/AdminJobValidator.h"
#include "planner/Admin.h"

namespace nebula {
namespace graph {

Status AdminJobValidator::validateImpl() {
    return Status::OK();
}

Status AdminJobValidator::toPlan() {
    auto* plan = qctx_->plan();
    auto *doNode = SubmitJob::make(plan, nullptr, sentence_->getType(), sentence_->moveParas());
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

}  // namespace graph
}  // namespace nebula
