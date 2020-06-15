/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "validator/ReOrganizeValidator.h"
#include "parser/TraverseSentences.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

Status OrderByValidator::validateImpl() {
    auto *orderBySentence = static_cast<OrderBySentence*>(sentence_);
    orderFactors_ = orderBySentence->moveFactors();
    return Status::OK();
}

Status OrderByValidator::toPlan() {
    auto* plan = qctx_->plan();
    auto *doNode = Sort::make(plan, plan->root(), std::move(orderFactors_));
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

}  // namespace graph
}  // namespace nebula
