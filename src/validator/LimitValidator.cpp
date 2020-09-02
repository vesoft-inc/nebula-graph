/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/LimitValidator.h"
#include "parser/TraverseSentences.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {
GraphStatus LimitValidator::validateImpl() {
    auto limitSentence = static_cast<LimitSentence*>(sentence_);
    offset_ = limitSentence->offset();
    count_ = limitSentence->count();
    if (offset_ < 0) {
        return GraphStatus::setSyntaxError(
                folly::stringPrintf("skip `%ld' is illegal", offset_));
    }
    if (count_ < 0) {
        return GraphStatus::setSyntaxError(
                folly::stringPrintf("count `%ld' is illegal", count_));
    }

    outputs_ = inputCols();
    return GraphStatus::OK();
}

GraphStatus LimitValidator::toPlan() {
    auto* plan = qctx_->plan();
    auto *limitNode = Limit::make(qctx_, plan->root(), offset_, count_);
    std::vector<std::string> colNames;
    for (auto &col : outputs_) {
        colNames.emplace_back(col.first);
    }
    limitNode->setColNames(std::move(colNames));
    root_ = limitNode;
    tail_ = root_;
    return GraphStatus::OK();
}
}  // namespace graph
}  // namespace nebula
