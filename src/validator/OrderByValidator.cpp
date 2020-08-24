/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/OrderByValidator.h"
#include "parser/TraverseSentences.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {
GraphStatus OrderByValidator::validateImpl() {
    auto sentence = static_cast<OrderBySentence*>(sentence_);
    outputs_ = inputCols();
    auto factors = sentence->factors();
    GraphStatus gStatus;
    for (auto &factor : factors) {
        if (factor->expr()->kind() != Expression::Kind::kInputProperty) {
            return GraphStatus::setSemanticError(
                    folly::stringPrintf("Order by with invalid expression `%s'",
                                        factor->expr()->toString().c_str()));
        }
        auto expr = static_cast<InputPropertyExpression*>(factor->expr());
        auto *name = expr->prop();
        gStatus = checkPropNonexistOrDuplicate(outputs_, *name);
        if (!gStatus.ok()) {
            return gStatus;
        }
        colOrderTypes_.emplace_back(std::make_pair(*name, factor->orderType()));
    }

    return GraphStatus::OK();
}

GraphStatus OrderByValidator::toPlan() {
    auto* plan = qctx_->plan();
    auto *sortNode = Sort::make(qctx_, plan->root(), std::move(colOrderTypes_));
    std::vector<std::string> colNames;
    for (auto &col : outputs_) {
        colNames.emplace_back(col.first);
    }
    sortNode->setColNames(std::move(colNames));
    root_ = sortNode;
    tail_ = root_;
    return GraphStatus::OK();
}
}  // namespace graph
}  // namespace nebula
