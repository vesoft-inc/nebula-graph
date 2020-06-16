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
Status OrderByValidator::validateImpl() {
    auto sentence = static_cast<OrderBySentence*>(sentence_);
    factors_ = sentence->factors();

    return Status::OK();
}

Status OrderByValidator::toPlan() {
    LOG(INFO) << "OrderByValidator::toPlan";
    std::vector<std::pair<std::string, OrderFactor::OrderType>> nameType;
    for (auto &factor : factors_) {
        if (factor->expr()->kind() != Expression::Kind::kInputProperty) {
            return Status::Error("Wrong expression");
        }
        auto expr = static_cast<InputPropertyExpression*>(factor->expr());
        nameType.emplace_back(std::make_pair(*expr->sym(), factor->orderType()));
    }

    auto* plan = qctx_->plan();
    auto *sortNode = Sort::make(plan, plan->root(), std::move(nameType));
    sortNode->setInputVar(plan->root()->varName());
    root_ = sortNode;
    tail_ = root_;
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
