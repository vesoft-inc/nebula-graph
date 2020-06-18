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
    auto* plan = qctx_->plan();
    if (plan->root() == nullptr) {
        return Status::Error("No input node");
    }
    auto inputColNames = plan->root()->colNames();
    std::vector<std::pair<std::string, OrderFactor::OrderType>> nameType;
    for (auto &factor : factors_) {
        if (factor->expr()->kind() != Expression::Kind::kInputProperty) {
            return Status::Error("Wrong expression");
        }
        auto expr = static_cast<InputPropertyExpression*>(factor->expr());
        auto name = *expr->prop();
        nameType.emplace_back(std::make_pair(name, factor->orderType()));
        // Check factor in input node's colNames
        auto find = std::find_if(inputColNames.begin(), inputColNames.end(),
                [&name] (const auto& colName) {
                    return colName == name;
                });
        if (find == inputColNames.end()) {
            LOG(ERROR) << "Order BY on factor `" << name <<"` is not exist";
            return Status::Error("Order BY on factor `%s` is not exist", name.c_str());
        }
    }

    auto *sortNode = Sort::make(plan, plan->root(), std::move(nameType));
    auto* project = Project::make(plan, sortNode, {});
    root_ = project;
    tail_ = sortNode;
    plan->setRoot(root_);
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
