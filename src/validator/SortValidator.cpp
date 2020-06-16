/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/SortValidator.h"
#include "parser/TraverseSentences.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {
Status SortValidator::validateImpl() {
    auto sentence = static_cast<OrderBySentence*>(sentence_);
    factors_ = sentence->factors();

    return Status::OK();
}

Status SortValidator::toPlan() {
    auto* plan = qctx_->plan();
    auto *start = StartNode::make(plan);

    std::vector<std::pair<std::string, OrderFactor::OrderType>> nameType;
    auto* columns = new YieldColumns();
    for (auto &factor : factors_) {
        if (factor->expr()->kind() != Expression::Kind::kInputProperty) {
            return Status::Error("Wrong expression");
        }
        auto expr = static_cast<InputPropertyExpression*>(factor->expr());
        nameType.emplace_back(std::make_pair(*expr->sym(), factor->orderType()));
        auto* column = new YieldColumn(expr);
        columns->addColumn(column);
    }

    auto projectName = vctx_->varGen()->getVar();
    auto* project = Project::make(plan, start, plan->saveObject(columns));
    project->setOutputVar(projectName);
    project->setColNames(evalResultColNames(columns));

    auto *sortNode = Sort::make(plan, project, std::move(nameType));
    sortNode->setInputVar(projectName);
    root_ = sortNode;
    tail_ = root_;
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
