/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/YieldValidator.h"

#include "common/expression/Expression.h"
#include "context/QueryContext.h"
#include "parser/Clauses.h"
#include "parser/TraverseSentences.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

YieldValidator::YieldValidator(Sentence *sentence, QueryContext *qctx)
    : Validator(sentence, qctx) {}

Status YieldValidator::validateImpl() {
    auto yield = static_cast<YieldSentence *>(sentence_);
    NG_RETURN_IF_ERROR(validate(yield->yield()));
    NG_RETURN_IF_ERROR(validate(yield->where()));

    if (!srcTagProps_.empty() || !dstTagProps_.empty() || !edgeProps_.empty()) {
        return Status::SyntaxError("Only support input and variable in yield sentence.");
    }

    if (!inputProps_.empty() && !varProps_.empty()) {
        return Status::Error("Not support both input and variable.");
    }

    if (!varProps_.empty() && varProps_.size() > 1) {
        return Status::Error("Only one variable allowed to use.");
    }

    NG_RETURN_IF_ERROR(checkInputProps());
    NG_RETURN_IF_ERROR(checkVarProps());

    if (hasAggFun_) {
        NG_RETURN_IF_ERROR(checkColumnRefAggFun(yield->yield()));
    }

    return Status::OK();
}

Status YieldValidator::checkColumnRefAggFun(const YieldClause *clause) const {
    auto yield = clause->yields();
    for (auto column : yield->columns()) {
        auto expr = column->expr();
        auto fun = column->getFunName();
        if ((expr->kind() == Expression::Kind::kInputProperty ||
             expr->kind() == Expression::Kind::kVarProperty) &&
            fun.empty()) {
            return Status::SyntaxError(
                "Input columns without aggregation are not supported in YIELD statement "
                "without GROUP BY, near `%s'",
                expr->toString().c_str());
        }
    }
    return Status::OK();
}

Status YieldValidator::checkInputProps() const {
    for (auto &prop : inputProps_) {
        if (prop == "*") {
            if (!inputs_.empty()) continue;
            return Status::SyntaxError("no inputs for `$-.*'");
        }
        auto iter = std::find_if(
            inputs_.cbegin(), inputs_.cend(), [&](auto &in) { return prop == in.first; });
        if (iter == inputs_.cend()) {
            return Status::SyntaxError("column `%s' not exist in input", prop.c_str());
        }
    }
    return Status::OK();
}

Status YieldValidator::checkVarProps() const {
    for (auto &pair : varProps_) {
        auto &var = pair.first;
        if (!vctx_->existVar(var)) {
            return Status::SyntaxError("variable `%s' not exist.", var.c_str());
        }
        auto props = vctx_->getVar(var);
        for (auto &prop : pair.second) {
            if (prop == "*") continue;
            auto iter = std::find_if(
                props.cbegin(), props.cend(), [&](auto &in) { return in.first == prop; });
            if (iter == props.cend()) {
                return Status::SyntaxError("column `%s' not exist in variable.", prop.c_str());
            }
        }
    }
    return Status::OK();
}

Status YieldValidator::validate(const YieldClause *clause) {
    auto columns = clause->columns();
    for (auto column : columns) {
        auto expr = column->expr();
        NG_RETURN_IF_ERROR(deduceProps(expr));

        auto fun = column->getFunName();
        if (!fun.empty()) {
            hasAggFun_ = true;
        }
    }
    return Status::OK();
}

Status YieldValidator::validate(const WhereClause *clause) {
    Expression *filter = nullptr;
    if (clause != nullptr) {
        filter = clause->filter();
    }
    if (filter != nullptr) {
        NG_RETURN_IF_ERROR(deduceProps(filter));
    }
    return Status::OK();
}

Status YieldValidator::toPlan() {
    auto yield = static_cast<const YieldSentence *>(sentence_);
    auto plan = qctx_->plan();
    auto yieldColumns = yield->yield()->yields();
    auto outputColumns = deduceColNames(yieldColumns);

    Filter *filter = nullptr;
    if (yield->where()) {
        filter = Filter::make(plan, nullptr, yield->where()->filter());
        filter->setColNames(outputColumns);
    }

    auto project = Project::make(plan, filter, yieldColumns);
    project->setColNames(outputColumns);
    if (filter != nullptr) {
        project->setInputVar(filter->varName());
        tail_ = filter;
    } else {
        tail_ = project;
    }

    if (yield->yield()->isDistinct()) {
        auto dedup = Dedup::make(plan, project);
        dedup->setColNames(outputColumns);
        dedup->setInputVar(project->varName());
        root_ = dedup;
    } else {
        root_ = project;
    }

    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
