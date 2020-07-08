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
    NG_RETURN_IF_ERROR(validateYieldAndBuildOutputs(yield->yield()));
    NG_RETURN_IF_ERROR(validateWhere(yield->where()));

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
    if (inputs_.empty() && !inputProps_.empty()) {
        return Status::SyntaxError("no inputs for yield columns.");
    }
    for (auto &prop : inputProps_) {
        DCHECK_NE(prop, "*");
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
        auto &props = vctx_->getVar(var);
        for (auto &prop : pair.second) {
            DCHECK_NE(prop, "*");
            auto iter = std::find_if(
                props.cbegin(), props.cend(), [&](auto &in) { return in.first == prop; });
            if (iter == props.cend()) {
                return Status::SyntaxError("column `%s' not exist in variable.", prop.c_str());
            }
        }
    }
    return Status::OK();
}

Status YieldValidator::validateYieldAndBuildOutputs(const YieldClause *clause) {
    auto columns = clause->columns();
    for (auto column : columns) {
        auto expr = column->expr();
        bool unfolded = false;
        switch (DCHECK_NOTNULL(expr)->kind()) {
            case Expression::Kind::kInputProperty: {
                auto ipe = static_cast<const InputPropertyExpression *>(expr);
                // Get all props of input expression could NOT be a part of another expression. So
                // it's always a root of expression.
                if (*ipe->prop() == "*") {
                    for (auto &colDef : inputs_) {
                        outputs_.emplace_back(colDef);
                        inputProps_.emplace_back(colDef.first);
                    }
                    unfolded = true;
                }
                break;
            }
            case Expression::Kind::kVarProperty: {
                auto vpe = static_cast<const VariablePropertyExpression *>(expr);
                // Get all props of variable expression is same as above input property expression.
                if (*vpe->prop() == "*") {
                    auto var = vpe->sym();
                    if (!vctx_->existVar(*var)) {
                        return Status::Error("variable `%s' not exists.", var->c_str());
                    }
                    auto &varColDefs = vctx_->getVar(*var);
                    auto &propsVec = varProps_[*var];
                    for (auto &colDef : varColDefs) {
                        outputs_.emplace_back(colDef);
                        propsVec.emplace_back(colDef.first);
                    }
                    unfolded = true;
                }
                break;
            }
            default:
                break;
        }

        if (!unfolded) {
            auto status = deduceExprType(expr);
            NG_RETURN_IF_ERROR(status);
            auto type = std::move(status).value();
            auto name = deduceColName(column);
            outputs_.emplace_back(name, type);
            NG_RETURN_IF_ERROR(deduceProps(expr));
        }

        auto fun = column->getFunName();
        if (!fun.empty()) {
            hasAggFun_ = true;
        }
    }
    return Status::OK();
}

Status YieldValidator::validateWhere(const WhereClause *clause) {
    Expression *filter = nullptr;
    if (clause != nullptr) {
        filter = clause->filter();
    }
    if (filter != nullptr) {
        NG_RETURN_IF_ERROR(deduceProps(filter));
    }
    return Status::OK();
}

YieldColumns *YieldValidator::getYieldColumns(YieldColumns *yieldColumns,
                                              ObjectPool *objPool,
                                              size_t numColumns) {
    auto oldColumns = yieldColumns->columns();
    DCHECK_LE(oldColumns.size(), numColumns);
    if (oldColumns.size() == numColumns) {
        return yieldColumns;
    }

    // There are some unfolded expressions, need to rebuild project expressions
    auto newColumns = objPool->add(new YieldColumns);
    for (auto &column : oldColumns) {
        auto expr = column->expr();
        bool unfolded = false;
        switch (expr->kind()) {
            case Expression::Kind::kInputProperty: {
                auto ipe = static_cast<const InputPropertyExpression *>(expr);
                if (*ipe->prop() == "*") {
                    for (auto &colDef : inputs_) {
                        newColumns->addColumn(new YieldColumn(
                            new InputPropertyExpression(new std::string(colDef.first))));
                    }
                    unfolded = true;
                }
                break;
            }
            case Expression::Kind::kVarProperty: {
                auto vpe = static_cast<const VariablePropertyExpression *>(expr);
                if (*vpe->prop() == "*") {
                    auto sym = vpe->sym();
                    CHECK(vctx_->existVar(*sym));
                    auto &varColDefs = vctx_->getVar(*sym);
                    for (auto &colDef : varColDefs) {
                        newColumns->addColumn(new YieldColumn(new VariablePropertyExpression(
                            new std::string(*sym), new std::string(colDef.first))));
                    }
                    unfolded = true;
                }
                break;
            }
            default:
                break;
        }
        if (!unfolded) {
            auto newExpr = Expression::decode(column->expr()->encode());
            std::string *alias = nullptr;
            if (column->alias() != nullptr) {
                alias = new std::string(*column->alias());
            }
            newColumns->addColumn(new YieldColumn(newExpr.release(), alias));
        }
    }
    return newColumns;
}

Status YieldValidator::toPlan() {
    auto yield = static_cast<const YieldSentence *>(sentence_);
    auto plan = qctx_->plan();

    std::vector<std::string> outputColumns(outputs_.size());
    std::transform(outputs_.cbegin(), outputs_.cend(), outputColumns.begin(), [](auto &colDef) {
        return colDef.first;
    });

    Filter *filter = nullptr;
    if (yield->where()) {
        filter = Filter::make(plan, nullptr, yield->where()->filter());
        filter->setColNames(outputColumns);
    }

    auto yieldColumns = getYieldColumns(yield->yieldColumns(), plan->objPool(), outputs_.size());
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
