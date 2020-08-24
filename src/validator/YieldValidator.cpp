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

GraphStatus YieldValidator::validateImpl() {
    auto yield = static_cast<YieldSentence *>(sentence_);
    auto gStatus = validateYieldAndBuildOutputs(yield->yield());
    if (!gStatus.ok()) {
        return gStatus;
    }

    gStatus = validateWhere(yield->where());
    if (!gStatus.ok()) {
        return gStatus;
    }

    if (!exprProps_.srcTagProps().empty() || !exprProps_.dstTagProps().empty() ||
        !exprProps_.edgeProps().empty()) {
        return GraphStatus::setSemanticError("Only support input and variable in yield sentence");
    }

    if (!exprProps_.inputProps().empty() && !exprProps_.varProps().empty()) {
        return GraphStatus::setSemanticError("Not support both input and variable");
    }

    if (!exprProps_.varProps().empty() && exprProps_.varProps().size() > 1) {
        return GraphStatus::setSemanticError("Only one variable allowed to use");
    }

    // TODO(yee): following check maybe not make sense
    gStatus = checkInputProps();
    if (!gStatus.ok()) {
        return gStatus;
    }

    gStatus = checkVarProps();
    if (!gStatus.ok()) {
        return gStatus;
    }

    if (hasAggFun_) {
        gStatus = checkAggFunAndBuildGroupItems(yield->yield());
        return gStatus;
    }

    return GraphStatus::OK();
}

GraphStatus YieldValidator::checkAggFunAndBuildGroupItems(const YieldClause *clause) {
    auto yield = clause->yields();
    for (auto column : yield->columns()) {
        auto expr = column->expr();
        auto fun = column->getAggFunName();
        if (!evaluableExpr(expr) && fun.empty()) {
            return GraphStatus::setInvalidExpr(expr->toString());
        }

        groupItems_.emplace_back(Aggregate::GroupItem{expr, AggFun::nameIdMap_[fun], false});
    }
    return GraphStatus::OK();
}

GraphStatus YieldValidator::checkInputProps() const {
    auto& inputProps = const_cast<ExpressionProps*>(&exprProps_)->inputProps();
    if (inputs_.empty() && !inputProps.empty()) {
        return GraphStatus::setSemanticError("no inputs for yield columns.");
    }
    GraphStatus status;
    for (auto &prop : inputProps) {
        DCHECK_NE(prop, "*");
        status = checkPropNonexistOrDuplicate(inputs_, prop);
        if (!status.ok()) {
            return status;
        }
    }
    return GraphStatus::OK();
}

GraphStatus YieldValidator::checkVarProps() const {
    auto& varProps = const_cast<ExpressionProps*>(&exprProps_)->varProps();
    for (auto &pair : varProps) {
        auto &var = pair.first;
        if (!vctx_->existVar(var)) {
            return GraphStatus::setSemanticError(
                    folly::stringPrintf("variable `%s' not exist.", var.c_str()));
        }
        auto &props = vctx_->getVar(var);
        GraphStatus status;
        for (auto &prop : pair.second) {
            DCHECK_NE(prop, "*");
            status = checkPropNonexistOrDuplicate(props, prop);
            if (!status.ok()) {
                return status;
            }
        }
    }
    return GraphStatus::OK();
}

GraphStatus YieldValidator::makeOutputColumn(YieldColumn *column) {
    columns_->addColumn(column);

    auto expr = column->expr();
    DCHECK(expr != nullptr);
    auto status = deduceProps(expr, exprProps_);
    if (!status.ok()) {
        return GraphStatus::setUnsupportedExpr(expr->toString());
    }

    auto typeResult = deduceExprType(expr);
    if (!typeResult.ok()) {
        return GraphStatus::setInvalidExpr(expr->toString());
    }
    auto type = std::move(typeResult).value();

    auto name = deduceColName(column);
    outputColumnNames_.emplace_back(name);

    outputs_.emplace_back(name, type);
    return GraphStatus::OK();
}

GraphStatus YieldValidator::validateYieldAndBuildOutputs(const YieldClause *clause) {
    auto columns = clause->columns();
    columns_ = qctx_->objPool()->add(new YieldColumns);
    GraphStatus gStatus;
    for (auto column : columns) {
        auto expr = DCHECK_NOTNULL(column->expr());
        if (expr->kind() == Expression::Kind::kInputProperty) {
            auto ipe = static_cast<const InputPropertyExpression *>(expr);
            // Get all props of input expression could NOT be a part of another expression. So
            // it's always a root of expression.
            if (*ipe->prop() == "*") {
                for (auto &colDef : inputs_) {
                    auto newExpr = new InputPropertyExpression(new std::string(colDef.first));
                    gStatus = makeOutputColumn(new YieldColumn(newExpr));
                    if (!gStatus.ok()) {
                        return gStatus;
                    }
                }
                if (!column->getAggFunName().empty()) {
                    return GraphStatus::setSemanticError(
                            "could not apply aggregation function on `$-.*'");
                }
                continue;
            }
        } else if (expr->kind() == Expression::Kind::kVarProperty) {
            auto vpe = static_cast<const VariablePropertyExpression *>(expr);
            // Get all props of variable expression is same as above input property expression.
            if (*vpe->prop() == "*") {
                auto var = DCHECK_NOTNULL(vpe->sym());
                if (!vctx_->existVar(*var)) {
                    return GraphStatus::setSemanticError(
                            folly::stringPrintf("variable `%s' not exists.", var->c_str()));
                }
                auto &varColDefs = vctx_->getVar(*var);
                for (auto &colDef : varColDefs) {
                    auto newExpr = new VariablePropertyExpression(new std::string(*var),
                                                                  new std::string(colDef.first));
                    gStatus = makeOutputColumn(new YieldColumn(newExpr));
                    if (!gStatus.ok()) {
                        return gStatus;
                    }
                }
                if (!column->getAggFunName().empty()) {
                    return GraphStatus::setSemanticError(
                            folly::stringPrintf("could not apply aggregation function on `$%s.*'",
                                                 var->c_str()));
                }
                continue;
            }
        }

        auto fun = column->getAggFunName();
        if (!fun.empty()) {
            auto foundAgg = AggFun::nameIdMap_.find(fun);
            if (foundAgg == AggFun::nameIdMap_.end()) {
                return GraphStatus::setSemanticError(
                        folly::stringPrintf("Unkown aggregate function: `%s'", fun.c_str()));
            }
            hasAggFun_ = true;
        }

        gStatus = makeOutputColumn(column->clone().release());
        if (!gStatus.ok()) {
            return gStatus;
        }
    }
    return GraphStatus::OK();
}

GraphStatus YieldValidator::validateWhere(const WhereClause *clause) {
    Expression *filter = nullptr;
    if (clause != nullptr) {
        filter = clause->filter();
    }
    if (filter != nullptr) {
        auto status = deduceProps(filter, exprProps_);
        if (!status.ok()) {
            return GraphStatus::setUnsupportedExpr(filter->toString());
        }
    }
    return GraphStatus::OK();
}

GraphStatus YieldValidator::toPlan() {
    auto yield = static_cast<const YieldSentence *>(sentence_);

    Filter *filter = nullptr;
    if (yield->where()) {
        filter = Filter::make(qctx_, nullptr, yield->where()->filter());
        std::vector<std::string> colNames(inputs_.size());
        std::transform(
            inputs_.cbegin(), inputs_.cend(), colNames.begin(), [](auto &in) { return in.first; });
        filter->setColNames(std::move(colNames));
    }

    SingleInputNode *dedupDep = nullptr;
    if (!hasAggFun_) {
        dedupDep = Project::make(qctx_, filter, columns_);
    } else {
        // We do not use group items later, so move it is safe
        dedupDep = Aggregate::make(qctx_, filter, {}, std::move(groupItems_));
    }

    dedupDep->setColNames(std::move(outputColumnNames_));
    if (filter != nullptr) {
        dedupDep->setInputVar(filter->varName());
        tail_ = filter;
    } else {
        tail_ = dedupDep;
    }

    if (!exprProps_.varProps().empty()) {
        DCHECK_EQ(exprProps_.varProps().size(), 1u);
        auto var = exprProps_.varProps().cbegin()->first;
        static_cast<SingleInputNode *>(tail_)->setInputVar(var);
    }

    if (yield->yield()->isDistinct()) {
        auto dedup = Dedup::make(qctx_, dedupDep);
        dedup->setColNames(dedupDep->colNames());
        dedup->setInputVar(dedupDep->varName());
        root_ = dedup;
    } else {
        root_ = dedupDep;
    }

    return GraphStatus::OK();
}

}   // namespace graph
}   // namespace nebula
