/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/GroupByValidator.h"
#include "planner/Query.h"


namespace nebula {
namespace graph {

GraphStatus GroupByValidator::validateImpl() {
    auto *groupBySentence = static_cast<GroupBySentence*>(sentence_);
    auto gStatus = validateGroup(groupBySentence->groupClause());
    if (!gStatus.ok()) {
        return gStatus;
    }

    gStatus = validateYield(groupBySentence->yieldClause());
    if (!gStatus.ok()) {
        return gStatus;
    }

    gStatus = checkInputProps();
    if (!gStatus.ok()) {
        return gStatus;
    }

    gStatus = checkVarProps();
    if (!gStatus.ok()) {
        return gStatus;
    }

    if (!exprProps_.srcTagProps().empty() || !exprProps_.dstTagProps().empty()) {
        return GraphStatus::setSemanticError(
                "Only support input and variable in GroupBy sentence");
    }
    if (!exprProps_.inputProps().empty() && !exprProps_.varProps().empty()) {
        return GraphStatus::setSemanticError(
                "Not support both input and variable in GroupBy sentence");
    }
    return GraphStatus::OK();
}

GraphStatus GroupByValidator::checkInputProps() const {
    auto& inputProps = const_cast<ExpressionProps*>(&exprProps_)->inputProps();
    if (inputs_.empty() && !inputProps.empty()) {
        return GraphStatus::setSemanticError("no inputs for GroupBy.");
    }
    GraphStatus gStatus;
    for (auto &prop : inputProps) {
        DCHECK_NE(prop, "*");
        gStatus = checkPropNonexistOrDuplicate(inputs_, prop);
        if (!gStatus.ok()) {
            return gStatus;
        }
    }
    return GraphStatus::OK();
}

GraphStatus GroupByValidator::checkVarProps() const {
    auto& varProps = const_cast<ExpressionProps*>(&exprProps_)->varProps();
    GraphStatus gStatus;
    for (auto &pair : varProps) {
        auto &var = pair.first;
        if (!vctx_->existVar(var)) {
            return GraphStatus::setSemanticError(
                    folly::stringPrintf("variable `%s' not exist.", var.c_str()));
        }
        auto &props = vctx_->getVar(var);
        for (auto &prop : pair.second) {
            DCHECK_NE(prop, "*");
            gStatus = checkPropNonexistOrDuplicate(props, prop);
            if (!gStatus.ok()) {
                return gStatus;
            }
        }
    }
    return GraphStatus::OK();
}

GraphStatus GroupByValidator::validateYield(const YieldClause *yieldClause) {
    std::vector<YieldColumn*> columns;
    if (yieldClause != nullptr) {
        columns = yieldClause->columns();
    }
    if (columns.empty()) {
        return GraphStatus::setSemanticError("Yield cols is Empty");
    }

    Status status;
    for (auto* col : columns) {
        auto fun = col->getAggFunName();
        if (!fun.empty()) {
            auto iter = AggFun::nameIdMap_.find(fun);
            if (iter == AggFun::nameIdMap_.end()) {
                return GraphStatus::setSemanticError(
                        folly::stringPrintf("Unkown aggregate function `%s`", fun.c_str()));
            }
            if (iter->second != AggFun::Function::kCount && col->expr()->toString() == "*") {
                return GraphStatus::setInvalidExpr(col->toString());
            }
        }

        // todo(jmq) count(distinct)
        groupItems_.emplace_back(Aggregate::GroupItem{col->expr(), AggFun::nameIdMap_[fun], false});

        auto typeResult = deduceExprType(col->expr());
        if (!typeResult.ok()) {
            return GraphStatus::setInvalidExpr(col->expr()->toString());
        }
        auto type = std::move(typeResult).value();
        auto name = deduceColName(col);
        outputs_.emplace_back(name, type);
        outputColumnNames_.emplace_back(std::move(name));
        // todo(jmq) extend $-.*

        yieldCols_.emplace_back(col);
        if (col->alias() != nullptr) {
            aliases_.emplace(*col->alias(), col);
        }

        // check input yield filed without agg function and not in group cols
        ExpressionProps yieldProps;
        status = deduceProps(col->expr(), yieldProps);
        if (!status.ok()) {
            return GraphStatus::setInvalidExpr(col->expr()->toString());
        }

        if (col->getAggFunName().empty()) {
            if (!yieldProps.inputProps().empty()) {
                if (!exprProps_.isSubsetOfInput(yieldProps.inputProps())) {
                    return GraphStatus::setInvalidExpr(col->toString().c_str());
                }
            } else if (!yieldProps.varProps().empty()) {
                if (!exprProps_.isSubsetOfVar(yieldProps.varProps())) {
                    return GraphStatus::setInvalidExpr(col->toString());
                }
            }
        }
        exprProps_.unionProps(std::move(yieldProps));
    }
    return GraphStatus::OK();
}


GraphStatus GroupByValidator::validateGroup(const GroupClause *groupClause) {
    std::vector<YieldColumn*> columns;
    if (groupClause != nullptr) {
        columns = groupClause->columns();
    }

    if (columns.empty()) {
        return GraphStatus::setSemanticError("Group cols is Empty");
    }
    Status status;
    for (auto* col : columns) {
        if (col->expr()->kind() != Expression::Kind::kInputProperty &&
            col->expr()->kind() != Expression::Kind::kFunctionCall) {
            return GraphStatus::setInvalidExpr(col->toString());
        }
        if (!col->getAggFunName().empty()) {
            return GraphStatus::setInvalidExpr(col->toString());
        }
        auto typeResult = deduceExprType(col->expr());
        if (!typeResult.ok()) {
            return GraphStatus::setInvalidExpr(col->toString());
        }

        status = deduceProps(col->expr(), exprProps_);
        if (!status.ok()) {
            return GraphStatus::setUnsupportedExpr(col->expr()->toString());
        }

        groupCols_.emplace_back(col);
        groupKeys_.emplace_back(col->expr());
    }
    return GraphStatus::OK();
}

GraphStatus GroupByValidator::toPlan() {
    auto *groupBy = Aggregate::make(qctx_, nullptr, std::move(groupKeys_), std::move(groupItems_));
    groupBy->setColNames(std::vector<std::string>(outputColumnNames_));
    root_ = groupBy;
    tail_ = groupBy;
    return GraphStatus::OK();
}

}  // namespace graph
}  // namespace nebula
