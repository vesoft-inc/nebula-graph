/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/GroupByValidator.h"
#include "planner/Query.h"
#include "util/ExpressionUtils.h"
#include "util/AnonColGenerator.h"
#include "util/AnonVarGenerator.h"
#include "visitor/RewriteAggExprVisitor.h"
#include "visitor/FindAnySubExprVisitor.h"


namespace nebula {
namespace graph {

Status GroupByValidator::validateImpl() {
    auto *groupBySentence = static_cast<GroupBySentence*>(sentence_);
    NG_RETURN_IF_ERROR(validateGroup(groupBySentence->groupClause()));
    NG_RETURN_IF_ERROR(validateYield(groupBySentence->yieldClause()));

    if (!exprProps_.srcTagProps().empty() || !exprProps_.dstTagProps().empty()) {
        return Status::SemanticError("Only support input and variable in GroupBy sentence.");
    }
    if (!exprProps_.inputProps().empty() && !exprProps_.varProps().empty()) {
        return Status::SemanticError("Not support both input and variable in GroupBy sentence.");
    }

    if (groupKeys_.empty()) {
        if (yieldCols_.empty()) {
            // group by *
            auto* star = qctx_->objPool()->add(new ConstantExpression("*"));
            groupKeys_.emplace_back(star);
        } else {
            // implicit group by
            groupKeys_ = yieldCols_;
        }
    } else {
        for (auto* expr : groupKeys_) {
            if (std::find(begin(yieldCols_), end(yieldCols_), expr) != yieldCols_.end()) {
                return Status::SemanticError("GROUP BY item `%s` must be in the yield list",
                                             expr->toString().c_str());
            }
        }

        std::unordered_set<Expression*> group_set(begin(groupKeys_), end(groupKeys_));
        FindAnySubExprVisitor visitor(group_set);
        for (auto* expr : yieldCols_) {
            expr->accept(&visitor);
            if (!visitor.found()) {
                return Status::SemanticError("Yield non-agg expression `%s` must be"
                " functionally dependent on items in GROUP BY clause", expr->toString().c_str());
            }
        }
    }

    return Status::OK();
}

Status GroupByValidator::validateYield(const YieldClause *yieldClause) {
    std::vector<YieldColumn*> columns;
    if (yieldClause != nullptr) {
        columns = yieldClause->columns();
    }
    if (columns.empty()) {
        return Status::SemanticError("Yield cols is Empty");
    }

    projCols_.reset(new YieldColumns);
    for (auto* col : columns) {
        auto col_name = deduceColName(col);
        auto rewrited = 0;

        // rewrite inner agg expr
        if (col->expr()->kind() != Expression::Kind::kAggregate) {
            auto aggs = ExpressionUtils::collectAll(col->expr(), {Expression::Kind::kAggregate});
            if (aggs.size() > 1) {
                return Status::SemanticError("Agg function nesting is not allowed");
            }
            if (aggs.size() == 1) {
                auto col_copy = col->expr()->clone();
                // set aggExpr
                col->setExpr(aggs[0]->clone().release());
                auto agg_col = col->toString();
                // rewrite to VariablePropertyExpression
                RewriteAggExprVisitor rewriteAggVisitor(new std::string(),
                                                        new std::string(agg_col));
                col_copy->accept(&rewriteAggVisitor);
                rewrited = 1;
                pushUp_ = true;
                projCols_->addColumn(new YieldColumn(std::move(col_copy).release(),
                                     new std::string(col_name)));
                projOutputColumnNames_.emplace_back(col_name);
            }
        }

        auto col_expr = col->expr();
        // collect exprs for check
        if (col_expr->kind() == Expression::Kind::kAggregate) {
            auto* agg_expr = static_cast<AggregateExpression*>(col_expr);
            auto func = agg_expr->name();
            if (func) {
                auto iter = AggregateExpression::nameIdMap_.find(func->c_str());
                if (iter == AggregateExpression::nameIdMap_.end()) {
                    return Status::SemanticError("Unkown aggregate function `%s`", func->c_str());
                }
                if (iter->second != AggregateExpression::Function::kCount &&
                    agg_expr->arg()->toString() == "*") {
                    // TODO : support count($-.*) count($var.*)
                    return Status::SemanticError("`%s` invaild, * valid in count.",
                                             col_expr->toString().c_str());
                }
            }
            yieldAggs_.emplace_back(col_expr);
        } else {
            yieldCols_.emplace_back(col_expr);
        }

        groupItems_.emplace_back(col_expr);
        auto status = deduceExprType(col_expr);
        NG_RETURN_IF_ERROR(status);
        auto name = deduceColName(col);
        auto type = std::move(status).value();
        if (!rewrited) {
            projCols_->addColumn(new YieldColumn(
                new VariablePropertyExpression(new std::string(),
                                           new std::string(name)),
                new std::string(col_name)));
            projOutputColumnNames_.emplace_back(col_name);
        }
        outputs_.emplace_back(name, type);
        outputColumnNames_.emplace_back(std::move(name));

        if (col->alias() != nullptr) {
            aliases_.emplace(*col->alias(), col);
        }

        ExpressionProps yieldProps;
        NG_RETURN_IF_ERROR(deduceProps(col_expr, yieldProps));
        exprProps_.unionProps(std::move(yieldProps));
    }
    return Status::OK();
}


Status GroupByValidator::validateGroup(const GroupClause *groupClause) {
    if (!groupClause) return Status::OK();
    std::vector<YieldColumn*> columns;
    if (groupClause != nullptr) {
        columns = groupClause->columns();
    }

    if (columns.empty()) {
        return Status::SemanticError("Group cols is Empty");
    }
    for (auto* col : columns) {
        if (col->expr()->kind() == Expression::Kind::kAggregate) {
            return Status::SemanticError("Use invalid group function `%s`",
                    static_cast<AggregateExpression*>(col->expr())->name()->c_str());
        }
        if (col->expr()->kind() != Expression::Kind::kInputProperty &&
            col->expr()->kind() != Expression::Kind::kVarProperty &&
            col->expr()->kind() != Expression::Kind::kFunctionCall) {
            return Status::SemanticError("Group `%s` invalid", col->expr()->toString().c_str());
        }

        NG_RETURN_IF_ERROR(deduceExprType(col->expr()));
        NG_RETURN_IF_ERROR(deduceProps(col->expr(), exprProps_));

        groupKeys_.emplace_back(col->expr());
    }
    return Status::OK();
}

Status GroupByValidator::toPlan() {
    auto *groupBy = Aggregate::make(qctx_, nullptr, std::move(groupKeys_), std::move(groupItems_));
    groupBy->setColNames(std::vector<std::string>(outputColumnNames_));
    if (pushUp_) {
        // rewrite Expr which has inner aggExpr and push it up to Project.
        auto *project = Project::make(qctx_, groupBy, std::move(projCols_).release());
        project->setInputVar(groupBy->outputVar());
        project->setColNames(projOutputColumnNames_);
        root_ = project;
    } else {
        root_ = groupBy;
    }
    auto distinct = static_cast<GroupBySentence*>(sentence_)->yieldClause()->isDistinct();
    if (distinct) {
        auto dedup = Dedup::make(qctx_, root_);
        dedup->setColNames(root_->colNames());
        dedup->setInputVar(root_->outputVar());
        root_ = dedup;
    }
    tail_ = groupBy;
    return Status::OK();
}

}  // namespace graph
}  // namespace nebula
