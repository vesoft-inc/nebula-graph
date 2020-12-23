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
        // if (yieldCols_.empty()) {
        //     // group by *
        //     auto* star = qctx_->objPool()->add(new ConstantExpression("*"));
        //     groupKeys_.emplace_back(star);
        // } else {
            // implicit group by
            groupKeys_ = yieldCols_;
        // }
    } else {
        // check yield non-agg expr
        std::unordered_set<Expression*> groupSet(begin(groupKeys_), end(groupKeys_));
        FindAnySubExprVisitor groupVisitor(groupSet, true);
        for (auto* expr : yieldCols_) {
            expr->accept(&groupVisitor);
            if (!groupVisitor.found()) {
                return Status::SemanticError("Yield non-agg expression `%s` must be"
                " functionally dependent on items in GROUP BY clause", expr->toString().c_str());
            }
        }

        if (!yieldCols_.empty()) {
            std::unordered_set<Expression*> yieldSet(begin(yieldCols_), end(yieldCols_));
            FindAnySubExprVisitor yieldVisitor(yieldSet, false);
            for (auto* expr : groupKeys_) {
                expr->accept(&yieldVisitor);
                if (!yieldVisitor.found()) {
                    return Status::SemanticError("GroupBy item `%s` must be"
                    " in Yield list", expr->toString().c_str());
                }
            }
        } else {
            return Status::SemanticError("GroupBy list must in Yield list");
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
        auto colOldName = deduceColName(col);
        auto rewrited = false;

        // rewrite inner agg expr
        if (col->expr()->kind() != Expression::Kind::kAggregate) {
            auto* collectAggCol = qctx_->objPool()->add(col->expr()->clone().release());
            auto aggs = ExpressionUtils::collectAll(collectAggCol, {Expression::Kind::kAggregate});
            if (aggs.size() > 1) {
                return Status::SemanticError("Agg function nesting is not allowed");
            }
            if (aggs.size() == 1) {
                auto colRewited = col->expr()->clone().release();
                // set aggExpr
                col->setExpr(aggs[0]->clone().release());
                auto aggColName = col->expr()->toString();
                // rewrite to VariablePropertyExpression
                RewriteAggExprVisitor rewriteAggVisitor(new std::string(),
                                                        new std::string(aggColName));
                colRewited->accept(&rewriteAggVisitor);
                rewrited = true;
                needGenProject_ = true;
                projCols_->addColumn(new YieldColumn(colRewited,
                                     new std::string(colOldName)));
            }
        }

        auto colExpr = col->expr();
        // collect exprs for check
        if (colExpr->kind() == Expression::Kind::kAggregate) {
            auto* agg_expr = static_cast<AggregateExpression*>(colExpr);
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
                                             colExpr->toString().c_str());
                }
            }
            yieldAggs_.emplace_back(colExpr);
        } else {
            yieldCols_.emplace_back(colExpr);
        }

        groupItems_.emplace_back(colExpr);
        auto status = deduceExprType(colExpr);
        NG_RETURN_IF_ERROR(status);
        auto type = std::move(status).value();
        std::string name;
        if (!rewrited) {
            name = deduceColName(col);
            projCols_->addColumn(new YieldColumn(

                new VariablePropertyExpression(new std::string(),
                                           new std::string(name)),
                new std::string(colOldName)));
        } else {
            name = colExpr->toString();
        }
        projOutputColumnNames_.emplace_back(colOldName);
        outputs_.emplace_back(name, type);
        outputColumnNames_.emplace_back(name);

        if (col->alias() != nullptr && !rewrited) {
            aliases_.emplace(*col->alias(), col);
        }

        ExpressionProps yieldProps;
        NG_RETURN_IF_ERROR(deduceProps(colExpr, yieldProps));
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

    auto groupByValid = [](Expression::Kind kind)->bool {
        return kind < Expression::Kind::kCase;
    };
    for (auto* col : columns) {
        if (!groupByValid(col->expr()->kind())) {
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
    if (needGenProject_) {
        // rewrite Expr which has inner aggExpr and push it up to Project.
        auto *project = Project::make(qctx_, groupBy, std::move(projCols_).release());
        project->setInputVar(groupBy->outputVar());
        project->setColNames(projOutputColumnNames_);
        root_ = project;
    } else {
        root_ = groupBy;
    }
    tail_ = groupBy;
    return Status::OK();
}

}  // namespace graph
}  // namespace nebula
