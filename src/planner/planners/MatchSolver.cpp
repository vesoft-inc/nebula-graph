/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/planners/MatchSolver.h"

#include "visitor/RewriteMatchLabelVisitor.h"

namespace nebula {
namespace graph {

Status MatchSolver::buildReturn(MatchAstContext* matchCtx, SubPlan& subPlan) {
    auto *yields = new YieldColumns();
    std::vector<std::string> colNames;
    auto *sentence = static_cast<MatchSentence*>(matchCtx->sentence);
    PlanNode *current = subPlan.root;

    for (auto *col : matchCtx->yieldColumns->columns()) {
        auto kind = col->expr()->kind();
        YieldColumn *newColumn = nullptr;
        if (kind == Expression::Kind::kLabel) {
            auto *label = static_cast<const LabelExpression*>(col->expr());
            newColumn = new YieldColumn(rewrite(label));
        } else if (kind == Expression::Kind::kLabelAttribute) {
            auto *la = static_cast<const LabelAttributeExpression*>(col->expr());
            newColumn = new YieldColumn(rewrite(la));
        } else {
            auto newExpr = col->expr()->clone();
            auto rewriter = [] (const Expression *expr) {
                if (expr->kind() == Expression::Kind::kLabel) {
                    return rewrite(static_cast<const LabelExpression*>(expr));
                } else {
                    return rewrite(static_cast<const LabelAttributeExpression*>(expr));
                }
            };
            RewriteMatchLabelVisitor visitor(std::move(rewriter));
            newExpr->accept(&visitor);
            newColumn = new YieldColumn(newExpr.release());
        }
        yields->addColumn(newColumn);
        if (col->alias() != nullptr) {
            colNames.emplace_back(*col->alias());
        } else {
            colNames.emplace_back(col->expr()->toString());
        }
    }

    auto *project = Project::make(matchCtx->qctx, current, yields);
    project->setInputVar(current->outputVar());
    project->setColNames(std::move(colNames));

    current = project;
    if (sentence->ret()->isDistinct()) {
        auto *dedup = Dedup::make(matchCtx->qctx, current);
        dedup->setInputVar(current->outputVar());
        dedup->setColNames(current->colNames());
        current = dedup;
    }

    if (sentence->ret()->orderFactors() != nullptr) {
        auto inputColList = current->colNames();
        std::unordered_map<std::string, size_t> inputColIndices;
        for (auto i = 0u; i < inputColList.size(); i++) {
            if (!inputColIndices.emplace(inputColList[i], i).second) {
                return Status::SemanticError("Duplicated columns not allowed: %s",
                        inputColList[i].c_str());
            }
        }

        std::vector<std::pair<size_t, OrderFactor::OrderType>> indexedFactors;
        auto *factors = sentence->ret()->orderFactors();
        for (auto &factor : factors->factors()) {
            if (factor->expr()->kind() != Expression::Kind::kLabel) {
                return Status::SemanticError("Only column name can be used as sort item");
            }
            auto *name = static_cast<const LabelExpression*>(factor->expr())->name();
            auto iter = inputColIndices.find(*name);
            if (iter == inputColIndices.end()) {
                return Status::SemanticError("Column `%s' not found", name->c_str());
            }
            indexedFactors.emplace_back(iter->second, factor->orderType());
        }

        auto *sort = Sort::make(matchCtx->qctx, current, std::move(indexedFactors));
        sort->setInputVar(current->outputVar());
        sort->setColNames(current->colNames());
        current = sort;
    }

    auto *skipExpr = sentence->ret()->skip();
    auto *limitExpr = sentence->ret()->limit();
    if (skipExpr != nullptr || limitExpr != nullptr) {
        int64_t offset = 0;
        int64_t count = 0;
        if (skipExpr != nullptr) {
            auto *constant = static_cast<const ConstantExpression*>(skipExpr);
            offset = constant->value().getInt();
        }
        if (limitExpr != nullptr) {
            auto *constant = static_cast<const ConstantExpression*>(limitExpr);
            count = constant->value().getInt();
        }
        if (count == 0) {
            count = std::numeric_limits<int64_t>::max();
        }
        auto *limit = Limit::make(matchCtx->qctx, current, offset, count);
        limit->setInputVar(current->outputVar());
        limit->setColNames(current->colNames());
        current = limit;
    }

    subPlan.root = current;

    return Status::OK();
}

Expression* MatchSolver::rewrite(const LabelExpression *label) {
    auto *expr = new VariablePropertyExpression(
            new std::string(),
            new std::string(*label->name()));
    return expr;
}

Expression* MatchSolver::rewrite(const LabelAttributeExpression *la) {
    auto *expr = new AttributeExpression(
            new VariablePropertyExpression(
                new std::string(),
                new std::string(*la->left()->name())),
            new LabelExpression(*la->right()->name()));
    return expr;
}
}  // namespace graph
}  // namespace nebula
