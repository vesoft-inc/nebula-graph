/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/RewriteMatchLabelVisitor.h"

namespace nebula {
namespace graph {

void RewriteMatchLabelVisitor::visit(TypeCastingExpression *expr) {
    if (isLabel(expr->operand())) {
        expr->setOperand(rewriter_(expr->operand()));
    } else {
        expr->operand()->accept(this);
    }
}


void RewriteMatchLabelVisitor::visit(UnaryExpression *expr) {
    if (isLabel(expr->operand())) {
        expr->setOperand(rewriter_(expr->operand()));
    } else {
        expr->operand()->accept(this);
    }
}


void RewriteMatchLabelVisitor::visit(FunctionCallExpression *expr) {
    for (auto &arg : expr->args()->args()) {
        if (isLabel(arg.get())) {
            arg.reset(rewriter_(arg.get()));
        } else {
            arg->accept(this);
        }
    }
}


void RewriteMatchLabelVisitor::visit(ListExpression *expr) {
    auto newItems = rewriteExprList(expr->items());
    if (!newItems.empty()) {
        expr->setItems(std::move(newItems));
    }
}


void RewriteMatchLabelVisitor::visit(SetExpression *expr) {
    auto newItems = rewriteExprList(expr->items());
    if (!newItems.empty()) {
        expr->setItems(std::move(newItems));
    }
}


void RewriteMatchLabelVisitor::visit(MapExpression *expr) {
    auto items = expr->items();
    auto iter = std::find_if(items.cbegin(), items.cend(), [] (auto &pair) {
        return isLabel(pair.second);
    });
    if (iter == items.cend()) {
        return;
    }

    std::vector<MapExpression::Item> newItems;
    newItems.reserve(items.size());
    for (auto &pair : items) {
        MapExpression::Item newItem;
        newItem.first.reset(new std::string(*pair.first));
        if (isLabel(pair.second)) {
            newItem.second.reset(rewriter_(pair.second));
        } else {
            newItem.second = pair.second->clone();
            newItem.second->accept(this);
        }
        newItems.emplace_back(std::move(newItem));
    }

    expr->setItems(std::move(newItems));
}


void RewriteMatchLabelVisitor::visitBinaryExpr(BinaryExpression *expr) {
    if (isLabel(expr->left())) {
        expr->setLeft(rewriter_(expr->left()));
    } else {
        expr->left()->accept(this);
    }
    if (isLabel(expr->right())) {
        expr->setRight(rewriter_(expr->right()));
    } else {
        expr->right()->accept(this);
    }
}


std::vector<std::unique_ptr<Expression>>
RewriteMatchLabelVisitor::rewriteExprList(const std::vector<const Expression*> &list) {
    std::vector<std::unique_ptr<Expression>> newList;
    auto iter = std::find_if(list.cbegin(), list.cend(), isLabel);
    if (iter != list.cend()) {
        std::for_each(list.cbegin(), list.cend(), [this] (auto expr) {
            const_cast<Expression*>(expr)->accept(this);
        });
        return newList;
    }

    newList.reserve(list.size());
    for (auto expr : list) {
        if (isLabel(expr)) {
            newList.emplace_back(rewriter_(expr));
        } else {
            auto newExpr = expr->clone();
            newExpr->accept(this);
            newList.emplace_back(std::move(newExpr));
        }
    }

    return newList;
}

}   // namespace graph
}   // namespace nebula
