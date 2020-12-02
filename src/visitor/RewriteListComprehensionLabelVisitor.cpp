/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/RewriteListComprehensionLabelVisitor.h"

namespace nebula {
namespace graph {

void RewriteListComprehensionLabelVisitor::visit(TypeCastingExpression *expr) {
    if (isLabel(expr->operand())) {
        expr->setOperand(rewriteLabel(expr->operand()));
    } else {
        expr->operand()->accept(this);
    }
}

void RewriteListComprehensionLabelVisitor::visit(UnaryExpression *expr) {
    if (isLabel(expr->operand())) {
        expr->setOperand(rewriteLabel(expr->operand()));
    } else {
        expr->operand()->accept(this);
    }
}

void RewriteListComprehensionLabelVisitor::visit(FunctionCallExpression *expr) {
    for (auto &arg : expr->args()->args()) {
        if (isLabel(arg.get())) {
            arg.reset(rewriteLabel(arg.get()));
        } else {
            arg->accept(this);
        }
    }
}

void RewriteListComprehensionLabelVisitor::visit(AttributeExpression *expr) {
    if (isLabel(expr->left())) {
        expr->setLeft(rewriteLabel(expr->left()));
    } else {
        expr->left()->accept(this);
    }
}

void RewriteListComprehensionLabelVisitor::visit(ListExpression *expr) {
    auto newItems = rewriteExprList(expr->items());
    if (!newItems.empty()) {
        expr->setItems(std::move(newItems));
    }
}

void RewriteListComprehensionLabelVisitor::visit(SetExpression *expr) {
    auto newItems = rewriteExprList(expr->items());
    if (!newItems.empty()) {
        expr->setItems(std::move(newItems));
    }
}

void RewriteListComprehensionLabelVisitor::visit(MapExpression *expr) {
    auto &items = expr->items();
    auto iter = std::find_if(
        items.cbegin(), items.cend(), [](auto &pair) { return isLabel(pair.second.get()); });
    if (iter == items.cend()) {
        return;
    }

    std::vector<MapExpression::Item> newItems;
    newItems.reserve(items.size());
    for (auto &pair : items) {
        MapExpression::Item newItem;
        newItem.first.reset(new std::string(*pair.first));
        if (isLabel(pair.second.get())) {
            newItem.second.reset(rewriteLabel(pair.second.get()));
        } else {
            newItem.second = pair.second->clone();
            newItem.second->accept(this);
        }
        newItems.emplace_back(std::move(newItem));
    }

    expr->setItems(std::move(newItems));
}

void RewriteListComprehensionLabelVisitor::visit(CaseExpression *expr) {
    if (expr->hasCondition()) {
        if (isLabel(expr->condition())) {
            expr->setCondition(rewriteLabel(expr));
        } else {
            expr->condition()->accept(this);
        }
    }
    if (expr->hasDefault()) {
        if (isLabel(expr->defaultResult())) {
            expr->setDefault(rewriteLabel(expr));
        } else {
            expr->defaultResult()->accept(this);
        }
    }
    auto &cases = expr->cases();
    for (size_t i = 0; i < cases.size(); ++i) {
        auto when = cases[i].when.get();
        auto then = cases[i].then.get();
        if (isLabel(when)) {
            expr->setWhen(i, rewriteLabel(when));
        } else {
            when->accept(this);
        }
        if (isLabel(then)) {
            expr->setThen(i, rewriteLabel(then));
        } else {
            then->accept(this);
        }
    }
}

void RewriteListComprehensionLabelVisitor::visitBinaryExpr(BinaryExpression *expr) {
    if (isLabel(expr->left())) {
        expr->setLeft(rewriteLabel(expr->left()));
    } else {
        expr->left()->accept(this);
    }
    if (isLabel(expr->right())) {
        expr->setRight(rewriteLabel(expr->right()));
    } else {
        expr->right()->accept(this);
    }
}

std::vector<std::unique_ptr<Expression>> RewriteListComprehensionLabelVisitor::rewriteExprList(
    const std::vector<std::unique_ptr<Expression>> &list) {
    std::vector<std::unique_ptr<Expression>> newList;
    auto iter =
        std::find_if(list.cbegin(), list.cend(), [](auto &expr) { return isLabel(expr.get()); });
    if (iter != list.cend()) {
        std::for_each(list.cbegin(), list.cend(), [this](auto &expr) {
            const_cast<Expression *>(expr.get())->accept(this);
        });
        return newList;
    }

    newList.reserve(list.size());
    for (auto &expr : list) {
        if (isLabel(expr.get())) {
            newList.emplace_back(rewriteLabel(expr.get()));
        } else {
            auto newExpr = expr->clone();
            newExpr->accept(this);
            newList.emplace_back(std::move(newExpr));
        }
    }

    return newList;
}

Expression *RewriteListComprehensionLabelVisitor::rewriteLabel(const Expression *expr) {
    if (expr->kind() == Expression::Kind::kLabel) {
        auto *label = static_cast<const LabelExpression *>(expr);
        if (*label->name() == oldVarName_) {
            return new VariableExpression(new std::string(newVarName_));
        } else {
            return label->clone().release();
        }
    } else {
        DCHECK(expr->kind() == Expression::Kind::kLabelAttribute);
        auto *la = static_cast<const LabelAttributeExpression *>(expr);
        if (*la->left()->name() == oldVarName_) {
            const auto &value = la->right()->value();
            return new AttributeExpression(new VariableExpression(new std::string(newVarName_)),
                                           new ConstantExpression(value));
        } else {
            return la->clone().release();
        }
    }
}

}   // namespace graph
}   // namespace nebula
