/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/ResolveSymPropExprVisitor.h"

#include "common/base/Logging.h"
#include "common/expression/BinaryExpression.h"
#include "common/expression/ContainerExpression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/SymbolPropertyExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "common/expression/UnaryExpression.h"

namespace nebula {
namespace graph {

ResolveSymPropExprVisitor::ResolveSymPropExprVisitor(bool isTag) : isTag_(isTag) {}

void ResolveSymPropExprVisitor::visitTypeCastingExpr(const TypeCastingExpression* expr) {
    if (isSymPropExpr(expr->operand())) {
        auto newExpr = const_cast<TypeCastingExpression*>(expr);
        auto operand = static_cast<const SymbolPropertyExpression*>(expr->operand());
        newExpr->setOperand(createExpr(operand));
    } else {
        expr->operand()->accept(this);
    }
}

void ResolveSymPropExprVisitor::visitUnaryExpr(const UnaryExpression* expr) {
    if (isSymPropExpr(expr->operand())) {
        auto newExpr = const_cast<UnaryExpression*>(expr);
        auto operand = static_cast<const SymbolPropertyExpression*>(expr->operand());
        newExpr->setOperand(createExpr(operand));
    } else {
        expr->operand()->accept(this);
    }
}

void ResolveSymPropExprVisitor::visitFunctionCallExpr(const FunctionCallExpression* expr) {
    for (auto& arg : const_cast<FunctionCallExpression*>(expr)->args()->args()) {
        if (isSymPropExpr(arg.get())) {
            auto newArg = static_cast<const SymbolPropertyExpression*>(arg.get());
            arg.reset(createExpr(newArg));
        } else {
            arg->accept(this);
        }
    }
}

void ResolveSymPropExprVisitor::visitListExpr(const ListExpression* expr) {
    auto newItems = resolveExprList(expr->items());
    if (!newItems.empty()) {
        const_cast<ListExpression*>(expr)->setItems(std::move(newItems));
    }
}

void ResolveSymPropExprVisitor::visitSetExpr(const SetExpression* expr) {
    auto newItems = resolveExprList(expr->items());
    if (!newItems.empty()) {
        const_cast<SetExpression*>(expr)->setItems(std::move(newItems));
    }
}

void ResolveSymPropExprVisitor::visitMapExpr(const MapExpression* expr) {
    auto items = expr->items();
    auto found = std::find_if(
        items.cbegin(), items.cend(), [](auto& pair) { return isSymPropExpr(pair.second); });
    if (found == items.cend()) {
        std::for_each(
            items.cbegin(), items.cend(), [this](auto& pair) { pair.second->accept(this); });
        return;
    }

    std::vector<MapExpression::Item> newItems;
    newItems.reserve(items.size());
    for (auto& pair : items) {
        MapExpression::Item newItem;
        newItem.first.reset(new std::string(*pair.first));
        if (isSymPropExpr(pair.second)) {
            auto symExpr = static_cast<const SymbolPropertyExpression*>(pair.second);
            newItem.second.reset(createExpr(symExpr));
        } else {
            newItem.second = Expression::decode(pair.second->encode());
            newItem.second->accept(this);
        }
        newItems.emplace_back(std::move(newItem));
    }
    const_cast<MapExpression*>(expr)->setItems(std::move(newItems));
}

void ResolveSymPropExprVisitor::visitBinaryExpr(const BinaryExpression* expr) {
    if (isSymPropExpr(expr->left())) {
        auto newExpr = const_cast<BinaryExpression*>(expr);
        auto left = static_cast<const SymbolPropertyExpression*>(expr->left());
        newExpr->setLeft(createExpr(left));
    } else {
        expr->left()->accept(this);
    }
    if (isSymPropExpr(expr->right())) {
        auto newExpr = const_cast<BinaryExpression*>(expr);
        auto right = static_cast<const SymbolPropertyExpression*>(expr->right());
        newExpr->setRight(createExpr(right));
    } else {
        expr->right()->accept(this);
    }
}

std::vector<std::unique_ptr<Expression>> ResolveSymPropExprVisitor::resolveExprList(
    const std::vector<const Expression*>& exprs) {
    std::vector<std::unique_ptr<Expression>> newExprs;

    auto found = std::find_if(exprs.cbegin(), exprs.cend(), isSymPropExpr);
    if (found == exprs.cend()) {
        std::for_each(exprs.cbegin(), exprs.cend(), [this](auto expr) { expr->accept(this); });
        return newExprs;
    }

    newExprs.reserve(exprs.size());
    for (auto item : exprs) {
        if (isSymPropExpr(item)) {
            auto symExpr = static_cast<const SymbolPropertyExpression*>(item);
            newExprs.emplace_back(createExpr(symExpr));
        } else {
            auto newExpr = Expression::decode(item->encode());
            newExpr->accept(this);
            newExprs.emplace_back(std::move(newExpr));
        }
    }
    return newExprs;
}

Expression* ResolveSymPropExprVisitor::createExpr(const SymbolPropertyExpression* expr) {
    auto newSym = new std::string(*expr->sym());
    auto newProp = new std::string(*expr->prop());
    if (isTag_) {
        return new TagPropertyExpression(newSym, newProp);
    }
    return new EdgePropertyExpression(newSym, newProp);
}

bool ResolveSymPropExprVisitor::isSymPropExpr(const Expression* expr) {
    return expr->kind() == Expression::Kind::kSymProperty;
}

}   // namespace graph
}   // namespace nebula
