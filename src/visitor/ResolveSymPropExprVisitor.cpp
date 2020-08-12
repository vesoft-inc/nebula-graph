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
    if (expr->operand()->kind() == Expression::Kind::kSymProperty) {
        auto newExpr = const_cast<TypeCastingExpression*>(expr);
        auto operand = static_cast<const SymbolPropertyExpression*>(expr->operand());
        newExpr->setOperand(createExpr(operand));
    } else {
        expr->operand()->accept(this);
    }
}

void ResolveSymPropExprVisitor::visitUnaryExpr(const UnaryExpression* expr) {
    if (expr->operand()->kind() == Expression::Kind::kSymProperty) {
        auto newExpr = const_cast<UnaryExpression*>(expr);
        auto operand = static_cast<const SymbolPropertyExpression*>(expr->operand());
        newExpr->setOperand(createExpr(operand));
    } else {
        expr->operand()->accept(this);
    }
}

void ResolveSymPropExprVisitor::visitFunctionCallExpr(const FunctionCallExpression* expr) {
    for (auto& arg : const_cast<FunctionCallExpression*>(expr)->args()->args()) {
        if (arg->kind() == Expression::Kind::kSymProperty) {
            auto newArg = static_cast<const SymbolPropertyExpression*>(arg.get());
            arg.reset(createExpr(newArg));
        } else {
            arg->accept(this);
        }
    }
}

void ResolveSymPropExprVisitor::visitListExpr(const ListExpression* expr) {
    for (auto& item : expr->items()) {
        if (item->kind() == Expression::Kind::kSymProperty) {
            // FIXME
            // auto newArg = static_cast<const SymbolPropertyExpression*>(item.get());
            // item.reset(createExpr(newArg));
        } else {
            item->accept(this);
        }
    }
}

void ResolveSymPropExprVisitor::visitSetExpr(const SetExpression* expr) {
    for (auto& item : const_cast<SetExpression*>(expr)->items()) {
        if (item->kind() == Expression::Kind::kSymProperty) {
            // FIXME
            // auto newArg = static_cast<const SymbolPropertyExpression*>(item.get());
            // item.reset(createExpr(newArg));
        } else {
            item->accept(this);
        }
    }
}

void ResolveSymPropExprVisitor::visitMapExpr(const MapExpression* expr) {
    for (auto& pair : expr->items()) {
        if (pair.second->kind() == Expression::Kind::kSymProperty) {
            // FIXME
            // auto newArg = static_cast<const SymbolPropertyExpression*>(pair.second.get());
            // pair.second.reset(createExpr(newArg));
        } else {
            pair.second->accept(this);
        }
    }
}

void ResolveSymPropExprVisitor::visitBinaryExpr(const BinaryExpression* expr) {
    if (expr->left()->kind() == Expression::Kind::kSymProperty) {
        auto newExpr = const_cast<BinaryExpression*>(expr);
        auto left = static_cast<const SymbolPropertyExpression*>(expr->left());
        newExpr->setLeft(createExpr(left));
    } else {
        expr->left()->accept(this);
    }
    if (expr->right()->kind() == Expression::Kind::kSymProperty) {
        auto newExpr = const_cast<BinaryExpression*>(expr);
        auto right = static_cast<const SymbolPropertyExpression*>(expr->right());
        newExpr->setRight(createExpr(right));
    } else {
        expr->right()->accept(this);
    }
}

Expression* ResolveSymPropExprVisitor::createExpr(const SymbolPropertyExpression* expr) {
    auto newSym = new std::string(*expr->sym());
    auto newProp = new std::string(*expr->prop());
    if (isTag_) {
        return new TagPropertyExpression(newSym, newProp);
    }
    return new EdgePropertyExpression(newSym, newProp);
}

}   // namespace graph
}   // namespace nebula
