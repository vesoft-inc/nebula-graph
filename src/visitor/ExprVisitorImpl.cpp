/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/ExprVisitorImpl.h"

#include "common/expression/ArithmeticExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/ContainerExpression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/LabelExpression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/RelationalExpression.h"
#include "common/expression/SubscriptExpression.h"
#include "common/expression/SymbolPropertyExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "common/expression/UUIDExpression.h"
#include "common/expression/UnaryExpression.h"
#include "common/expression/VariableExpression.h"

namespace nebula {
namespace graph {

void ExprVisitorImpl::visitArithmeticExpr(const ArithmeticExpression *expr) {
    visitBinaryExpr(expr);
}

void ExprVisitorImpl::visitUnaryExpr(const UnaryExpression *expr) {
    expr->operand()->accept(this);
}

void ExprVisitorImpl::visitRelationalExpr(const RelationalExpression *expr) {
    visitBinaryExpr(expr);
}

void ExprVisitorImpl::visitLogicalExpr(const LogicalExpression *expr) {
    visitBinaryExpr(expr);
}

void ExprVisitorImpl::visitTypeCastingExpr(const TypeCastingExpression *expr) {
    expr->operand()->accept(this);
}

void ExprVisitorImpl::visitFunctionCallExpr(const FunctionCallExpression *expr) {
    for (const auto &arg : expr->args()->args()) {
        arg->accept(this);
        if (!ok()) {
            break;
        }
    }
}

void ExprVisitorImpl::visitListExpr(const ListExpression *expr) {
    for (const auto &item : expr->items()) {
        item->accept(this);
        if (!ok()) {
            break;
        }
    }
}

void ExprVisitorImpl::visitSetExpr(const SetExpression *expr) {
    for (const auto &item : expr->items()) {
        item->accept(this);
        if (!ok()) {
            break;
        }
    }
}

void ExprVisitorImpl::visitMapExpr(const MapExpression *expr) {
    for (const auto &pair : expr->items()) {
        pair.second->accept(this);
        if (!ok()) {
            break;
        }
    }
}

void ExprVisitorImpl::visitSubscriptExpr(const SubscriptExpression *expr) {
    visitBinaryExpr(expr);
}

void ExprVisitorImpl::visitConstantExpr(const ConstantExpression *expr) {
    VLOG(1) << "Visit constant expr: " << expr->toString();
}

void ExprVisitorImpl::visitEdgePropertyExpr(const EdgePropertyExpression *expr) {
    VLOG(1) << "Visit edge property expr: " << expr->toString();
}

void ExprVisitorImpl::visitTagPropertyExpr(const TagPropertyExpression *expr) {
    VLOG(1) << "Visit tag property expr: " << expr->toString();
}

void ExprVisitorImpl::visitInputPropertyExpr(const InputPropertyExpression *expr) {
    VLOG(1) << "Visit input property expr: " << expr->toString();
}

void ExprVisitorImpl::visitVariablePropertyExpr(const VariablePropertyExpression *expr) {
    VLOG(1) << "Visit variable property expr: " << expr->toString();
}

void ExprVisitorImpl::visitSourcePropertyExpr(const SourcePropertyExpression *expr) {
    VLOG(1) << "Visit source property expr: " << expr->toString();
}

void ExprVisitorImpl::visitDestPropertyExpr(const DestPropertyExpression *expr) {
    VLOG(1) << "Visit dest property expr: " << expr->toString();
}

void ExprVisitorImpl::visitEdgeSrcIdExpr(const EdgeSrcIdExpression *expr) {
    VLOG(1) << "Visit edge src id expr: " << expr->toString();
}

void ExprVisitorImpl::visitEdgeTypeExpr(const EdgeTypeExpression *expr) {
    VLOG(1) << "Visit edge type expr: " << expr->toString();
}

void ExprVisitorImpl::visitEdgeRankExpr(const EdgeRankExpression *expr) {
    VLOG(1) << "Visit edge rank expr: " << expr->toString();
}

void ExprVisitorImpl::visitEdgeDstIdExpr(const EdgeDstIdExpression *expr) {
    VLOG(1) << "Visit edge dst id expr: " << expr->toString();
}

void ExprVisitorImpl::visitUUIDExpr(const UUIDExpression *expr) {
    VLOG(1) << "Visit uuid expr: " << expr->toString();
}

void ExprVisitorImpl::visitVariableExpr(const VariableExpression *expr) {
    VLOG(1) << "Visit variable expr: " << expr->toString();
}

void ExprVisitorImpl::visitVersionedVariableExpr(const VersionedVariableExpression *expr) {
    VLOG(1) << "Visit versioned variable expr: " << expr->toString();
}

void ExprVisitorImpl::visitLabelExpr(const LabelExpression *expr) {
    VLOG(1) << "Visit label expr: " << expr->toString();
}

void ExprVisitorImpl::visitSymbolPropertyExpr(const SymbolPropertyExpression *expr) {
    VLOG(1) << "Visit symbol property expr: " << expr->toString();
}

void ExprVisitorImpl::visitBinaryExpr(const BinaryExpression *expr) {
    expr->left()->accept(this);
    if (ok()) {
        expr->right()->accept(this);
    }
}

}   // namespace graph
}   // namespace nebula
