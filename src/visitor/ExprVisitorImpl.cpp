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

void ExprVisitorImpl::visitArithmeticExpr(ArithmeticExpression *expr) {
    visitBinaryExpr(expr);
}

void ExprVisitorImpl::visitUnaryExpr(UnaryExpression *expr) {
    expr->operand()->accept(this);
}

void ExprVisitorImpl::visitRelationalExpr(RelationalExpression *expr) {
    visitBinaryExpr(expr);
}

void ExprVisitorImpl::visitLogicalExpr(LogicalExpression *expr) {
    visitBinaryExpr(expr);
}

void ExprVisitorImpl::visitTypeCastingExpr(TypeCastingExpression *expr) {
    expr->operand()->accept(this);
}

void ExprVisitorImpl::visitFunctionCallExpr(FunctionCallExpression *expr) {
    for (const auto &arg : expr->args()->args()) {
        arg->accept(this);
        if (!ok()) {
            break;
        }
    }
}

void ExprVisitorImpl::visitListExpr(ListExpression *expr) {
    for (const auto &item : expr->items()) {
        const_cast<Expression *>(item)->accept(this);
        if (!ok()) {
            break;
        }
    }
}

void ExprVisitorImpl::visitSetExpr(SetExpression *expr) {
    for (const auto &item : expr->items()) {
        const_cast<Expression *>(item)->accept(this);
        if (!ok()) {
            break;
        }
    }
}

void ExprVisitorImpl::visitMapExpr(MapExpression *expr) {
    for (const auto &pair : expr->items()) {
        const_cast<Expression *>(pair.second)->accept(this);
        if (!ok()) {
            break;
        }
    }
}

void ExprVisitorImpl::visitSubscriptExpr(SubscriptExpression *expr) {
    visitBinaryExpr(expr);
}

void ExprVisitorImpl::visitConstantExpr(ConstantExpression *expr) {
    VLOG(1) << "Visit constant expr: " << expr->toString();
}

void ExprVisitorImpl::visitEdgePropertyExpr(EdgePropertyExpression *expr) {
    VLOG(1) << "Visit edge property expr: " << expr->toString();
}

void ExprVisitorImpl::visitTagPropertyExpr(TagPropertyExpression *expr) {
    VLOG(1) << "Visit tag property expr: " << expr->toString();
}

void ExprVisitorImpl::visitInputPropertyExpr(InputPropertyExpression *expr) {
    VLOG(1) << "Visit input property expr: " << expr->toString();
}

void ExprVisitorImpl::visitVariablePropertyExpr(VariablePropertyExpression *expr) {
    VLOG(1) << "Visit variable property expr: " << expr->toString();
}

void ExprVisitorImpl::visitSourcePropertyExpr(SourcePropertyExpression *expr) {
    VLOG(1) << "Visit source property expr: " << expr->toString();
}

void ExprVisitorImpl::visitDestPropertyExpr(DestPropertyExpression *expr) {
    VLOG(1) << "Visit dest property expr: " << expr->toString();
}

void ExprVisitorImpl::visitEdgeSrcIdExpr(EdgeSrcIdExpression *expr) {
    VLOG(1) << "Visit edge src id expr: " << expr->toString();
}

void ExprVisitorImpl::visitEdgeTypeExpr(EdgeTypeExpression *expr) {
    VLOG(1) << "Visit edge type expr: " << expr->toString();
}

void ExprVisitorImpl::visitEdgeRankExpr(EdgeRankExpression *expr) {
    VLOG(1) << "Visit edge rank expr: " << expr->toString();
}

void ExprVisitorImpl::visitEdgeDstIdExpr(EdgeDstIdExpression *expr) {
    VLOG(1) << "Visit edge dst id expr: " << expr->toString();
}

void ExprVisitorImpl::visitUUIDExpr(UUIDExpression *expr) {
    VLOG(1) << "Visit uuid expr: " << expr->toString();
}

void ExprVisitorImpl::visitVariableExpr(VariableExpression *expr) {
    VLOG(1) << "Visit variable expr: " << expr->toString();
}

void ExprVisitorImpl::visitVersionedVariableExpr(VersionedVariableExpression *expr) {
    VLOG(1) << "Visit versioned variable expr: " << expr->toString();
}

void ExprVisitorImpl::visitLabelExpr(LabelExpression *expr) {
    VLOG(1) << "Visit label expr: " << expr->toString();
}

void ExprVisitorImpl::visitSymbolPropertyExpr(SymbolPropertyExpression *expr) {
    VLOG(1) << "Visit symbol property expr: " << expr->toString();
}

void ExprVisitorImpl::visitBinaryExpr(BinaryExpression *expr) {
    expr->left()->accept(this);
    if (ok()) {
        expr->right()->accept(this);
    }
}

}   // namespace graph
}   // namespace nebula
