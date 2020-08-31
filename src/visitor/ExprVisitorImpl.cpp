/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/ExprVisitorImpl.h"

namespace nebula {
namespace graph {

void ExprVisitorImpl::visit(ConstantExpression *expr) {
    VLOG(4) << "ExprVisitorImpl visit constant expr " << expr->toString();
}

void ExprVisitorImpl::visit(UnaryExpression *expr) {
    expr->operand()->accept(this);
}

void ExprVisitorImpl::visit(TypeCastingExpression *expr) {
    expr->operand()->accept(this);
}

void ExprVisitorImpl::visit(LabelExpression *expr) {
    VLOG(4) << "ExprVisitorImpl visit label expr " << expr->toString();
}

// binary expression
void ExprVisitorImpl::visit(ArithmeticExpression *expr) {
    visitBinaryExpr(expr);
}

void ExprVisitorImpl::visit(RelationalExpression *expr) {
    visitBinaryExpr(expr);
}

void ExprVisitorImpl::visit(SubscriptExpression *expr) {
    visitBinaryExpr(expr);
}

void ExprVisitorImpl::visit(AttributeExpression *expr) {
    visitBinaryExpr(expr);
}

void ExprVisitorImpl::visit(LogicalExpression *expr) {
    visitBinaryExpr(expr);
}

void ExprVisitorImpl::visit(LabelAttributeExpression *expr) {
    const_cast<LabelExpression *>(expr->left())->accept(this);
    if (ok()) {
        const_cast<LabelExpression *>(expr->right())->accept(this);
    }
}

// function call
void ExprVisitorImpl::visit(FunctionCallExpression *expr) {
    for (const auto &arg : expr->args()->args()) {
        arg->accept(this);
        if (!ok()) {
            break;
        }
    }
}

void ExprVisitorImpl::visit(UUIDExpression *expr) {
    VLOG(4) << "ExprVisitorImpl visit uuid expr " << expr->toString();
}
// variable expression
void ExprVisitorImpl::visit(VariableExpression *expr) {
    VLOG(4) << "ExprVisitorImpl visit variable expr " << expr->toString();
}

void ExprVisitorImpl::visit(VersionedVariableExpression *expr) {
    VLOG(4) << "ExprVisitorImpl visit version variable expr " << expr->toString();
}

// container expression
void ExprVisitorImpl::visit(ListExpression *expr) {
    for (auto item : expr->items()) {
        const_cast<Expression *>(item)->accept(this);
        if (!ok()) {
            break;
        }
    }
}

void ExprVisitorImpl::visit(SetExpression *expr) {
    for (auto item : expr->items()) {
        const_cast<Expression *>(item)->accept(this);
        if (!ok()) {
            break;
        }
    }
}

void ExprVisitorImpl::visit(MapExpression *expr) {
    for (auto &pair : expr->items()) {
        const_cast<Expression *>(pair.second)->accept(this);
        if (!ok()) {
            break;
        }
    }
}

// property Expression
void ExprVisitorImpl::visit(TagPropertyExpression *expr) {
    VLOG(4) << "ExprVisitorImpl visit tag property expr: " << expr->toString();
}

void ExprVisitorImpl::visit(EdgePropertyExpression *expr) {
    VLOG(4) << "ExprVisitorImpl visit edge property expr: " << expr->toString();
}

void ExprVisitorImpl::visit(InputPropertyExpression *expr) {
    VLOG(4) << "ExprVisitorImpl visit input property expr: " << expr->toString();
}

void ExprVisitorImpl::visit(VariablePropertyExpression *expr) {
    VLOG(4) << "ExprVisitorImpl visit variable property expr: " << expr->toString();
}

void ExprVisitorImpl::visit(DestPropertyExpression *expr) {
    VLOG(4) << "ExprVisitorImpl visit dst property expr: " << expr->toString();
}

void ExprVisitorImpl::visit(SourcePropertyExpression *expr) {
    VLOG(4) << "ExprVisitorImpl visit source property expr: " << expr->toString();
}

void ExprVisitorImpl::visit(EdgeSrcIdExpression *expr) {
    VLOG(4) << "ExprVisitorImpl visit edge src id expr: " << expr->toString();
}

void ExprVisitorImpl::visit(EdgeTypeExpression *expr) {
    VLOG(4) << "ExprVisitorImpl visit edge type expr: " << expr->toString();
}

void ExprVisitorImpl::visit(EdgeRankExpression *expr) {
    VLOG(4) << "ExprVisitorImpl visit edge rank expr: " << expr->toString();
}

void ExprVisitorImpl::visit(EdgeDstIdExpression *expr) {
    VLOG(4) << "ExprVisitorImpl visit dst id expr: " << expr->toString();
}

// vertex/edge expression
void ExprVisitorImpl::visit(VertexExpression *expr) {
    VLOG(4) << "ExprVisitorImpl visit vertex expr: " << expr->toString();
}

void ExprVisitorImpl::visit(EdgeExpression *expr) {
    VLOG(4) << "ExprVisitorImpl visit edge expr: " << expr->toString();
}

void ExprVisitorImpl::visitBinaryExpr(BinaryExpression *expr) {
    expr->left()->accept(this);
    if (ok()) {
        expr->right()->accept(this);
    }
}

}   // namespace graph
}   // namespace nebula
