/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/FindExprVisitor.h"

namespace nebula {
namespace graph {

FindExprVisitor::FindExprVisitor(const std::unordered_set<Expression::Kind> &exprs)
    : exprs_(exprs) {
    DCHECK(!exprs.empty());
}

void FindExprVisitor::visit(TypeCastingExpression *expr) {
    findExpr(expr);
    if (found_) return;
    expr->operand()->accept(this);
}

void FindExprVisitor::visit(UnaryExpression *expr) {
    findExpr(expr);
    if (found_) return;
    expr->operand()->accept(this);
}

void FindExprVisitor::visit(FunctionCallExpression *expr) {
    findExpr(expr);
    if (found_) return;
    for (const auto &arg : expr->args()->args()) {
        arg->accept(this);
        if (found_) break;
    }
}

void FindExprVisitor::visit(ListExpression *expr) {
    findExpr(expr);
    if (found_) return;
    for (const auto &item : expr->items()) {
        const_cast<Expression *>(item)->accept(this);
        if (found_) return;
    }
}

void FindExprVisitor::visit(SetExpression *expr) {
    findExpr(expr);
    if (found_) return;
    for (const auto &item : expr->items()) {
        const_cast<Expression *>(item)->accept(this);
        if (found_) return;
    }
}

void FindExprVisitor::visit(MapExpression *expr) {
    findExpr(expr);
    if (found_) return;
    for (const auto &pair : expr->items()) {
        const_cast<Expression *>(pair.second)->accept(this);
        if (found_) return;
    }
}

void FindExprVisitor::visit(ConstantExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visit(EdgePropertyExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visit(TagPropertyExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visit(InputPropertyExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visit(VariablePropertyExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visit(SourcePropertyExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visit(DestPropertyExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visit(EdgeSrcIdExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visit(EdgeTypeExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visit(EdgeRankExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visit(EdgeDstIdExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visit(UUIDExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visit(VariableExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visit(VersionedVariableExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visit(LabelExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visit(VertexExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visit(EdgeExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visitBinaryExpr(BinaryExpression *expr) {
    findExpr(expr);
    if (found_) return;
    expr->left()->accept(this);
    if (found_) return;
    expr->right()->accept(this);
}

void FindExprVisitor::findExpr(const Expression *expr) {
    found_ = exprs_.find(expr->kind()) != exprs_.cend();
    if (found_) {
        expr_ = expr;
    }
}

}   // namespace graph
}   // namespace nebula
