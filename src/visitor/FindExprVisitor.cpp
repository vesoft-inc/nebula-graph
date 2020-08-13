/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/FindExprVisitor.h"

#include "common/expression/ConstantExpression.h"
#include "common/expression/ContainerExpression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/LabelExpression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/RelationalExpression.h"
#include "common/expression/SymbolPropertyExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "common/expression/UUIDExpression.h"
#include "common/expression/UnaryExpression.h"
#include "common/expression/VariableExpression.h"

namespace nebula {
namespace graph {

FindExprVisitor::FindExprVisitor(const std::unordered_set<Expression::Kind> &exprs)
    : exprs_(exprs) {
    DCHECK(!exprs.empty());
}

void FindExprVisitor::visitTypeCastingExpr(TypeCastingExpression *expr) {
    findExpr(expr);
    if (found_) return;
    findExpr(expr->operand());
    if (found_) return;
    expr->operand()->accept(this);
}

void FindExprVisitor::visitUnaryExpr(UnaryExpression *expr) {
    findExpr(expr);
    if (found_) return;
    findExpr(expr->operand());
    if (found_) return;
    expr->operand()->accept(this);
}

void FindExprVisitor::visitFunctionCallExpr(FunctionCallExpression *expr) {
    findExpr(expr);
    if (found_) return;
    for (const auto &arg : expr->args()->args()) {
        findExpr(arg.get());
        if (found_) break;
        arg->accept(this);
        if (found_) break;
    }
}

void FindExprVisitor::visitListExpr(ListExpression *expr) {
    findExpr(expr);
    if (found_) return;
    for (const auto &item : expr->items()) {
        findExpr(item);
        if (found_) return;
        const_cast<Expression *>(item)->accept(this);
        if (found_) return;
    }
}

void FindExprVisitor::visitSetExpr(SetExpression *expr) {
    findExpr(expr);
    if (found_) return;
    for (const auto &item : expr->items()) {
        findExpr(item);
        if (found_) return;
        const_cast<Expression *>(item)->accept(this);
        if (found_) return;
    }
}

void FindExprVisitor::visitMapExpr(MapExpression *expr) {
    findExpr(expr);
    if (found_) return;
    for (const auto &pair : expr->items()) {
        findExpr(pair.second);
        if (found_) return;
        const_cast<Expression *>(pair.second)->accept(this);
        if (found_) return;
    }
}

void FindExprVisitor::visitConstantExpr(ConstantExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visitEdgePropertyExpr(EdgePropertyExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visitTagPropertyExpr(TagPropertyExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visitInputPropertyExpr(InputPropertyExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visitVariablePropertyExpr(VariablePropertyExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visitSourcePropertyExpr(SourcePropertyExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visitDestPropertyExpr(DestPropertyExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visitEdgeSrcIdExpr(EdgeSrcIdExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visitEdgeTypeExpr(EdgeTypeExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visitEdgeRankExpr(EdgeRankExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visitEdgeDstIdExpr(EdgeDstIdExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visitUUIDExpr(UUIDExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visitVariableExpr(VariableExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visitVersionedVariableExpr(VersionedVariableExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visitLabelExpr(LabelExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visitSymbolPropertyExpr(SymbolPropertyExpression *expr) {
    findExpr(expr);
}

void FindExprVisitor::visitBinaryExpr(BinaryExpression *expr) {
    findExpr(expr);
    if (found_) return;
    findExpr(expr->left());
    if (found_) return;
    findExpr(expr->right());
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
