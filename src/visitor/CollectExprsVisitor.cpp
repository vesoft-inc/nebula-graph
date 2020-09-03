/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/CollectExprsVisitor.h"

namespace nebula {
namespace graph {

CollectExprsVisitor::CollectExprsVisitor(const std::unordered_set<Expression::Kind> &exprTypes)
    : exprTypes_(exprTypes) {}

void CollectExprsVisitor::visit(TypeCastingExpression *expr) {
    collectExpr(expr);
    expr->operand()->accept(this);
}

void CollectExprsVisitor::visit(UnaryExpression *expr) {
    collectExpr(expr);
    expr->operand()->accept(this);
}

void CollectExprsVisitor::visit(FunctionCallExpression *expr) {
    collectExpr(expr);
    for (const auto &arg : expr->args()->args()) {
        arg->accept(this);
    }
}

void CollectExprsVisitor::visit(ListExpression *expr) {
    collectExpr(expr);
    for (auto item : expr->items()) {
        const_cast<Expression *>(item)->accept(this);
    }
}

void CollectExprsVisitor::visit(SetExpression *expr) {
    collectExpr(expr);
    for (auto item : expr->items()) {
        const_cast<Expression *>(item)->accept(this);
    }
}

void CollectExprsVisitor::visit(MapExpression *expr) {
    collectExpr(expr);
    for (const auto &pair : expr->items()) {
        const_cast<Expression *>(pair.second)->accept(this);
    }
}

void CollectExprsVisitor::visit(ConstantExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visit(EdgePropertyExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visit(TagPropertyExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visit(InputPropertyExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visit(VariablePropertyExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visit(SourcePropertyExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visit(DestPropertyExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visit(EdgeSrcIdExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visit(EdgeTypeExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visit(EdgeRankExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visit(EdgeDstIdExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visit(UUIDExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visit(VariableExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visit(VersionedVariableExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visit(LabelExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visit(VertexExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visit(EdgeExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitBinaryExpr(BinaryExpression *expr) {
    collectExpr(expr);
    expr->left()->accept(this);
    expr->right()->accept(this);
}

void CollectExprsVisitor::collectExpr(const Expression *expr) {
    if (exprTypes_.find(expr->kind()) != exprTypes_.cend()) {
        exprs_.push_back(expr);
    }
}

}   // namespace graph
}   // namespace nebula
