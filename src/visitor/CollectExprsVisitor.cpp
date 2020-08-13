/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/CollectExprsVisitor.h"

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

CollectExprsVisitor::CollectExprsVisitor(const std::unordered_set<Expression::Kind> &exprTypes)
    : exprTypes_(exprTypes) {}

void CollectExprsVisitor::visitTypeCastingExpr(TypeCastingExpression *expr) {
    collectExpr(expr);
    expr->operand()->accept(this);
}

void CollectExprsVisitor::visitUnaryExpr(UnaryExpression *expr) {
    collectExpr(expr);
    expr->operand()->accept(this);
}

void CollectExprsVisitor::visitFunctionCallExpr(FunctionCallExpression *expr) {
    collectExpr(expr);
    for (const auto &arg : expr->args()->args()) {
        arg->accept(this);
    }
}

void CollectExprsVisitor::visitListExpr(ListExpression *expr) {
    collectExpr(expr);
    for (auto item : expr->items()) {
        const_cast<Expression *>(item)->accept(this);
    }
}

void CollectExprsVisitor::visitSetExpr(SetExpression *expr) {
    collectExpr(expr);
    for (auto item : expr->items()) {
        const_cast<Expression *>(item)->accept(this);
    }
}

void CollectExprsVisitor::visitMapExpr(MapExpression *expr) {
    collectExpr(expr);
    for (const auto &pair : expr->items()) {
        const_cast<Expression *>(pair.second)->accept(this);
    }
}

void CollectExprsVisitor::visitConstantExpr(ConstantExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitEdgePropertyExpr(EdgePropertyExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitTagPropertyExpr(TagPropertyExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitInputPropertyExpr(InputPropertyExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitVariablePropertyExpr(VariablePropertyExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitSourcePropertyExpr(SourcePropertyExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitDestPropertyExpr(DestPropertyExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitEdgeSrcIdExpr(EdgeSrcIdExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitEdgeTypeExpr(EdgeTypeExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitEdgeRankExpr(EdgeRankExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitEdgeDstIdExpr(EdgeDstIdExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitUUIDExpr(UUIDExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitVariableExpr(VariableExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitVersionedVariableExpr(VersionedVariableExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitLabelExpr(LabelExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitSymbolPropertyExpr(SymbolPropertyExpression *expr) {
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
