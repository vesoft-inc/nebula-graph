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

void CollectExprsVisitor::visitTypeCastingExpr(const TypeCastingExpression *expr) {
    collectExpr(expr);
    expr->operand()->accept(this);
}

void CollectExprsVisitor::visitUnaryExpr(const UnaryExpression *expr) {
    collectExpr(expr);
    expr->operand()->accept(this);
}

void CollectExprsVisitor::visitFunctionCallExpr(const FunctionCallExpression *expr) {
    collectExpr(expr);
    for (const auto &arg : expr->args()->args()) {
        arg->accept(this);
    }
}

void CollectExprsVisitor::visitListExpr(const ListExpression *expr) {
    collectExpr(expr);
    for (const auto &item : expr->items()) {
        item->accept(this);
    }
}

void CollectExprsVisitor::visitSetExpr(const SetExpression *expr) {
    collectExpr(expr);
    for (const auto &item : expr->items()) {
        item->accept(this);
    }
}

void CollectExprsVisitor::visitMapExpr(const MapExpression *expr) {
    collectExpr(expr);
    for (const auto &pair : expr->items()) {
        pair.second->accept(this);
    }
}

void CollectExprsVisitor::visitConstantExpr(const ConstantExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitEdgePropertyExpr(const EdgePropertyExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitTagPropertyExpr(const TagPropertyExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitInputPropertyExpr(const InputPropertyExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitVariablePropertyExpr(const VariablePropertyExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitSourcePropertyExpr(const SourcePropertyExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitDestPropertyExpr(const DestPropertyExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitEdgeSrcIdExpr(const EdgeSrcIdExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitEdgeTypeExpr(const EdgeTypeExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitEdgeRankExpr(const EdgeRankExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitEdgeDstIdExpr(const EdgeDstIdExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitUUIDExpr(const UUIDExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitVariableExpr(const VariableExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitVersionedVariableExpr(const VersionedVariableExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitLabelExpr(const LabelExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitSymbolPropertyExpr(const SymbolPropertyExpression *expr) {
    collectExpr(expr);
}

void CollectExprsVisitor::visitBinaryExpr(const BinaryExpression *expr) {
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
