/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/RewriteRelExprVisitor.h"

#include "context/QueryExpressionContext.h"
#include "util/ExpressionUtils.h"
namespace nebula {
namespace graph {

RewriteRelationalExprVisitor::RewriteRelationalExprVisitor(ObjectPool *objPool) : pool_(objPool) {}

void RewriteRelationalExprVisitor::visit(ConstantExpression *expr) {
    expr_ = expr;
}

void RewriteRelationalExprVisitor::visit(UnaryExpression *expr) {
    expr_ = expr;
}

void RewriteRelationalExprVisitor::visit(TypeCastingExpression *expr) {
    expr_ = expr;
}

void RewriteRelationalExprVisitor::visit(LabelExpression *expr) {
    expr_ = expr;
}

void RewriteRelationalExprVisitor::visit(LabelAttributeExpression *expr) {
    expr_ = expr;
}

// binary expression
void RewriteRelationalExprVisitor::visit(ArithmeticExpression *expr) {
    visitBinaryExpr(expr);
}

void RewriteRelationalExprVisitor::visit(RelationalExpression *expr) {
    auto left = expr->left();
    auto right = expr->right();
    // if(left->kind()==Expression::Kind::kLabelAttribute)
    if(isArithmeticExpr(left)) {
        auto leftOperand = static_cast<ArithmeticExpression *>(left)->left();
        auto rightOperand = static_cast<ArithmeticExpression *>(left)->right();
        auto arithmType = static_cast<ArithmeticExpression *>(left)->kind();
        auto negateType = negateArithmeticType(arithmType);
        if (leftOperand->kind() == Expression::Kind::kLabelAttribute ||
            leftOperand->kind() == Expression::Kind::kAttribute) {
            // Move evaluableExpr to the other side
            if (ExpressionUtils::isEvaluableExpr(rightOperand)) {
                auto newExpr =
                    std::make_unique<ArithmeticExpression>(negateType, right, rightOperand);
                expr->setLeft(leftOperand);
                expr->setRight(newExpr.release());
            }
        }
        // DCHECK()
    }
    expr_ = expr;
}

void RewriteRelationalExprVisitor::visit(SubscriptExpression *expr) {
    visitBinaryExpr(expr);
}

void RewriteRelationalExprVisitor::visit(AttributeExpression *expr) {
    visitBinaryExpr(expr);
}

void RewriteRelationalExprVisitor::visit(LogicalExpression *expr) {
    auto &operands = expr->operands();
    for (auto i = 0u; i < operands.size(); i++) {
        operands[i]->accept(this);
        if (expr_ != operands[i].get()) {
            expr->setOperand(i, expr_->clone().release());
        }
    }
    expr_ = expr;
}

// Rewrite Unary expresssion to Binary expression
void RewriteRelationalExprVisitor::visitBinaryExpr(BinaryExpression *expr) {
    expr->left()->accept(this);
    if (expr_ != expr->left()) {
        expr->setLeft(expr_->clone().release());
    }
    expr->right()->accept(this);
    if (expr_ != expr->right()) {
        expr->setRight(expr_->clone().release());
    }
    expr_ = expr;
}

// function call
void RewriteRelationalExprVisitor::visit(FunctionCallExpression *expr) {
    expr_ = expr;
}

void RewriteRelationalExprVisitor::visit(AggregateExpression *expr) {
    expr_ = expr;
}

void RewriteRelationalExprVisitor::visit(UUIDExpression *expr) {
    expr_ = expr;
}

// variable expression
void RewriteRelationalExprVisitor::visit(VariableExpression *expr) {
    expr_ = expr;
}

void RewriteRelationalExprVisitor::visit(VersionedVariableExpression *expr) {
    expr_ = expr;
}

// container expression
void RewriteRelationalExprVisitor::visit(ListExpression *expr) {
    auto &items = expr->items();
    for (auto i = 0u; i < items.size(); ++i) {
        auto item = items[i].get();
        item->accept(this);
        if (expr_ != item) {
            expr->setItem(i, expr_->clone());
        }
    }
    expr_ = expr;
}

void RewriteRelationalExprVisitor::visit(SetExpression *expr) {
    auto &items = expr->items();
    for (auto i = 0u; i < items.size(); ++i) {
        auto item = items[i].get();
        item->accept(this);
        if (expr_ != item) {
            expr->setItem(i, expr_->clone());
        }
    }
    expr_ = expr;
}

void RewriteRelationalExprVisitor::visit(MapExpression *expr) {
    auto &items = expr->items();
    for (auto i = 0u; i < items.size(); ++i) {
        auto &pair = items[i];
        auto item = const_cast<Expression *>(pair.second.get());
        item->accept(this);
        if (expr_ != item) {
            auto key = std::make_unique<std::string>(*pair.first);
            auto val = expr_->clone();
            expr->setItem(i, std::make_pair(std::move(key), std::move(val)));
        }
    }
    expr_ = expr;
}

// property Expression
void RewriteRelationalExprVisitor::visit(TagPropertyExpression *expr) {
    expr_ = expr;
}

void RewriteRelationalExprVisitor::visit(EdgePropertyExpression *expr) {
    expr_ = expr;
}

void RewriteRelationalExprVisitor::visit(InputPropertyExpression *expr) {
    expr_ = expr;
}

void RewriteRelationalExprVisitor::visit(VariablePropertyExpression *expr) {
    expr_ = expr;
}

void RewriteRelationalExprVisitor::visit(DestPropertyExpression *expr) {
    expr_ = expr;
}

void RewriteRelationalExprVisitor::visit(SourcePropertyExpression *expr) {
    expr_ = expr;
}

void RewriteRelationalExprVisitor::visit(EdgeSrcIdExpression *expr) {
    expr_ = expr;
}

void RewriteRelationalExprVisitor::visit(EdgeTypeExpression *expr) {
    expr_ = expr;
}

void RewriteRelationalExprVisitor::visit(EdgeRankExpression *expr) {
    expr_ = expr;
}

void RewriteRelationalExprVisitor::visit(EdgeDstIdExpression *expr) {
    expr_ = expr;
}

// vertex/edge expression
void RewriteRelationalExprVisitor::visit(VertexExpression *expr) {
    expr_ = expr;
}

void RewriteRelationalExprVisitor::visit(EdgeExpression *expr) {
    expr_ = expr;
}

// case expression
void RewriteRelationalExprVisitor::visit(CaseExpression *expr) {
    expr_ = expr;
}

// path build expression
void RewriteRelationalExprVisitor::visit(PathBuildExpression *expr) {
    expr_ = expr;
}

// column expression
void RewriteRelationalExprVisitor::visit(ColumnExpression *expr) {
    expr_ = expr;
}

// predicate expression
void RewriteRelationalExprVisitor::visit(PredicateExpression *expr) {
    expr_ = expr;
}

// list comprehension expression
void RewriteRelationalExprVisitor::visit(ListComprehensionExpression *expr) {
    expr_ = expr;
}

// reduce expression
void RewriteRelationalExprVisitor::visit(ReduceExpression *expr) {
    expr_ = expr;
}

}   // namespace graph
}   // namespace nebula
