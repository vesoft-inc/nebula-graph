/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/RewriteUnaryNotExprVisitor.h"

#include "context/QueryExpressionContext.h"

namespace nebula {
namespace graph {

RewriteUnaryNotExprVisitor::RewriteUnaryNotExprVisitor(ObjectPool *objPool) : pool_(objPool) {}

void RewriteUnaryNotExprVisitor::visit(ConstantExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(UnaryExpression *expr) {
    auto operand = expr->operand();
    if (isUnaryNotExpr(expr)) {
        if (isUnaryNotExpr(operand)) {
            static_cast<UnaryExpression *>(operand)->operand()->accept(this);
            return;
        } else if (isRelExpr(operand)) {
            expr_ = pool_->add(reverseRelExpr(operand).release());
            return;
        }
    }
    operand->accept(this);
    expr->setOperand(expr_->clone().release());
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(TypeCastingExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(LabelExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(LabelAttributeExpression *expr) {
    expr_ = expr;
}

// binary expression
void RewriteUnaryNotExprVisitor::visit(ArithmeticExpression *expr) {
    visitBinaryExpr(expr);
}

// TODO(Aiee) reduce the combination of relational expr and unary expr
// e.g. !(a > b)  =>  (a <= b)
void RewriteUnaryNotExprVisitor::visit(RelationalExpression *expr) {
    visitBinaryExpr(expr);
}

void RewriteUnaryNotExprVisitor::visit(SubscriptExpression *expr) {
    visitBinaryExpr(expr);
}

void RewriteUnaryNotExprVisitor::visit(AttributeExpression *expr) {
    visitBinaryExpr(expr);
}

void RewriteUnaryNotExprVisitor::visit(LogicalExpression *expr) {
    auto &operands = expr->operands();
    for (auto i = 0u; i < operands.size(); i++) {
        operands[i]->accept(this);
        if (expr_) {
            expr->setOperand(i, expr_->clone().release());
        }
    }
    expr_ = expr;
}

// Rewrite Unary expresssion to Binary expression
void RewriteUnaryNotExprVisitor::visitBinaryExpr(BinaryExpression *expr) {
    expr->left()->accept(this);
    if (expr_) {
        expr->setLeft(expr_->clone().release());
    }
    expr->right()->accept(this);
    if (expr_) {
        expr->setRight(expr_->clone().release());
    }
    expr_ = expr;
}

// function call
void RewriteUnaryNotExprVisitor::visit(FunctionCallExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(AggregateExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(UUIDExpression *expr) {
    expr_ = expr;
}

// variable expression
void RewriteUnaryNotExprVisitor::visit(VariableExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(VersionedVariableExpression *expr) {
    expr_ = expr;
}

// container expression
void RewriteUnaryNotExprVisitor::visit(ListExpression *expr) {
    auto &items = expr->items();
    for (auto i = 0u; i < items.size(); ++i) {
        auto item = items[i].get();
        item->accept(this);
        if (expr_) {
            expr->setItem(i, expr_->clone());
        }
    }
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(SetExpression *expr) {
    auto &items = expr->items();
    for (auto i = 0u; i < items.size(); ++i) {
        auto item = items[i].get();
        item->accept(this);
        if (expr_) {
            expr->setItem(i, expr_->clone());
        }
    }
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(MapExpression *expr) {
    auto &items = expr->items();
    for (auto i = 0u; i < items.size(); ++i) {
        auto &pair = items[i];
        auto item = const_cast<Expression *>(pair.second.get());
        item->accept(this);
        if (expr_) {
            auto key = std::make_unique<std::string>(*pair.first);
            auto val = expr_->clone();
            expr->setItem(i, std::make_pair(std::move(key), std::move(val)));
        }
    }
    expr_ = expr;
}

// property Expression
void RewriteUnaryNotExprVisitor::visit(TagPropertyExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(EdgePropertyExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(InputPropertyExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(VariablePropertyExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(DestPropertyExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(SourcePropertyExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(EdgeSrcIdExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(EdgeTypeExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(EdgeRankExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(EdgeDstIdExpression *expr) {
    expr_ = expr;
}

// vertex/edge expression
void RewriteUnaryNotExprVisitor::visit(VertexExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(EdgeExpression *expr) {
    expr_ = expr;
}

// case expression
void RewriteUnaryNotExprVisitor::visit(CaseExpression *expr) {
    expr_ = expr;
}

// path build expression
void RewriteUnaryNotExprVisitor::visit(PathBuildExpression *expr) {
    expr_ = expr;
}

// column expression
void RewriteUnaryNotExprVisitor::visit(ColumnExpression *expr) {
    expr_ = expr;
}

// predicate expression
void RewriteUnaryNotExprVisitor::visit(PredicateExpression *expr) {
    expr_ = expr;
}

// list comprehension expression
void RewriteUnaryNotExprVisitor::visit(ListComprehensionExpression *expr) {
    expr_ = expr;
}

// reduce expression
void RewriteUnaryNotExprVisitor::visit(ReduceExpression *expr) {
    expr_ = expr;
}

// Reverese the type of the given relational expr
std::unique_ptr<Expression> RewriteUnaryNotExprVisitor::reverseRelExpr(Expression *expr) {
    auto left = static_cast<RelationalExpression *>(expr)->left();
    auto right = static_cast<RelationalExpression *>(expr)->right();
    auto reversedKind = getNegatedKind(expr->kind());

    return std::make_unique<RelationalExpression>(
        reversedKind, left->clone().release(), right->clone().release());
}

// Return the negation of the given relational kind
Expression::Kind RewriteUnaryNotExprVisitor::getNegatedKind(const Expression::Kind kind) {
    switch (kind) {
        case Expression::Kind::kRelEQ:
            return Expression::Kind::kRelNE;
            break;
        case Expression::Kind::kRelNE:
            return Expression::Kind::kRelEQ;
            break;
        case Expression::Kind::kRelLT:
            return Expression::Kind::kRelGE;
            break;
        case Expression::Kind::kRelLE:
            return Expression::Kind::kRelGT;
            break;
        case Expression::Kind::kRelGT:
            return Expression::Kind::kRelLE;
            break;
        case Expression::Kind::kRelGE:
            return Expression::Kind::kRelLT;
            break;
        case Expression::Kind::kRelIn:
            return Expression::Kind::kRelNotIn;
            break;
        case Expression::Kind::kRelNotIn:
            return Expression::Kind::kRelIn;
            break;
        case Expression::Kind::kContains:
            return Expression::Kind::kNotContains;
            break;
        case Expression::Kind::kNotContains:
            return Expression::Kind::kContains;
            break;
        case Expression::Kind::kStartsWith:
            return Expression::Kind::kNotStartsWith;
            break;
        case Expression::Kind::kNotStartsWith:
            return Expression::Kind::kStartsWith;
            break;
        case Expression::Kind::kEndsWith:
            return Expression::Kind::kNotEndsWith;
            break;
        case Expression::Kind::kNotEndsWith:
            return Expression::Kind::kEndsWith;
            break;
        default:
            LOG(FATAL) << "Invalid relational expression kind: " << static_cast<uint8_t>(kind);
            break;
    }
}

bool RewriteUnaryNotExprVisitor::isRelExpr(const Expression *expr) {
 //    expr->kind() == Expression::Kind::kRelREG is not supported
    return expr->kind() == Expression::Kind::kRelEQ ||
           expr->kind() == Expression::Kind::kRelNE ||
           expr->kind() == Expression::Kind::kRelLT ||
           expr->kind() == Expression::Kind::kRelLE ||
           expr->kind() == Expression::Kind::kRelGT ||
           expr->kind() == Expression::Kind::kRelGE ||
           expr->kind() == Expression::Kind::kRelIn ||
           expr->kind() == Expression::Kind::kRelNotIn ||
           expr->kind() == Expression::Kind::kContains ||
           expr->kind() == Expression::Kind::kNotContains ||
           expr->kind() == Expression::Kind::kStartsWith ||
           expr->kind() == Expression::Kind::kNotStartsWith ||
           expr->kind() == Expression::Kind::kEndsWith ||
           expr->kind() == Expression::Kind::kNotEndsWith;
}

bool RewriteUnaryNotExprVisitor::isUnaryNotExpr(const Expression *expr) {
    return expr->kind() == Expression::Kind::kUnaryNot;
}

}   // namespace graph
}   // namespace nebula
