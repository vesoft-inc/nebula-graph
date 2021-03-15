/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/RewriteUnaryNotExprVisitor.h"

#include "context/QueryExpressionContext.h"

namespace nebula {
namespace graph {

void RewriteUnaryNotExprVisitor::visit(ConstantExpression *expr) {
    expr_.reset(expr->clone().release());
}

void RewriteUnaryNotExprVisitor::visit(UnaryExpression *expr) {
    if (isUnaryNotExpr(expr)) {
        auto operand = expr->operand();
        operand->accept(this);
        if (isUnaryNotExpr(expr_.get())) {   // reduce unary expr
            if (!reduced_) {
                expr_ = static_cast<UnaryExpression *>(expr_.get())->operand()->clone();
                reduced_ = true;
                return;
            }
            expr_ = reduce(expr);
            reduced_ = true;
            return;
        }
        if (reduced_) {   // odd # of unaryNot
            auto exprCopy = expr_->clone();
            expr_.reset(new UnaryExpression(Expression::Kind::kUnaryNot, exprCopy.release()));
            reduced_ = false;
            return;
        }
    }
    expr_.reset(expr->clone().release());
}

void RewriteUnaryNotExprVisitor::visit(TypeCastingExpression *expr) {
    expr_.reset(expr->clone().release());
}

void RewriteUnaryNotExprVisitor::visit(LabelExpression *expr) {
    expr_.reset(expr->clone().release());
}

void RewriteUnaryNotExprVisitor::visit(LabelAttributeExpression *expr) {
    expr_.reset(expr->clone().release());
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
            expr->setOperand(i, expr_.release());
        }
    }
    expr_.reset(expr->clone().release());
}

// Rewrite Unary expresssion to Binary expression
void RewriteUnaryNotExprVisitor::visitBinaryExpr(BinaryExpression *expr) {
    expr->left()->accept(this);
    if (expr_) {
        expr->setLeft(expr_.release());
    }
    expr->right()->accept(this);
    if (expr_) {
        expr->setRight(expr_.release());
    }
    expr_.reset(expr->clone().release());
}

// function call
void RewriteUnaryNotExprVisitor::visit(FunctionCallExpression *expr) {
    expr_.reset(expr->clone().release());
}

void RewriteUnaryNotExprVisitor::visit(AggregateExpression *expr) {
    expr_.reset(expr->clone().release());
}

void RewriteUnaryNotExprVisitor::visit(UUIDExpression *expr) {
    expr_.reset(expr->clone().release());
}

// variable expression
void RewriteUnaryNotExprVisitor::visit(VariableExpression *expr) {
    expr_.reset(expr->clone().release());
}

void RewriteUnaryNotExprVisitor::visit(VersionedVariableExpression *expr) {
    expr_.reset(expr->clone().release());
}

// container expression
void RewriteUnaryNotExprVisitor::visit(ListExpression *expr) {
    expr_.reset(expr->clone().release());
}

void RewriteUnaryNotExprVisitor::visit(SetExpression *expr) {
    expr_.reset(expr->clone().release());
}

void RewriteUnaryNotExprVisitor::visit(MapExpression *expr) {
    expr_.reset(expr->clone().release());
}

// property Expression
void RewriteUnaryNotExprVisitor::visit(TagPropertyExpression *expr) {
    expr_.reset(expr->clone().release());
}

void RewriteUnaryNotExprVisitor::visit(EdgePropertyExpression *expr) {
    expr_.reset(expr->clone().release());
}

void RewriteUnaryNotExprVisitor::visit(InputPropertyExpression *expr) {
    expr_.reset(expr->clone().release());
}

void RewriteUnaryNotExprVisitor::visit(VariablePropertyExpression *expr) {
    expr_.reset(expr->clone().release());
}

void RewriteUnaryNotExprVisitor::visit(DestPropertyExpression *expr) {
    expr_.reset(expr->clone().release());
}

void RewriteUnaryNotExprVisitor::visit(SourcePropertyExpression *expr) {
    expr_.reset(expr->clone().release());
}

void RewriteUnaryNotExprVisitor::visit(EdgeSrcIdExpression *expr) {
    expr_.reset(expr->clone().release());
}

void RewriteUnaryNotExprVisitor::visit(EdgeTypeExpression *expr) {
    expr_.reset(expr->clone().release());
}

void RewriteUnaryNotExprVisitor::visit(EdgeRankExpression *expr) {
    expr_.reset(expr->clone().release());
}

void RewriteUnaryNotExprVisitor::visit(EdgeDstIdExpression *expr) {
    expr_.reset(expr->clone().release());
}

// vertex/edge expression
void RewriteUnaryNotExprVisitor::visit(VertexExpression *expr) {
    expr_.reset(expr->clone().release());
}

void RewriteUnaryNotExprVisitor::visit(EdgeExpression *expr) {
    expr_.reset(expr->clone().release());
}

// case expression
void RewriteUnaryNotExprVisitor::visit(CaseExpression *expr) {
    expr_.reset(expr->clone().release());
}

// path build expression
void RewriteUnaryNotExprVisitor::visit(PathBuildExpression *expr) {
    expr_.reset(expr->clone().release());
}

// column expression
void RewriteUnaryNotExprVisitor::visit(ColumnExpression *expr) {
    expr_.reset(expr->clone().release());
}

// predicate expression
void RewriteUnaryNotExprVisitor::visit(PredicateExpression *expr) {
    expr_.reset(expr->clone().release());
}

// list comprehension expression
void RewriteUnaryNotExprVisitor::visit(ListComprehensionExpression *expr) {
    expr_.reset(expr->clone().release());
}

// reduce expression
void RewriteUnaryNotExprVisitor::visit(ReduceExpression *expr) {
    expr_.reset(expr->clone().release());
}

std::unique_ptr<Expression> RewriteUnaryNotExprVisitor::reduce(UnaryExpression *expr) {
    auto reducedExpr = static_cast<UnaryExpression *>(expr->operand())->operand();
    return reducedExpr->clone();
}

bool RewriteUnaryNotExprVisitor::isUnaryNotExpr(const Expression *expr) {
    return expr->kind() == Expression::Kind::kUnaryNot;
}

}   // namespace graph
}   // namespace nebula
