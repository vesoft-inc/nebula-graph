/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/RewriteUnaryNotExprVisitor.h"

#include "context/QueryExpressionContext.h"

namespace nebula {
namespace graph {

// TODO(Aiee) reduce the combination of relational expr and unary expr
// e.g. !(a > b)  =>  (a <= b)
void RewriteUnaryNotExprVisitor::visit(RelationalExpression* expr) {
    visitBinaryExpr(expr);
}

void RewriteUnaryNotExprVisitor::visit(LogicalExpression* expr) {
    auto& operands = expr->operands();
    for (auto i = 0u; i < operands.size(); i++) {
        operands[i]->accept(this);
        if (expr_) {
            expr->setOperand(i, expr_.release());
        }
    }
}

// Rewrite Unary expresssion to Binary expression
void RewriteUnaryNotExprVisitor::visitBinaryExpr(BinaryExpression* expr) {
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

void RewriteUnaryNotExprVisitor::visit(ConstantExpression* expr) {
    QueryExpressionContext ctx;
    auto val = expr->eval(ctx(nullptr));
    if (val.isBool()) {
        expr_.reset(expr->clone().release());
    }
}

void RewriteUnaryNotExprVisitor::visit(UnaryExpression* expr) {
    if (isUnaryNotExpr(expr)) {
        auto operand = expr->operand();
        operand->accept(this);
        if (isUnaryNotExpr(expr_.get())) {   // reduce unary expr
            if (!reduced_) {
                auto curExpr_ = expr_->clone().release();
                expr_ = static_cast<UnaryExpression*>(curExpr_)->operand()->clone();
                reduced_ = true;
                return;
            }
            expr_ = reduce(expr);
            reduced_ = true;
            return;
        }
        if (reduced_) {   // odd # of unaryNot
            auto exprCopy = expr_->clone().release();
            expr_.reset(new UnaryExpression(Expression::Kind::kUnaryNot, exprCopy));
            reduced_ = false;
            return;
        }
    }
    expr_.reset(expr->clone().release());
}

std::unique_ptr<Expression> RewriteUnaryNotExprVisitor::reduce(UnaryExpression* expr) {
    auto reducedExpr = static_cast<UnaryExpression*>(expr->operand())->operand();
    return reducedExpr->clone();
}

bool RewriteUnaryNotExprVisitor::isUnaryNotExpr(const Expression* expr) {
    return expr->kind() == Expression::Kind::kUnaryNot;
}

}   // namespace graph
}   // namespace nebula
