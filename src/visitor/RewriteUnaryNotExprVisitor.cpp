/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/RewriteUnaryNotExprVisitor.h"

#include "context/QueryExpressionContext.h"

namespace nebula {
namespace graph {

void RewriteUnaryNotExprVisitor::visit(RelationalExpression *expr) {
    visitBinaryExpr(expr);
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
    // UNUSED(expr);
    // reducible_ = false;
}

void RewriteUnaryNotExprVisitor::visit(ConstantExpression* expr) {
    QueryExpressionContext ctx;
    auto val = expr->eval(ctx(nullptr));
    if (val.isBool()) {
        expr_.reset(expr->clone().release());
    }
    // UNUSED(expr);
    // reducible_ = false;
}

void RewriteUnaryNotExprVisitor::visit(UnaryExpression* expr) {
    if (isUnaryNotExpr(expr)) {
        auto operand = expr->operand();
        operand->accept(this);
        if (isUnaryNotExpr(expr_.get())) {   // reduce unary expr
            expr_ = reduce(expr);
            reducible_ = true;
            return;
        }
    } else {
        // reducible_ = false;
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
