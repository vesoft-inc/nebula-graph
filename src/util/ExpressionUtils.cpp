/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "util/ExpressionUtils.h"

#include <memory>

#include "common/expression/PropertyExpression.h"
#include "visitor/FoldConstantExprVisitor.h"

namespace nebula {
namespace graph {

std::unique_ptr<Expression> ExpressionUtils::foldConstantExpr(const Expression *expr) {
    auto newExpr = expr->clone();
    FoldConstantExprVisitor visitor;
    newExpr->accept(&visitor);
    if (visitor.canBeFolded()) {
        return std::unique_ptr<Expression>(visitor.fold(newExpr.get()));
    }
    return newExpr;
}

Expression* ExpressionUtils::pullAnds(Expression *expr) {
    DCHECK(expr->kind() == Expression::Kind::kLogicalAnd);
    auto *logic = static_cast<LogicalExpression*>(expr);
    std::vector<std::unique_ptr<Expression>> operands;
    pullAndsImpl(logic, operands);
    logic->setOperands(std::move(operands));
    return logic;
}

Expression* ExpressionUtils::pullOrs(Expression *expr) {
    DCHECK(expr->kind() == Expression::Kind::kLogicalOr);
    auto *logic = static_cast<LogicalExpression*>(expr);
    std::vector<std::unique_ptr<Expression>> operands;
    pullOrsImpl(logic, operands);
    logic->setOperands(std::move(operands));
    return logic;
}

void
ExpressionUtils::pullAndsImpl(LogicalExpression *expr,
                              std::vector<std::unique_ptr<Expression>> &operands) {
    for (auto &operand : expr->operands()) {
        if (operand->kind() != Expression::Kind::kLogicalAnd) {
            operands.emplace_back(std::move(operand));
            continue;
        }
        pullAndsImpl(static_cast<LogicalExpression*>(operand.get()), operands);
    }
}

void
ExpressionUtils::pullOrsImpl(LogicalExpression *expr,
                             std::vector<std::unique_ptr<Expression>> &operands) {
    for (auto &operand : expr->operands()) {
        if (operand->kind() != Expression::Kind::kLogicalOr) {
            operands.emplace_back(std::move(operand));
            continue;
        }
        pullOrsImpl(static_cast<LogicalExpression*>(operand.get()), operands);
    }
}

VariablePropertyExpression *ExpressionUtils::newVarPropExpr(const std::string &prop,
                                                            const std::string &var) {
    return new VariablePropertyExpression(new std::string(var), new std::string(prop));
}

std::unique_ptr<InputPropertyExpression> ExpressionUtils::inputPropExpr(const std::string &prop) {
    return std::make_unique<InputPropertyExpression>(new std::string(prop));
}

std::unique_ptr<Expression>
ExpressionUtils::pushOrs(const std::vector<std::unique_ptr<Expression>>& rels) {
    return pushImpl(Expression::Kind::kLogicalOr, rels);
}

std::unique_ptr<Expression>
ExpressionUtils::pushAnds(const std::vector<std::unique_ptr<Expression>>& rels) {
    return pushImpl(Expression::Kind::kLogicalAnd, rels);
}

std::unique_ptr<Expression>
ExpressionUtils::pushImpl(Expression::Kind kind,
                          const std::vector<std::unique_ptr<Expression>>& rels) {
    DCHECK_GT(rels.size(), 1);
    DCHECK(kind == Expression::Kind::kLogicalOr || kind == Expression::Kind::kLogicalAnd);
    auto root = std::make_unique<LogicalExpression>(kind);
    root->addOperand(rels[0]->clone().release());
    root->addOperand(rels[1]->clone().release());
    for (size_t i = 2; i < rels.size(); i++) {
        auto l = std::make_unique<LogicalExpression>(kind);
        l->addOperand(root->clone().release());
        l->addOperand(rels[i]->clone().release());
        root = std::move(l);
    }
    return root;
}

std::unique_ptr<Expression> ExpressionUtils::expandExpr(const Expression *expr) {
    auto kind = expr->kind();
    std::vector<std::unique_ptr<Expression>> target;
    switch (kind) {
        case Expression::Kind::kLogicalOr: {
            const auto *logic = static_cast<const LogicalExpression*>(expr);
            for (const auto& e : logic->operands()) {
                if (e->kind() == Expression::Kind::kLogicalAnd) {
                    target.emplace_back(expandImplAnd(e.get()));
                } else {
                    target.emplace_back(expandExpr(e.get()));
                }
            }
            break;
        }
        case Expression::Kind::kLogicalAnd: {
            target.emplace_back(expandImplAnd(expr));
            break;
        }
        default: {
            return expr->clone();
        }
    }
    DCHECK_GT(target.size(), 0);
    if (target.size() == 1) {
        if (target[0]->kind() == Expression::Kind::kLogicalAnd) {
            const auto *logic = static_cast<const LogicalExpression*>(target[0].get());
            const auto& ops = logic->operands();
            DCHECK_EQ(ops.size(), 2);
            if (ops[0]->kind() == Expression::Kind::kLogicalOr ||
                ops[1]->kind() == Expression::Kind::kLogicalOr) {
                return expandExpr(target[0].get());
            }
        }
        return std::move(target[0]);
    }
    return pushImpl(kind, target);
}

std::unique_ptr<Expression> ExpressionUtils::expandImplAnd(const Expression *expr) {
    DCHECK(expr->kind() == Expression::Kind::kLogicalAnd);
    const auto *logic = static_cast<const LogicalExpression*>(expr);
    DCHECK_EQ(logic->operands().size(), 2);
    std::vector<std::unique_ptr<Expression>> subL;
    auto& ops = logic->operands();
    if (ops[0]->kind() == Expression::Kind::kLogicalOr) {
        auto target = expandImplOr(ops[0].get());
        for (const auto& e : target) {
            subL.emplace_back(e->clone().release());
        }
    } else {
        subL.emplace_back(expandExpr(std::move(ops[0]).get()));
    }
    std::vector<std::unique_ptr<Expression>> subR;
    if (ops[1]->kind() == Expression::Kind::kLogicalOr) {
        auto target = expandImplOr(ops[1].get());
        for (const auto& e : target) {
            subR.emplace_back(e->clone().release());
        }
    } else {
        subR.emplace_back(expandExpr(std::move(ops[1]).get()));
    }

    DCHECK_GT(subL.size(), 0);
    DCHECK_GT(subR.size(), 0);
    std::vector<std::unique_ptr<Expression>> target;
    for (auto& le : subL) {
        for (auto& re : subR) {
            auto l = std::make_unique<LogicalExpression>(Expression::Kind::kLogicalAnd);
            l->addOperand(le->clone().release());
            l->addOperand(re->clone().release());
            target.emplace_back(std::move(l));
        }
    }
    DCHECK_GT(target.size(), 0);
    if (target.size() == 1) {
        return std::move(target[0]);
    }
    return pushImpl(Expression::Kind::kLogicalOr, target);
}

std::vector<std::unique_ptr<Expression>> ExpressionUtils::expandImplOr(const Expression *expr) {
    DCHECK(expr->kind() == Expression::Kind::kLogicalOr);
    const auto *logic = static_cast<const LogicalExpression*>(expr);
    std::vector<std::unique_ptr<Expression>> exprs;
    auto& ops = logic->operands();
    for (const auto& op : ops) {
        if (op->kind() == Expression::Kind::kLogicalOr) {
            auto target = expandImplOr(op.get());
            for (const auto& e : target) {
                exprs.emplace_back(e->clone().release());
            }
        } else {
            exprs.emplace_back(op->clone().release());
        }
    }
    return exprs;
}
}   // namespace graph
}   // namespace nebula
