/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/function/FunctionManager.h"

#include "visitor/FoldConstantExprVisitor.h"

#include "context/QueryExpressionContext.h"

namespace nebula {
namespace graph {

void FoldConstantExprVisitor::visit(ConstantExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = true;
}

void FoldConstantExprVisitor::visit(UnaryExpression *expr) {
    if (!isConstant(expr->operand())) {
        expr->operand()->accept(this);
        if (canBeFolded_) {
            expr->setOperand(fold(expr->operand()));
        }
    }
}

void FoldConstantExprVisitor::visit(TypeCastingExpression *expr) {
    if (!isConstant(expr->operand())) {
        expr->operand()->accept(this);
        if (canBeFolded_) {
            expr->setOperand(fold(expr->operand()));
        }
    }
}

void FoldConstantExprVisitor::visit(LabelExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(LabelAttributeExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

// binary expression
void FoldConstantExprVisitor::visit(ArithmeticExpression *expr) {
    visitBinaryExpr(expr);
}

void FoldConstantExprVisitor::visit(RelationalExpression *expr) {
    visitBinaryExpr(expr);
}

void FoldConstantExprVisitor::visit(SubscriptExpression *expr) {
    visitBinaryExpr(expr);
}

void FoldConstantExprVisitor::visit(AttributeExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(LogicalExpression *expr) {
    auto &operands = expr->operands();
    auto foldable = true;
    // auto shortCircuit = false;
    for (auto i = 0u; i < operands.size(); i++) {
        auto *operand = operands[i].get();
        operand->accept(this);
        if (canBeFolded_) {
            auto *newExpr = fold(operand);
            expr->setOperand(i, newExpr);
            /*
            if (newExpr->value().isBool()) {
                auto value = newExpr->value().getBool();
                if ((value && expr->kind() == Expression::Kind::kLogicalOr) ||
                        (!value && expr->kind() == Expression::Kind::kLogicalAnd)) {
                    shortCircuit = true;
                    break;
                }
            }
            */
        } else {
            foldable = false;
        }
    }
    // canBeFolded_ = foldable || shortCircuit;
    canBeFolded_ = foldable;
}

// function call
void FoldConstantExprVisitor::visit(FunctionCallExpression *expr) {
    bool canBeFolded = true;
    for (auto &arg : expr->args()->args()) {
        if (!isConstant(arg.get())) {
            arg->accept(this);
            if (canBeFolded_) {
                arg.reset(fold(arg.get()));
            } else {
                canBeFolded = false;
            }
        }
    }
    auto result = FunctionManager::getIsPure(*expr->name(), expr->args()->args().size());
    if (!result.ok()) {
        canBeFolded = false;
    } else if (!result.value()) {
        // stateful so can't fold
        canBeFolded = false;
    }
    canBeFolded_ = canBeFolded;
}

void FoldConstantExprVisitor::visit(AggregateExpression *expr) {
    // TODO : impl AggExpr foldConstantExprVisitor
    if (!isConstant(expr->arg())) {
        expr->arg()->accept(this);
        if (canBeFolded_) {
            expr->setArg(fold(expr->arg()));
        }
    }
}

void FoldConstantExprVisitor::visit(UUIDExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

// variable expression
void FoldConstantExprVisitor::visit(VariableExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(VersionedVariableExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

// container expression
void FoldConstantExprVisitor::visit(ListExpression *expr) {
    auto &items = expr->items();
    bool canBeFolded = true;
    for (size_t i = 0; i < items.size(); ++i) {
        auto item = items[i].get();
        if (isConstant(item)) {
            continue;
        }
        item->accept(this);
        if (canBeFolded_) {
            expr->setItem(i, std::unique_ptr<Expression>{fold(item)});
        } else {
            canBeFolded = false;
        }
    }
    canBeFolded_ = canBeFolded;
}

void FoldConstantExprVisitor::visit(SetExpression *expr) {
    auto &items = expr->items();
    bool canBeFolded = true;
    for (size_t i = 0; i < items.size(); ++i) {
        auto item = items[i].get();
        if (isConstant(item)) {
            continue;
        }
        item->accept(this);
        if (canBeFolded_) {
            expr->setItem(i, std::unique_ptr<Expression>{fold(item)});
        } else {
            canBeFolded = false;
        }
    }
    canBeFolded_ = canBeFolded;
}

void FoldConstantExprVisitor::visit(MapExpression *expr) {
    auto &items = expr->items();
    bool canBeFolded = true;
    for (size_t i = 0; i < items.size(); ++i) {
        auto &pair = items[i];
        auto item = const_cast<Expression *>(pair.second.get());
        if (isConstant(item)) {
            continue;
        }
        item->accept(this);
        if (canBeFolded_) {
            auto key = std::make_unique<std::string>(*pair.first);
            auto val = std::unique_ptr<Expression>(fold(item));
            expr->setItem(i, std::make_pair(std::move(key), std::move(val)));
        } else {
            canBeFolded = false;
        }
    }
    canBeFolded_ = canBeFolded;
}

// case Expression
void FoldConstantExprVisitor::visit(CaseExpression *expr) {
    bool canBeFolded = true;
    if (expr->hasCondition() && !isConstant(expr->condition())) {
        expr->condition()->accept(this);
        if (canBeFolded_) {
            expr->setCondition(fold(expr->condition()));
        } else {
            canBeFolded = false;
        }
    }
    if (expr->hasDefault() && !isConstant(expr->defaultResult())) {
        expr->defaultResult()->accept(this);
        if (canBeFolded_) {
            expr->setDefault(fold(expr->defaultResult()));
        } else {
            canBeFolded = false;
        }
    }
    auto &cases = expr->cases();
    for (size_t i = 0; i < cases.size(); ++i) {
        auto when = cases[i].when.get();
        auto then = cases[i].then.get();
        if (!isConstant(when)) {
            when->accept(this);
            if (canBeFolded_) {
                expr->setWhen(i, fold(when));
            } else {
                canBeFolded = false;
            }
        }
        if (!isConstant(then)) {
            then->accept(this);
            if (canBeFolded_) {
                expr->setThen(i, fold(then));
            } else {
                canBeFolded = false;
            }
        }
    }
    canBeFolded_ = canBeFolded;
}

// property Expression
void FoldConstantExprVisitor::visit(TagPropertyExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(EdgePropertyExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(InputPropertyExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(VariablePropertyExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(DestPropertyExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(SourcePropertyExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(EdgeSrcIdExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(EdgeTypeExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(EdgeRankExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(EdgeDstIdExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

// vertex/edge expression
void FoldConstantExprVisitor::visit(VertexExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(EdgeExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(ColumnExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visitBinaryExpr(BinaryExpression *expr) {
    bool leftCanBeFolded = true, rightCanBeFolded = true;
    if (!isConstant(expr->left())) {
        expr->left()->accept(this);
        leftCanBeFolded = canBeFolded_;
        if (leftCanBeFolded) {
            expr->setLeft(fold(expr->left()));
        }
    }
    if (!isConstant(expr->right())) {
        expr->right()->accept(this);
        rightCanBeFolded = canBeFolded_;
        if (rightCanBeFolded) {
            expr->setRight(fold(expr->right()));
        }
    }
    canBeFolded_ = leftCanBeFolded && rightCanBeFolded;
}

Expression *FoldConstantExprVisitor::fold(Expression *expr) const {
    QueryExpressionContext ctx;
    auto value = expr->eval(ctx(nullptr));
    return new ConstantExpression(std::move(value));
}

void FoldConstantExprVisitor::visit(PathBuildExpression *expr) {
    auto &items = expr->items();
    bool canBeFolded = true;
    for (size_t i = 0; i < items.size(); ++i) {
        auto item = items[i].get();
        if (isConstant(item)) {
            continue;
        }
        item->accept(this);
        if (!canBeFolded_) {
            canBeFolded = false;
            continue;
        }
        expr->setItem(i, std::unique_ptr<Expression>{fold(item)});
    }
    canBeFolded_ = canBeFolded;
}

void FoldConstantExprVisitor::visit(ListComprehensionExpression *expr) {
    bool canBeFolded = true;
    if (!isConstant(expr->collection())) {
        expr->collection()->accept(this);
        if (canBeFolded_) {
            expr->setCollection(fold(expr->collection()));
        } else {
            canBeFolded = false;
        }
    }
    if (expr->hasFilter() && !isConstant(expr->filter())) {
        expr->filter()->accept(this);
        if (canBeFolded_) {
            expr->setFilter(fold(expr->filter()));
        } else {
            canBeFolded = false;
        }
    }
    if (expr->hasMapping() && !isConstant(expr->mapping())) {
        expr->mapping()->accept(this);
        if (canBeFolded_) {
            expr->setMapping(fold(expr->mapping()));
        } else {
            canBeFolded = false;
        }
    }
    canBeFolded_ = canBeFolded;
}

void FoldConstantExprVisitor::visit(PredicateExpression *expr) {
    bool canBeFolded = true;
    if (!isConstant(expr->collection())) {
        expr->collection()->accept(this);
        if (canBeFolded_) {
            expr->setCollection(fold(expr->collection()));
        } else {
            canBeFolded = false;
        }
    }
    if (!isConstant(expr->filter())) {
        expr->filter()->accept(this);
        if (canBeFolded_) {
            expr->setFilter(fold(expr->filter()));
        } else {
            canBeFolded = false;
        }
    }
    canBeFolded_ = canBeFolded;
}

}   // namespace graph
}   // namespace nebula
