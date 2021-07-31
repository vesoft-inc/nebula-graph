/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "visitor/AggregateVisitor.h"

// Implement a mechanism similar to golang defer
#define DEFER(code)   defers.emplace_back([&, found_ = this->found_](){code;})
#define DEFINE_DEFER_LIST std::list<DeferFunc<std::function<void()> > > defers;

template <typename F>
struct DeferFunc {
    F f_;
    explicit DeferFunc(F f) : f_(f) {}
    ~DeferFunc() { f_(); }
};

namespace nebula {
namespace graph {

void AggregateVisitor::visit(TypeCastingExpression* expr) {
    DEFINE_DEFER_LIST
    bool found = false;

    expr->operand()->accept(this);
    found = found || found_;
    DEFER(
        if (found && !found_ && !isConstant(expr->operand())) {
            addToGroupKeys(expr->operand());
        });

    found_ = found;
}

void AggregateVisitor::visit(UnaryExpression* expr) {
    DEFINE_DEFER_LIST
    bool found = false;

    expr->operand()->accept(this);
    found = found || found_;
    DEFER(
        if (found && !found_ && !isConstant(expr->operand())) {
            addToGroupKeys(expr->operand());
        });

    found_ = found;
}

void AggregateVisitor::visit(FunctionCallExpression* expr) {
    DEFINE_DEFER_LIST
    bool found = false;

    for (auto& arg : expr->args()->args()) {
        arg->accept(this);
        found = found || found_;
        DEFER(
            if (found && !found_ && !isConstant(arg)) {
                addToGroupKeys(arg);
            });
    }

    found_ = found;
}

void AggregateVisitor::visit(AggregateExpression* expr) {
    found_ = true;
    addToGroupItems(expr);
}

void AggregateVisitor::visit(ListExpression* expr) {
    DEFINE_DEFER_LIST
    bool found = false;

    for (const auto& item : expr->items()) {
        item->accept(this);
        found = found || found_;
        DEFER(
            if (found && !found_ && !isConstant(item)) {
                addToGroupKeys(item);
            });
    }

    found_ = found;
}

void AggregateVisitor::visit(SetExpression* expr) {
    DEFINE_DEFER_LIST
    bool found = false;

    for (const auto& item : expr->items()) {
        item->accept(this);
        found = found || found_;
        DEFER(
            if (found && !found_ && !isConstant(item)) {
                addToGroupKeys(item);
            });
    }

    found_ = found;
}

void AggregateVisitor::visit(MapExpression* expr) {
    DEFINE_DEFER_LIST
    bool found = false;

    for (const auto& pair : expr->items()) {
        pair.second->accept(this);
        found = found || found_;
        DEFER(
            if (found && !found_ && !isConstant(pair.second)) {
                addToGroupKeys(pair.second);
            });
    }

    found_ = found;
}

void AggregateVisitor::visit(CaseExpression* expr) {
    DEFINE_DEFER_LIST
    bool found = false;

    if (expr->hasCondition()) {
        expr->condition()->accept(this);
        found = found || found_;
        DEFER(
            if (found && !found_ && !isConstant(expr->condition())) {
                addToGroupKeys(expr->condition());
            });
    }

    if (expr->hasDefault()) {
        expr->defaultResult()->accept(this);
        found = found || found_;
        DEFER(
            if (found && !found_ && !isConstant(expr->defaultResult())) {
                addToGroupKeys(expr->defaultResult());
            });
    }
    for (const auto& whenThen : expr->cases()) {
        whenThen.when->accept(this);
        found = found || found_;
        DEFER(
            if (found && !found_ && !isConstant(whenThen.when)) {
                addToGroupKeys(whenThen.when);
            });

        whenThen.then->accept(this);
        found = found || found_;
        DEFER(
            if (found && !found_ && !isConstant(whenThen.then)) {
                addToGroupKeys(whenThen.then);
            });
    }

    found_ = found;
}

void AggregateVisitor::visit(PredicateExpression* expr) {
    DEFINE_DEFER_LIST
    bool found = false;

    expr->collection()->accept(this);
    found = found || found_;
    DEFER(
        if (found && !found_ && !isConstant(expr->collection())) {
            addToGroupKeys(expr->collection());
        });

    if (expr->hasFilter()) {
        expr->filter()->accept(this);
        found = found || found_;
        DEFER(
            if (found && !found_ && !isConstant(expr->filter())) {
                addToGroupKeys(expr->filter());
            });
    }

    found_ = found;
}

void AggregateVisitor::visit(ReduceExpression* expr) {
    DEFINE_DEFER_LIST
    bool found = false;

    expr->initial()->accept(this);
    found = found || found_;
    DEFER(
        if (found && !found_ && !isConstant(expr->initial())) {
            addToGroupKeys(expr->initial());
        });

    expr->collection()->accept(this);
    found = found || found_;
    DEFER(
        if (found && !found_ && !isConstant(expr->collection())) {
            addToGroupKeys(expr->collection());
        });

    expr->mapping()->accept(this);
    found = found || found_;
    DEFER(
        if (found && !found_ && !isConstant(expr->mapping())) {
            addToGroupKeys(expr->mapping());
        });

    found_ = found;
}

void AggregateVisitor::visit(ListComprehensionExpression* expr) {
    DEFINE_DEFER_LIST
    bool found = false;

    expr->collection()->accept(this);
    found = found || found_;
    DEFER(
        if (found && !found_ && !isConstant(expr->collection())) {
            addToGroupKeys(expr->collection());
        });

    if (expr->hasFilter()) {
        expr->filter()->accept(this);
        found = found || found_;
        DEFER(
            if (found && !found_ && !isConstant(expr->filter())) {
                addToGroupKeys(expr->filter());
            });
    }

    if (expr->hasMapping()) {
        expr->mapping()->accept(this);
        found = found || found_;
        DEFER(
            if (found && !found_ && !isConstant(expr->mapping())) {
                addToGroupKeys(expr->mapping());
            });
    }

    found_ = found;
}

void AggregateVisitor::visit(ConstantExpression* expr) {
    UNUSED(expr);
    found_ = false;
}

void AggregateVisitor::visit(EdgePropertyExpression* expr) {
    UNUSED(expr);
    found_ = false;
}

void AggregateVisitor::visit(TagPropertyExpression* expr) {
    UNUSED(expr);
    found_ = false;
}

void AggregateVisitor::visit(InputPropertyExpression* expr) {
    UNUSED(expr);
    found_ = false;
}

void AggregateVisitor::visit(VariablePropertyExpression* expr) {
    UNUSED(expr);
    found_ = false;
}

void AggregateVisitor::visit(SourcePropertyExpression* expr) {
    UNUSED(expr);
    found_ = false;
}

void AggregateVisitor::visit(DestPropertyExpression* expr) {
    UNUSED(expr);
    found_ = false;
}

void AggregateVisitor::visit(EdgeSrcIdExpression* expr) {
    UNUSED(expr);
    found_ = false;
}

void AggregateVisitor::visit(EdgeTypeExpression* expr) {
    UNUSED(expr);
    found_ = false;
}

void AggregateVisitor::visit(EdgeRankExpression* expr) {
    UNUSED(expr);
    found_ = false;
}

void AggregateVisitor::visit(EdgeDstIdExpression* expr) {
    UNUSED(expr);
    found_ = false;
}

void AggregateVisitor::visit(UUIDExpression* expr) {
    UNUSED(expr);
    found_ = false;
}

void AggregateVisitor::visit(VariableExpression* expr) {
    UNUSED(expr);
    found_ = false;
}

void AggregateVisitor::visit(VersionedVariableExpression* expr) {
    UNUSED(expr);
    found_ = false;
}

void AggregateVisitor::visit(LabelExpression* expr) {
    UNUSED(expr);
    found_ = false;
}

void AggregateVisitor::visit(LabelAttributeExpression* expr) {
    DEFINE_DEFER_LIST
    bool found = false;

    expr->left()->accept(this);
    found = found || found_;
    DEFER(
        if (found && !found_ && !isConstant(expr->left())) {
            addToGroupKeys(expr->left());
        });

    expr->right()->accept(this);
    found = found || found_;
    DEFER(
        if (found && !found_ && !isConstant(expr->right())) {
            addToGroupKeys(expr->right());
        });

    found_ = found;
}

void AggregateVisitor::visit(VertexExpression* expr) {
    UNUSED(expr);
    found_ = false;
}

void AggregateVisitor::visit(EdgeExpression* expr) {
    UNUSED(expr);
    found_ = false;
}

void AggregateVisitor::visit(ColumnExpression* expr) {
    UNUSED(expr);
    found_ = false;
}

void AggregateVisitor::visit(SubscriptRangeExpression* expr) {
    DEFINE_DEFER_LIST
    bool found = false;

    expr->list()->accept(this);
    found = found || found_;
    DEFER(
        if (found && !found_ && !isConstant(expr->list())) {
            addToGroupKeys(expr->list());
        });

    if (expr->lo()) {
        expr->lo()->accept(this);
        found = found || found_;
        DEFER(
            if (found && !found_ && !isConstant(expr->lo())) {
                addToGroupKeys(expr->lo());
            });
    }

    if (expr->hi()) {
        expr->hi()->accept(this);
        found = found || found_;
        DEFER(
            if (found && !found_ && !isConstant(expr->hi())) {
                addToGroupKeys(expr->hi());
            });
    }

    found_ = found;
}

void AggregateVisitor::visitBinaryExpr(BinaryExpression* expr) {
    DEFINE_DEFER_LIST
    bool found = false;

    expr->left()->accept(this);
    found = found || found_;
    DEFER(
        if (found && !found_ && !isConstant(expr->left())) {
            addToGroupKeys(expr->left());
        });

    expr->right()->accept(this);
    found = found || found_;
    DEFER(
        if (found && !found_ && !isConstant(expr->right())) {
            addToGroupKeys(expr->right());
        });

    found_ = found;
}

}   // namespace graph
}   // namespace nebula
