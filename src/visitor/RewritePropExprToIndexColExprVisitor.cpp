/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/RewritePropExprToIndexColExprVisitor.h"

namespace nebula {
namespace graph {
RewritePropExprToIndexColExprVisitor::RewritePropExprToIndexColExprVisitor(
    std::vector<std::string> cols)
    : cols_(std::move(cols)) {}

void RewritePropExprToIndexColExprVisitor::visit(ConstantExpression *expr) {
    UNUSED(expr);
}

void RewritePropExprToIndexColExprVisitor::visit(UnaryExpression *expr) {
    expr->operand()->accept(this);
    if (rewritedExpr_) {
        expr->setOperand(rewritedExpr_.release());
    }
}

void RewritePropExprToIndexColExprVisitor::visit(TypeCastingExpression *expr) {
    expr->operand()->accept(this);
    if (rewritedExpr_) {
        expr->setOperand(rewritedExpr_.release());
    }
}

void RewritePropExprToIndexColExprVisitor::visit(LabelExpression *expr) {
    UNUSED(expr);
}

void RewritePropExprToIndexColExprVisitor::visit(LabelAttributeExpression *expr) {
    UNUSED(expr);
}

void RewritePropExprToIndexColExprVisitor::visit(ArithmeticExpression *expr) {
    visitBinaryExpr(expr);
}

void RewritePropExprToIndexColExprVisitor::visit(RelationalExpression *expr) {
    visitBinaryExpr(expr);
}

void RewritePropExprToIndexColExprVisitor::visit(SubscriptExpression *expr) {
    UNUSED(expr);
}

void RewritePropExprToIndexColExprVisitor::visit(AttributeExpression *expr) {
    UNUSED(expr);
}

void RewritePropExprToIndexColExprVisitor::visit(LogicalExpression *expr) {
    auto &operands = expr->operands();
    for (auto i = 0u; i < operands.size(); i++) {
        operands[i]->accept(this);
        if (rewritedExpr_) {
            expr->setOperand(i, rewritedExpr_.release());
        }
    }
}

// function call
void RewritePropExprToIndexColExprVisitor::visit(FunctionCallExpression *expr) {
    const auto &args = expr->args()->args();
    for (size_t i = 0; i < args.size(); ++i) {
        auto &arg = args[i];
        arg->accept(this);
        if (rewritedExpr_) {
            expr->args()->setArg(i, std::move(rewritedExpr_));
        }
    }
}

void RewritePropExprToIndexColExprVisitor::visit(AggregateExpression *expr) {
    auto* arg = expr->arg();
    arg->accept(this);
    if (rewritedExpr_) {
        expr->setArg(std::move(rewritedExpr_).release());
    }
}

void RewritePropExprToIndexColExprVisitor::visit(UUIDExpression *expr) {
    UNUSED(expr);
}

// variable expression
void RewritePropExprToIndexColExprVisitor::visit(VariableExpression *expr) {
    UNUSED(expr);
}

void RewritePropExprToIndexColExprVisitor::visit(VersionedVariableExpression *expr) {
    UNUSED(expr);
}

// container expression
void RewritePropExprToIndexColExprVisitor::visit(ListExpression *expr) {
    const auto &items = expr->items();
    for (size_t i = 0; i < items.size(); ++i) {
        items[i]->accept(this);
        if (rewritedExpr_) {
            expr->setItem(i, std::move(rewritedExpr_));
        }
    }
}

void RewritePropExprToIndexColExprVisitor::visit(SetExpression *expr) {
    const auto &items = expr->items();
    for (size_t i = 0; i < items.size(); ++i) {
        items[i]->accept(this);
        if (rewritedExpr_) {
            expr->setItem(i, std::move(rewritedExpr_));
        }
    }
}

void RewritePropExprToIndexColExprVisitor::visit(MapExpression *expr) {
    const auto &items = expr->items();
    for (size_t i = 0; i < items.size(); ++i) {
        items[i].second->accept(this);
        if (rewritedExpr_) {
            auto key = std::make_unique<std::string>(*items[i].first);
            expr->setItem(i, {std::move(key), std::move(rewritedExpr_)});
        }
    }
}

// property Expression
void RewritePropExprToIndexColExprVisitor::visit(TagPropertyExpression *expr) {
    UNUSED(expr);
}

void RewritePropExprToIndexColExprVisitor::visit(EdgePropertyExpression *expr) {
    UNUSED(expr);
}

void RewritePropExprToIndexColExprVisitor::visit(InputPropertyExpression *expr) {
    auto index = find(*expr->prop());
    if (!index.ok()) {
        status_ = std::move(index).status();
    }
    rewritedExpr_ = std::make_unique<ColumnExpression>(std::move(index).value());
}

void RewritePropExprToIndexColExprVisitor::visit(VariablePropertyExpression *expr) {
    auto index = find(*expr->prop());
    if (!index.ok()) {
        status_ = std::move(index).status();
    }
    rewritedExpr_ = std::make_unique<ColumnExpression>(std::move(index).value());
}

void RewritePropExprToIndexColExprVisitor::visit(DestPropertyExpression *expr) {
    UNUSED(expr);
}

void RewritePropExprToIndexColExprVisitor::visit(SourcePropertyExpression *expr) {
    UNUSED(expr);
}

void RewritePropExprToIndexColExprVisitor::visit(EdgeSrcIdExpression *expr) {
    UNUSED(expr);
}

void RewritePropExprToIndexColExprVisitor::visit(EdgeTypeExpression *expr) {
    UNUSED(expr);
}

void RewritePropExprToIndexColExprVisitor::visit(EdgeRankExpression *expr) {
    UNUSED(expr);
}

void RewritePropExprToIndexColExprVisitor::visit(EdgeDstIdExpression *expr) {
    UNUSED(expr);
}

void RewritePropExprToIndexColExprVisitor::visit(VertexExpression *expr) {
    UNUSED(expr);
}

void RewritePropExprToIndexColExprVisitor::visit(EdgeExpression *expr) {
    UNUSED(expr);
}

void RewritePropExprToIndexColExprVisitor::visit(ColumnExpression *expr) {
    UNUSED(expr);
}

void RewritePropExprToIndexColExprVisitor::visit(CaseExpression *expr) {
    if (expr->hasCondition()) {
        expr->condition()->accept(this);
        if (rewritedExpr_) {
            expr->setCondition(rewritedExpr_.release());
        }
    }
    if (expr->hasDefault()) {
        expr->defaultResult()->accept(this);
        if (rewritedExpr_) {
            expr->setDefault(rewritedExpr_.release());
        }
    }
    auto &cases = expr->cases();
    for (size_t i = 0; i < cases.size(); ++i) {
        auto when = cases[i].when.get();
        auto then = cases[i].then.get();
        when->accept(this);
        if (rewritedExpr_) {
            expr->setWhen(i, rewritedExpr_.release());
        }
        then->accept(this);
        if (rewritedExpr_) {
            expr->setThen(i, rewritedExpr_.release());
        }
    }
}

void RewritePropExprToIndexColExprVisitor::visitBinaryExpr(BinaryExpression *expr) {
    expr->left()->accept(this);
    if (rewritedExpr_) {
        expr->setLeft(rewritedExpr_.release());
    }
    expr->right()->accept(this);
    if (rewritedExpr_) {
        expr->setRight(rewritedExpr_.release());
    }
}

void RewritePropExprToIndexColExprVisitor::visit(PathBuildExpression *expr) {
    const auto &items = expr->items();
    for (size_t i = 0; i < items.size(); ++i) {
        items[i]->accept(this);
        if (rewritedExpr_) {
            expr->setItem(i, std::move(rewritedExpr_));
        }
    }
}

void RewritePropExprToIndexColExprVisitor::visit(ListComprehensionExpression *expr) {
    expr->collection()->accept(this);
    if (rewritedExpr_) {
        expr->setCollection(rewritedExpr_.release());
    }
    if (expr->hasFilter()) {
        expr->filter()->accept(this);
        if (rewritedExpr_) {
            expr->setFilter(rewritedExpr_.release());
        }
    }
    if (expr->hasMapping()) {
        expr->mapping()->accept(this);
        if (rewritedExpr_) {
            expr->setMapping(rewritedExpr_.release());
        }
    }
}

StatusOr<size_t> RewritePropExprToIndexColExprVisitor::find(const std::string& col) const {
    for (size_t i = 0; i < cols_.size(); ++i) {
        if (col == cols_[i]) {
            return i;
        }
    }

    auto cols = folly::join(",", cols_);
    return Status::Error(
        "Does not find the col `%s' in input columns: [%s]", col.c_str(), cols.c_str());
}
}  // namespace graph
}  // namespace nebula
