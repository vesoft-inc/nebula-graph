/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VISITOR_AGGREGATEVISITOR_H_
#define VISITOR_AGGREGATEVISITOR_H_

#include "common/expression/Expression.h"
#include "visitor/ExprVisitorImpl.h"

namespace nebula {
namespace graph {
class AggregateVisitor final : public ExprVisitorImpl {
public:
    using Matcher = std::function<bool(const Expression*)>;
    using Rewriter = std::function<Expression*(const Expression*)>;

    AggregateVisitor() {}

    std::unordered_set<Expression*> groupKeys() && {
        return std::move(groupKeys_);
    }

    std::unordered_set<Expression*> groupItems() && {
        return std::move(groupItems_);
    }

    bool ok() const override {
        // TODO: delete this interface
        return true;
    }

    bool found() const {
        return found_;
    }

private:
    using ExprVisitorImpl::visit;

    void visit(TypeCastingExpression* expr) override;
    void visit(UnaryExpression* expr) override;
    void visit(FunctionCallExpression* expr) override;
    void visit(AggregateExpression* expr) override;
    void visit(ListExpression* expr) override;
    void visit(SetExpression* expr) override;
    void visit(MapExpression* expr) override;
    void visit(CaseExpression* expr) override;
    void visit(PredicateExpression* expr) override;
    void visit(ReduceExpression* expr) override;

    void visit(ConstantExpression* expr) override;
    void visit(EdgePropertyExpression* expr) override;
    void visit(TagPropertyExpression* expr) override;
    void visit(InputPropertyExpression* expr) override;
    void visit(VariablePropertyExpression* expr) override;
    void visit(SourcePropertyExpression* expr) override;
    void visit(DestPropertyExpression* expr) override;
    void visit(EdgeSrcIdExpression* expr) override;
    void visit(EdgeTypeExpression* expr) override;
    void visit(EdgeRankExpression* expr) override;
    void visit(EdgeDstIdExpression* expr) override;
    void visit(UUIDExpression* expr) override;
    void visit(VariableExpression* expr) override;
    void visit(VersionedVariableExpression* expr) override;
    void visit(LabelExpression* expr) override;
    void visit(LabelAttributeExpression* expr) override;
    void visit(VertexExpression* expr) override;
    void visit(EdgeExpression* expr) override;
    void visit(ColumnExpression* expr) override;
    void visit(ListComprehensionExpression* expr) override;
    void visit(SubscriptRangeExpression* expr) override;

    void visitBinaryExpr(BinaryExpression* expr) override;

private:
    void addToGroupKeys(Expression* expr) {
        LOG(INFO) << "wang: addTOgroupKeys: " << expr->toString();
        groupKeys_.emplace(expr);
        groupItems_.emplace(expr);
    }

    void addToGroupItems(Expression* expr) {
        groupItems_.emplace(expr);
    }

    bool isConstant(Expression *expr) const {
        return expr->kind() == Expression::Kind::kConstant;
    }

private:
    std::unordered_set<Expression*> groupKeys_;
    std::unordered_set<Expression*> groupItems_;

    bool found_{false};
};

}   // namespace graph
}   // namespace nebula

#endif   // VISITOR_AGGREGATEVISITOR_H_
