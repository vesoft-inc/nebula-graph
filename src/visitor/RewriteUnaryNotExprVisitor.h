/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VISITOR_REWRITEUNARYNOTEXPRVISITOR_H_
#define VISITOR_REWRITEUNARYNOTEXPRVISITOR_H_

#include "common/expression/ExprVisitor.h"
#include "visitor/ExprVisitorImpl.h"

namespace nebula {
namespace graph {

class RewriteUnaryNotExprVisitor final : public ExprVisitorImpl {
public:
    bool ok() const override {
        return true;
    }

    bool canBeReduced() const {
        return reducible_;
    }

    std::unique_ptr<Expression> getExpr() {
        return std::move(expr_);
    }

    std::unique_ptr<Expression> reduce(UnaryExpression* expr);

private:
    using ExprVisitorImpl::visit;

    void visit(TypeCastingExpression*) override{};
    void visit(FunctionCallExpression*) override{};
    void visit(ArithmeticExpression*) override{};

    void visit(UnaryExpression* expr) override;
    // expressions return a bool value
    void visit(ConstantExpression* expr) override;
    void visit(RelationalExpression* expr) override;
    void visit(LogicalExpression* expr) override;
    void visitBinaryExpr(BinaryExpression* expr) override;

    void visit(ListExpression*) override {}
    void visit(SetExpression*) override {}
    void visit(MapExpression*) override {}
    void visit(CaseExpression*) override {}
    void visit(LabelExpression*) override {}
    void visit(UUIDExpression*) override {}
    void visit(LabelAttributeExpression*) override {}
    void visit(VariableExpression*) override {}
    void visit(VersionedVariableExpression*) override {}
    void visit(TagPropertyExpression*) override {}
    void visit(EdgePropertyExpression*) override {}
    void visit(InputPropertyExpression*) override {}
    void visit(VariablePropertyExpression*) override {}
    void visit(DestPropertyExpression*) override {}
    void visit(SourcePropertyExpression*) override {}
    void visit(EdgeSrcIdExpression*) override {}
    void visit(EdgeTypeExpression*) override {}
    void visit(EdgeRankExpression*) override {}
    void visit(EdgeDstIdExpression*) override {}
    void visit(VertexExpression*) override {}
    void visit(EdgeExpression*) override {}
    void visit(ColumnExpression*) override {}

    bool isUnaryNotExpr(const Expression* expr);

private:
    bool reducible_{false};
    std::unique_ptr<Expression> expr_;
};

}   // namespace graph
}   // namespace nebula

#endif   // VISITOR_REWRITEUNARYNOTEXPRVISITOR_H_
