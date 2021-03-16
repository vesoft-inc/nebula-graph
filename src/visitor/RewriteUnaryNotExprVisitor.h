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

    std::unique_ptr<Expression> getExpr() {
        return std::move(expr_);
    }

    std::unique_ptr<Expression> reduce(UnaryExpression* expr);

private:
    using ExprVisitorImpl::visit;

    void visit(ConstantExpression *expr) override;
    void visit(UnaryExpression *expr) override;
    void visit(TypeCastingExpression *expr) override;
    void visit(LabelExpression *expr) override;
    void visit(LabelAttributeExpression *expr) override;
    // binary expression
    void visit(ArithmeticExpression *expr) override;
    void visit(RelationalExpression *expr) override;
    void visit(SubscriptExpression *expr) override;
    void visit(AttributeExpression *expr) override;
    void visit(LogicalExpression *expr) override;
    // function call
    void visit(FunctionCallExpression *expr) override;
    void visit(AggregateExpression *expr) override;
    void visit(UUIDExpression *expr) override;
    // variable expression
    void visit(VariableExpression *expr) override;
    void visit(VersionedVariableExpression *expr) override;
    // container expression
    void visit(ListExpression *expr) override;
    void visit(SetExpression *expr) override;
    void visit(MapExpression *expr) override;
    // property Expression
    void visit(TagPropertyExpression *expr) override;
    void visit(EdgePropertyExpression *expr) override;
    void visit(InputPropertyExpression *expr) override;
    void visit(VariablePropertyExpression *expr) override;
    void visit(DestPropertyExpression *expr) override;
    void visit(SourcePropertyExpression *expr) override;
    void visit(EdgeSrcIdExpression *expr) override;
    void visit(EdgeTypeExpression *expr) override;
    void visit(EdgeRankExpression *expr) override;
    void visit(EdgeDstIdExpression *expr) override;
    // vertex/edge expression
    void visit(VertexExpression *expr) override;
    void visit(EdgeExpression *expr) override;
    // case expression
    void visit(CaseExpression *expr) override;
    // path build expression
    void visit(PathBuildExpression *expr) override;
    // column expression
    void visit(ColumnExpression *expr) override;
    // predicate expression
    void visit(PredicateExpression *expr) override;
    // list comprehension expression
    void visit(ListComprehensionExpression *) override;
    // reduce expression
    void visit(ReduceExpression *expr) override;

    bool isUnaryNotExpr(const Expression *expr);
    bool isRelExpr(const Expression *expr);
    std::unique_ptr<Expression> reverseRelExpr(Expression *expr);
    Expression::Kind getNegatedKind(const Expression::Kind kind);
    void visitBinaryExpr(BinaryExpression *expr) override;

private:
    bool reduced_{false};
    std::unique_ptr<Expression> expr_;
};

}   // namespace graph
}   // namespace nebula

#endif   // VISITOR_REWRITEUNARYNOTEXPRVISITOR_H_
