/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VISITOR_REWRITERELEXPRVISITOR_H_
#define VISITOR_REWRITERELEXPRVISITOR_H_

#include "common/base/ObjectPool.h"
#include "common/expression/ExprVisitor.h"
#include "visitor/ExprVisitorImpl.h"

namespace nebula {
namespace graph {

class RewriteRelationalExprVisitor final : public ExprVisitorImpl {
public:
    explicit RewriteRelationalExprVisitor(ObjectPool *objPool);

    bool ok() const override {
        return true;
    }

    Expression *getExpr() {
        return expr_;
    }

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

    void visitBinaryExpr(BinaryExpression *expr) override;

private:
    static bool isArithmeticExpr(const Expression* expr) {
        return (
            expr->kind() == Expression::Kind::kAdd ||
            expr->kind() == Expression::Kind::kMinus ||
            expr->kind() == Expression::Kind::kMultiply ||
            expr->kind() == Expression::Kind::kDivision ||
            expr->kind() == Expression::Kind::kMod);
    }

    static Expression::Kind negateArithmeticType(const Expression::Kind kind) {
        switch (kind)
        {
        case Expression::Kind::kAdd:
            return Expression::Kind::kMinus;
        case Expression::Kind::kMinus:
            return Expression::Kind::kAdd;
        case Expression::Kind::kMultiply:
            return Expression::Kind::kDivision;
        case Expression::Kind::kDivision:
            return Expression::Kind::kMultiply;
        case Expression::Kind::kMod:
            LOG(FATAL) << "Unsupported expression kind: " << static_cast<uint8_t>(kind);
            break;
        default:
            LOG(FATAL) << "Invalid arithmetic expression kind: " << static_cast<uint8_t>(kind);
            break;
        }
    }

    Expression *expr_;
    ObjectPool *pool_;
};

}   // namespace graph
}   // namespace nebula

#endif   // VISITOR_REWRITERELEXPRVISITOR_H_
