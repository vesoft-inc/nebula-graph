/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VISITOR_COLLECTEXPRSVISITOR_H_
#define VISITOR_COLLECTEXPRSVISITOR_H_

#include <unordered_set>

#include "common/expression/Expression.h"
#include "visitor/ExprVisitorImpl.h"

namespace nebula {
namespace graph {

class CollectExprsVisitor final : public ExprVisitorImpl {
public:
    explicit CollectExprsVisitor(const std::unordered_set<Expression::Kind>& exprTypes);
    bool ok() const override {
        return true;
    }

    std::vector<const Expression*> exprs() && {
        return std::move(exprs_);
    }

private:
    void visitTypeCastingExpr(TypeCastingExpression* expr) override;
    void visitUnaryExpr(UnaryExpression* expr) override;
    void visitFunctionCallExpr(FunctionCallExpression* expr) override;
    void visitListExpr(ListExpression* expr) override;
    void visitSetExpr(SetExpression* expr) override;
    void visitMapExpr(MapExpression* expr) override;

    void visitConstantExpr(ConstantExpression* expr) override;
    void visitEdgePropertyExpr(EdgePropertyExpression* expr) override;
    void visitTagPropertyExpr(TagPropertyExpression* expr) override;
    void visitInputPropertyExpr(InputPropertyExpression* expr) override;
    void visitVariablePropertyExpr(VariablePropertyExpression* expr) override;
    void visitSourcePropertyExpr(SourcePropertyExpression* expr) override;
    void visitDestPropertyExpr(DestPropertyExpression* expr) override;
    void visitEdgeSrcIdExpr(EdgeSrcIdExpression* expr) override;
    void visitEdgeTypeExpr(EdgeTypeExpression* expr) override;
    void visitEdgeRankExpr(EdgeRankExpression* expr) override;
    void visitEdgeDstIdExpr(EdgeDstIdExpression* expr) override;
    void visitUUIDExpr(UUIDExpression* expr) override;
    void visitVariableExpr(VariableExpression* expr) override;
    void visitVersionedVariableExpr(VersionedVariableExpression* expr) override;
    void visitLabelExpr(LabelExpression* expr) override;
    void visitSymbolPropertyExpr(SymbolPropertyExpression* expr) override;

    void visitBinaryExpr(BinaryExpression* expr) override;
    void collectExpr(const Expression* expr);

    const std::unordered_set<Expression::Kind>& exprTypes_;
    std::vector<const Expression*> exprs_;
};

}   // namespace graph
}   // namespace nebula

#endif   // VISITOR_COLLECTEXPRSVISITOR_H_
