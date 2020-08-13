/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VISITOR_EXPRVISITORIMPL_H_
#define VISITOR_EXPRVISITORIMPL_H_

#include "common/expression/ExprVisitor.h"

namespace nebula {

class BinaryExpression;

namespace graph {

class ExprVisitorImpl : public ExprVisitor {
public:
    virtual bool ok() const = 0;

protected:
    void visitArithmeticExpr(ArithmeticExpression* expr) override;
    void visitUnaryExpr(UnaryExpression* expr) override;
    void visitRelationalExpr(RelationalExpression* expr) override;
    void visitLogicalExpr(LogicalExpression* expr) override;
    void visitTypeCastingExpr(TypeCastingExpression* expr) override;
    void visitFunctionCallExpr(FunctionCallExpression* expr) override;
    void visitListExpr(ListExpression* expr) override;
    void visitSetExpr(SetExpression* expr) override;
    void visitMapExpr(MapExpression* expr) override;
    void visitSubscriptExpr(SubscriptExpression* expr) override;

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

    virtual void visitBinaryExpr(BinaryExpression* expr);
};

}   // namespace graph
}   // namespace nebula

#endif   // VISITOR_EXPRVISITORIMPL_H_
