/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_VISITOR_EXPRVISITORIMPL_H_
#define VALIDATOR_VISITOR_EXPRVISITORIMPL_H_

#include "common/expression/ExprVisitor.h"

namespace nebula {

class BinaryExpression;

namespace graph {

class ExprVisitorImpl : public ExprVisitor {
public:
    virtual bool ok() const = 0;

protected:
    void visitArithmeticExpr(const ArithmeticExpression* expr) override;
    void visitUnaryExpr(const UnaryExpression* expr) override;
    void visitRelationalExpr(const RelationalExpression* expr) override;
    void visitLogicalExpr(const LogicalExpression* expr) override;
    void visitTypeCastingExpr(const TypeCastingExpression* expr) override;
    void visitFunctionCallExpr(const FunctionCallExpression* expr) override;
    void visitListExpr(const ListExpression* expr) override;
    void visitSetExpr(const SetExpression* expr) override;
    void visitMapExpr(const MapExpression* expr) override;
    void visitSubscriptExpr(const SubscriptExpression* expr) override;

    void visitConstantExpr(const ConstantExpression* expr) override;
    void visitEdgePropertyExpr(const EdgePropertyExpression* expr) override;
    void visitTagPropertyExpr(const TagPropertyExpression* expr) override;
    void visitInputPropertyExpr(const InputPropertyExpression* expr) override;
    void visitVariablePropertyExpr(const VariablePropertyExpression* expr) override;
    void visitSourcePropertyExpr(const SourcePropertyExpression* expr) override;
    void visitDestPropertyExpr(const DestPropertyExpression* expr) override;
    void visitEdgeSrcIdExpr(const EdgeSrcIdExpression* expr) override;
    void visitEdgeTypeExpr(const EdgeTypeExpression* expr) override;
    void visitEdgeRankExpr(const EdgeRankExpression* expr) override;
    void visitEdgeDstIdExpr(const EdgeDstIdExpression* expr) override;
    void visitUUIDExpr(const UUIDExpression* expr) override;
    void visitVariableExpr(const VariableExpression* expr) override;
    void visitVersionedVariableExpr(const VersionedVariableExpression* expr) override;
    void visitLabelExpr(const LabelExpression* expr) override;
    void visitSymbolPropertyExpr(const SymbolPropertyExpression* expr) override;

    void visitBinaryExpr(const BinaryExpression* expr);
};

}   // namespace graph
}   // namespace nebula

#endif   // VALIDATOR_VISITOR_EXPRVISITORIMPL_H_
