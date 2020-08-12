/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_VISITOR_FINDEXPRVISITOR_H_
#define VALIDATOR_VISITOR_FINDEXPRVISITOR_H_

#include <unordered_set>

#include "common/expression/Expression.h"
#include "visitor/ExprVisitorImpl.h"

namespace nebula {
namespace graph {

class FindExprVisitor final : public ExprVisitorImpl {
public:
    explicit FindExprVisitor(const std::unordered_set<Expression::Kind>& exprs);

    bool ok() const override {
        // continue if not found
        return !found_;
    }

    const Expression* expr() const {
        return expr_;
    }

private:
    void visitTypeCastingExpr(const TypeCastingExpression* expr) override;
    void visitFunctionCallExpr(const FunctionCallExpression* expr) override;
    void visitListExpr(const ListExpression* expr) override;
    void visitSetExpr(const SetExpression* expr) override;
    void visitMapExpr(const MapExpression* expr) override;

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

    void visitBinaryExpr(const BinaryExpression* expr) override;

    void findExpr(const Expression* expr);

    bool found_{false};
    const Expression* expr_{nullptr};
    const std::unordered_set<Expression::Kind>& exprs_;
};

}   // namespace graph
}   // namespace nebula

#endif   // VALIDATOR_VISITOR_FINDEXPRVISITOR_H_
