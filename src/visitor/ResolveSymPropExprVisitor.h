/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VISITOR_RESOLVESYMPROPEXPRVISITOR_H_
#define VISITOR_RESOLVESYMPROPEXPRVISITOR_H_

#include <memory>
#include <type_traits>
#include <vector>

#include "visitor/ExprVisitorImpl.h"

namespace nebula {

class Expression;

namespace graph {

class ResolveSymPropExprVisitor final : public ExprVisitorImpl {
public:
    explicit ResolveSymPropExprVisitor(bool isTag);

    bool ok() const override {
        return true;
    }

private:
    void visitTypeCastingExpr(TypeCastingExpression* expr) override;
    void visitUnaryExpr(UnaryExpression* expr) override;
    void visitFunctionCallExpr(FunctionCallExpression* expr) override;
    void visitListExpr(ListExpression* expr) override;
    void visitSetExpr(SetExpression* expr) override;
    void visitMapExpr(MapExpression* expr) override;
    void visitBinaryExpr(BinaryExpression* expr) override;

    Expression* createExpr(const SymbolPropertyExpression* expr);
    std::vector<std::unique_ptr<Expression>> resolveExprList(
        const std::vector<const Expression*>& exprs);
    static bool isSymPropExpr(const Expression* expr);

    bool isTag_{false};
};

}   // namespace graph
}   // namespace nebula

#endif   // VISITOR_RESOLVESYMPROPEXPRVISITOR_H_
