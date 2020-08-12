/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VISITOR_RESOLVESYMPROPEXPRVISITOR_H_
#define VISITOR_RESOLVESYMPROPEXPRVISITOR_H_

#include <type_traits>

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
    void visitTypeCastingExpr(const TypeCastingExpression* expr) override;
    void visitUnaryExpr(const UnaryExpression* expr) override;
    void visitFunctionCallExpr(const FunctionCallExpression* expr) override;
    void visitListExpr(const ListExpression* expr) override;
    void visitSetExpr(const SetExpression* expr) override;
    void visitMapExpr(const MapExpression* expr) override;

    void visitBinaryExpr(const BinaryExpression* expr) override;

    Expression* createExpr(const SymbolPropertyExpression* expr);

    bool isTag_{false};
};

}   // namespace graph
}   // namespace nebula

#endif   // VISITOR_RESOLVESYMPROPEXPRVISITOR_H_
