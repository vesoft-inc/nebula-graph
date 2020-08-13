/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VISITOR_DEDUCETYPEVISITOR_H_
#define VISITOR_DEDUCETYPEVISITOR_H_

#include "common/base/Status.h"
#include "common/datatypes/Value.h"
#include "common/expression/ExprVisitor.h"
#include "context/ValidateContext.h"

namespace nebula {
namespace graph {

class QueryContext;

class DeduceTypeVisitor final : public ExprVisitor {
public:
    DeduceTypeVisitor(QueryContext *qctx,
                      ValidateContext *vctx,
                      const ColsDef &inputs,
                      GraphSpaceID space);
    ~DeduceTypeVisitor() = default;

    bool ok() const {
        return status_.ok();
    }

    Status status() && {
        return std::move(status_);
    }

    Value::Type type() const {
        return type_;
    }

private:
    void visitArithmeticExpr(ArithmeticExpression *expr) override;
    void visitRelationalExpr(RelationalExpression *expr) override;
    void visitLogicalExpr(LogicalExpression *expr) override;
    void visitUnaryExpr(UnaryExpression *expr) override;
    void visitFunctionCallExpr(FunctionCallExpression *expr) override;
    void visitTypeCastingExpr(TypeCastingExpression *expr) override;
    void visitTagPropertyExpr(TagPropertyExpression *expr) override;
    void visitSourcePropertyExpr(SourcePropertyExpression *expr) override;
    void visitDestPropertyExpr(DestPropertyExpression *expr) override;
    void visitEdgePropertyExpr(EdgePropertyExpression *expr) override;
    void visitVariablePropertyExpr(VariablePropertyExpression *expr) override;
    void visitInputPropertyExpr(InputPropertyExpression *expr) override;
    void visitSymbolPropertyExpr(SymbolPropertyExpression *expr) override;
    void visitLabelExpr(LabelExpression *expr) override;
    void visitConstantExpr(ConstantExpression *expr) override;
    void visitEdgeSrcIdExpr(EdgeSrcIdExpression *expr) override;
    void visitEdgeTypeExpr(EdgeTypeExpression *expr) override;
    void visitEdgeRankExpr(EdgeRankExpression *expr) override;
    void visitEdgeDstIdExpr(EdgeDstIdExpression *expr) override;
    void visitUUIDExpr(UUIDExpression *expr) override;
    void visitVariableExpr(VariableExpression *expr) override;
    void visitVersionedVariableExpr(VersionedVariableExpression *expr) override;
    void visitListExpr(ListExpression *expr) override;
    void visitSetExpr(SetExpression *expr) override;
    void visitMapExpr(MapExpression *expr) override;
    void visitSubscriptExpr(SubscriptExpression *expr) override;

    void visitTagPropExpr(SymbolPropertyExpression *expr);

    const QueryContext *qctx_;
    const ValidateContext *vctx_;
    const ColsDef &inputs_;
    GraphSpaceID space_;
    Status status_;
    Value::Type type_;
};

}   // namespace graph
}   // namespace nebula

#endif   // VISITOR_DEDUCETYPEVISITOR_H_
