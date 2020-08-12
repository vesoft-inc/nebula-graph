/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_VISITOR_DEDUCETYPEVISITOR_H_
#define VALIDATOR_VISITOR_DEDUCETYPEVISITOR_H_

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
    void visitArithmeticExpr(const ArithmeticExpression *expr) override;
    void visitRelationalExpr(const RelationalExpression *expr) override;
    void visitLogicalExpr(const LogicalExpression *expr) override;
    void visitUnaryExpr(const UnaryExpression *expr) override;
    void visitFunctionCallExpr(const FunctionCallExpression *expr) override;
    void visitTypeCastingExpr(const TypeCastingExpression *expr) override;
    void visitTagPropertyExpr(const TagPropertyExpression *expr) override;
    void visitSourcePropertyExpr(const SourcePropertyExpression *expr) override;
    void visitDestPropertyExpr(const DestPropertyExpression *expr) override;
    void visitEdgePropertyExpr(const EdgePropertyExpression *expr) override;
    void visitVariablePropertyExpr(const VariablePropertyExpression *expr) override;
    void visitInputPropertyExpr(const InputPropertyExpression *expr) override;
    void visitSymbolPropertyExpr(const SymbolPropertyExpression *expr) override;
    void visitLabelExpr(const LabelExpression *expr) override;
    void visitConstantExpr(const ConstantExpression *expr) override;
    void visitEdgeSrcIdExpr(const EdgeSrcIdExpression *expr) override;
    void visitEdgeTypeExpr(const EdgeTypeExpression *expr) override;
    void visitEdgeRankExpr(const EdgeRankExpression *expr) override;
    void visitEdgeDstIdExpr(const EdgeDstIdExpression *expr) override;
    void visitUUIDExpr(const UUIDExpression *expr) override;
    void visitVariableExpr(const VariableExpression *expr) override;
    void visitVersionedVariableExpr(const VersionedVariableExpression *expr) override;
    void visitListExpr(const ListExpression *expr) override;
    void visitSetExpr(const SetExpression *expr) override;
    void visitMapExpr(const MapExpression *expr) override;
    void visitSubscriptExpr(const SubscriptExpression *expr) override;

    void visitTagPropExpr(const SymbolPropertyExpression *expr);

    const QueryContext *qctx_;
    const ValidateContext *vctx_;
    const ColsDef &inputs_;
    GraphSpaceID space_;
    Status status_;
    Value::Type type_;
};

}   // namespace graph
}   // namespace nebula

#endif   // VALIDATOR_VISITOR_DEDUCETYPEVISITOR_H_
