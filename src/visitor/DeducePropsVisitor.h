/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VISITOR_DEDUCEPROPSVISITOR_H_
#define VISITOR_DEDUCEPROPSVISITOR_H_

#include "common/base/Status.h"
#include "common/thrift/ThriftTypes.h"
#include "visitor/ExprVisitorImpl.h"

namespace nebula {

class Expression;

namespace graph {

class QueryContext;
class ExpressionProps;

class DeducePropsVisitor : public ExprVisitorImpl {
public:
    DeducePropsVisitor(QueryContext* qctx, GraphSpaceID space, ExpressionProps* exprProps);

    bool ok() const override {
        return status_.ok();
    }

    const Status& status() const {
        return status_;
    }

private:
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

    void visitEdgeSymPropExpr(SymbolPropertyExpression* expr);

    void reportError(const Expression* expr);

    QueryContext* qctx_{nullptr};
    GraphSpaceID space_;
    ExpressionProps* exprProps_{nullptr};
    Status status_;
};

}   // namespace graph
}   // namespace nebula

#endif   // VISITOR_DEDUCEPROPSVISITOR_H_
