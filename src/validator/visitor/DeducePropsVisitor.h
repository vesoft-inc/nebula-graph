/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_VISITOR_DEDUCEPROPSVISITOR_H_
#define VALIDATOR_VISITOR_DEDUCEPROPSVISITOR_H_

#include "common/base/Status.h"
#include "common/thrift/ThriftTypes.h"
#include "validator/visitor/ExprVisitorImpl.h"

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

    void visitEdgeSymPropExpr(const SymbolPropertyExpression* expr);

    void reportError(const Expression* expr);

    QueryContext* qctx_{nullptr};
    GraphSpaceID space_;
    ExpressionProps* exprProps_{nullptr};
    Status status_;
};

}   // namespace graph
}   // namespace nebula

#endif   // VALIDATOR_VISITOR_DEDUCEPROPSVISITOR_H_
