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

    const std::string name() override {
        return name_;
    }

private:
    using ExprVisitorImpl::visit;
    void visit(EdgePropertyExpression* expr) override;
    void visit(TagPropertyExpression* expr) override;
    void visit(InputPropertyExpression* expr) override;
    void visit(VariablePropertyExpression* expr) override;
    void visit(SourcePropertyExpression* expr) override;
    void visit(DestPropertyExpression* expr) override;
    void visit(EdgeSrcIdExpression* expr) override;
    void visit(EdgeTypeExpression* expr) override;
    void visit(EdgeRankExpression* expr) override;
    void visit(EdgeDstIdExpression* expr) override;
    void visit(UUIDExpression* expr) override;
    void visit(VariableExpression* expr) override;
    void visit(VersionedVariableExpression* expr) override;
    void visit(LabelExpression* expr) override;
    void visit(AttributeExpression* expr) override;
    void visit(LabelAttributeExpression* expr) override;
    void visit(ConstantExpression* expr) override;
    void visit(VertexExpression* expr) override;
    void visit(EdgeExpression* expr) override;

    void visitEdgePropExpr(PropertyExpression* expr);

    QueryContext* qctx_{nullptr};
    GraphSpaceID space_;
    ExpressionProps* exprProps_{nullptr};
    const std::string name_ = "Deduce Props";
};

}   // namespace graph
}   // namespace nebula

#endif   // VISITOR_DEDUCEPROPSVISITOR_H_
