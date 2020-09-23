/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef VISITOR_EXTRACTPROPEXPRVISITON_H_
#define VISITOR_EXTRACTPROPEXPRVISITON_H_

#include "visitor/ExprVisitorImpl.h"

namespace nebula {
namespace graph {

class ExtractPropExprVisitor final : public ExprVisitorImpl {
public:
    explicit ExtractPropExprVisitor(YieldColumns* props) : props_(props) {}

private:
    using ExprVisitorImpl::visit;

    void visit(ConstantExpression *) override;
    void visit(LabelExpression *) override;
    void visit(UUIDExpression *) override;
    void visit(VariableExpression *) override;
    void visit(VersionedVariableExpression *) override;
    void visit(TagPropertyExpression *) override;
    void visit(EdgePropertyExpression *) override;
    void visit(InputPropertyExpression *) override;
    void visit(VariablePropertyExpression *) override;
    void visit(DestPropertyExpression *) override;
    void visit(SourcePropertyExpression *) override;
    void visit(EdgeSrcIdExpression *) override;
    void visit(EdgeTypeExpression *) override;
    void visit(EdgeRankExpression *) override;
    void visit(EdgeDstIdExpression *) override;
    void visit(VertexExpression *) override;
    void visit(EdgeExpression *) override;
    void visit(LogicalExpression *) override;

    void reportError(const Expression* expr);

private:
    YieldColumns*                         props_{nullptr};
};
}   // namespace graph
}   // namespace nebula
