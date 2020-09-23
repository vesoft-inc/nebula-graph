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
    ExtractPropExprVisitor(ValidateContext *vctx,
                           YieldColumns *srcAndEdgePropCols,
                           YieldColumns *dstPropCols,
                           YieldColumns *inputPropCols,
                           std::unordered_map<std::string, YieldColumn *> &propExprColMap)
        : vctx_(DCHECK_NOTNULL(vctx)),
          srcAndEdgePropCols_(srcAndEdgePropCols),
          dstPropCols_(dstPropCols),
          inputPropCols_(inputPropCols),
          propExprColMap_(propExprColMap) {}

    bool ok() const override {
        return status_.ok();
    }

    const Status& status() const {
        return status_;
    }

private:
    using ExprVisitorImpl::visit;

    void visit(ConstantExpression *) override;
    void visit(LabelExpression *) override;
    void visit(UUIDExpression *) override;
    void visit(UnaryExpression *) override;
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

    void visitVertexEdgePropExpr(PropertyExpression *);
    void visitPropertyExpr(PropertyExpression *);
    void reportError(const Expression *);

private:
    YieldColumns *srcAndEdgePropCols_{nullptr};
    YieldColumns *dstPropCols_{nullptr};
    YieldColumns *inputPropCols_{nullptr};
    std::unordered_map<std::string, YieldColumn *> propExprColMap_;

    Status status_;
};
}   // namespace graph
}   // namespace nebula

#endif
