/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/DeducePropsVisitor.h"

#include <sstream>

#include "common/expression/LabelExpression.h"
#include "common/expression/SymbolPropertyExpression.h"
#include "common/expression/UUIDExpression.h"
#include "common/expression/VariableExpression.h"
#include "context/QueryContext.h"
#include "validator/ExpressionProps.h"

namespace nebula {
namespace graph {

DeducePropsVisitor::DeducePropsVisitor(QueryContext *qctx,
                                       GraphSpaceID space,
                                       ExpressionProps *exprProps)
    : qctx_(qctx), space_(space), exprProps_(exprProps) {
    DCHECK(qctx != nullptr);
    DCHECK(exprProps != nullptr);
}

void DeducePropsVisitor::visitEdgePropertyExpr(EdgePropertyExpression *expr) {
    visitEdgeSymPropExpr(expr);
}

void DeducePropsVisitor::visitTagPropertyExpr(TagPropertyExpression *expr) {
    auto status = qctx_->schemaMng()->toTagID(space_, *expr->sym());
    if (!status.ok()) {
        status_ = std::move(status).status();
        return;
    }
    exprProps_->insertTagProp(status.value(), *expr->prop());
}

void DeducePropsVisitor::visitInputPropertyExpr(InputPropertyExpression *expr) {
    exprProps_->insertInputProp(*expr->prop());
}

void DeducePropsVisitor::visitVariablePropertyExpr(VariablePropertyExpression *expr) {
    exprProps_->insertVarProp(*expr->sym(), *expr->prop());
}

void DeducePropsVisitor::visitDestPropertyExpr(DestPropertyExpression *expr) {
    auto status = qctx_->schemaMng()->toTagID(space_, *expr->sym());
    if (!status.ok()) {
        status_ = std::move(status).status();
        return;
    }
    exprProps_->insertDstTagProp(std::move(status).value(), *expr->prop());
}

void DeducePropsVisitor::visitSourcePropertyExpr(SourcePropertyExpression *expr) {
    auto status = qctx_->schemaMng()->toTagID(space_, *expr->sym());
    if (!status.ok()) {
        status_ = std::move(status).status();
        return;
    }
    exprProps_->insertSrcTagProp(std::move(status).value(), *expr->prop());
}

void DeducePropsVisitor::visitEdgeSrcIdExpr(EdgeSrcIdExpression *expr) {
    visitEdgeSymPropExpr(expr);
}

void DeducePropsVisitor::visitEdgeTypeExpr(EdgeTypeExpression *expr) {
    visitEdgeSymPropExpr(expr);
}

void DeducePropsVisitor::visitEdgeRankExpr(EdgeRankExpression *expr) {
    visitEdgeSymPropExpr(expr);
}

void DeducePropsVisitor::visitEdgeDstIdExpr(EdgeDstIdExpression *expr) {
    visitEdgeSymPropExpr(expr);
}

void DeducePropsVisitor::visitUUIDExpr(UUIDExpression *expr) {
    reportError(expr);
}

void DeducePropsVisitor::visitVariableExpr(VariableExpression *expr) {
    reportError(expr);
}

void DeducePropsVisitor::visitVersionedVariableExpr(VersionedVariableExpression *expr) {
    reportError(expr);
}

void DeducePropsVisitor::visitLabelExpr(LabelExpression *expr) {
    reportError(expr);
}

void DeducePropsVisitor::visitSymbolPropertyExpr(SymbolPropertyExpression *expr) {
    reportError(expr);
}

void DeducePropsVisitor::visitEdgeSymPropExpr(SymbolPropertyExpression *expr) {
    auto status = qctx_->schemaMng()->toEdgeType(space_, *expr->sym());
    if (!status.ok()) {
        status_ = std::move(status).status();
        return;
    }
    exprProps_->insertEdgeProp(status.value(), *expr->prop());
}

void DeducePropsVisitor::reportError(const Expression *expr) {
    std::stringstream ss;
    ss << "Not supported expression `" << expr->toString() << "' for type deduction.";
    status_ = Status::SemanticError(ss.str());
}

}   // namespace graph
}   // namespace nebula
