/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "ExtractPropExprVisitor.h"

namespace nebula {
namespace graph {

void ExtractPropExprVisitor::visit(ConstantExpression* expr) {
    UNUSED(expr);
}

void ExtractPropExprVisitor::visit(VertexExpression* expr) {
    UNUSED(expr);
}

void ExtractPropExprVisitor::visit(EdgeExpression* expr) {
    UNUSED(expr);
}

void ExtractPropExprVisitor::visit(VariableExpression* expr) {
    reportError(expr);
}

void ExtractPropExprVisitor::visit(SubscriptExpression* expr) {
    reportError(expr);
}

void ExtractPropExprVisitor::visit(LabelExpression* expr) {
    reportError(expr);
}

void ExtractPropExprVisitor::visit(LabelAttributeExpression* expr) {
    reportError(expr);
}

void ExtractPropExprVisitor::visit(VersionedVariableExpression* expr) {
    reportError(expr);
}

void ExtractPropExprVisitor::visit(SubscriptExpression* expr) {
    reportError(expr);
}

void ExtractPropExprVisitor::visit(UUIDExpression* expr) {
    reportError(expr);
}

void ExtractPropExprVisitor::visit(UnaryExpression* expr) {
    switch (expr->kind()) {
        case Expression::Kind::kUnaryPlus:
        case Expression::Kind::kUnaryNegate:
        case Expression::Kind::kUnaryNot: {
            expr->operand()->accept(this);
            break;
        }
        case Expression::Kind::kUnaryDecr:
        case Expression::Kind::kUnaryIncr: {
            reportError(expr);
        }
    }
}

void ExtractPropExprVisitor::visitPropertyExpr(PropertyExpression* expr) {
    void* propExpr = nullptr;
    switch (expr->kind()) {
        case Expression::Kind::kInputProperty: {
            propExpr = static_cast<const InputPropertyExpression*>(expr);
            break;
        }
        case Expression::Kind::kVarProperty: {
            propExpr = static_cast<const VariablePropertyExpression*>(expr);
            break;
        }
        default: {
            LOG(FATAL) << "Invalid Kind " << expr->kind();
        }
    }
    auto found = propExprColMap_.find(propExpr->toString());
    if (found == propExprColMap_.end()) {
        auto newExpr = propExpr->clone();
        auto col = new YieldColumn(newExpr.release(), new std::string(*expr->prop()));
        propExprColMap_.emplace(propExpr->toString(), col);
        inputPropCols_->addColumn(col);
    }
}

void ExtractPropExprVisitor::visit(VariablePropertyExpression* expr) {
    visitPropertyExpr(expr);
}

void ExtractPropExprVisitor::visit(InputPropertyExpression* expr) {
    visitPropertyExpr(expr);
}

void ExtractPropExprVisitor::visitVertexEdgePropExpr(PropertyExpression* expr) {
    void* propExpr = nullptr;
    switch (expr->kind()) {
        case Expression::Kind::kTagProperty: {
            propExpr = static_cast<const TagPropertyExpression*>(expr);
            break;
        }
        case Expression::Kind::kSrcProperty: {
            propExpr = static_cast<const SourcePropertyExpression*>(expr);
            break;
        }
        case Expression::Kind::kEdgeProperty: {
            propExpr = static_cast<const EdgePropertyExpression*>(expr);
            break;
        }
        case Expression::Kind::kEdgeSrc: {
            propExpr = static_cast<const EdgeSrcIdExpression*>(expr);
            break;
        }
        case Expression::Kind::kEdgeType: {
            propExpr = static_cast<const EdgeTypeExpression*>(expr);
            break;
        }
        case Expression::Kind::kEdgeRank: {
            propExpr = static_cast<const EdgeRankExpression*>(expr);
            break;
        }
        case Expression::Kind::kEdgeDst: {
            propExpr = static_cast<const EdgeDstIdExpression*>(expr);
            break;
        }
        default: {
            LOG(FATAL) << "Invalid Kind " << expr->kind();
        }
    }
    auto found = propExprColMap_.find(propExpr->toString());
    if (found == propExprColMap_.end()) {
        auto newExpr = propExpr->clone();
        auto col =
            new YieldColumn(newExpr.release(), new std::string(vctx_->anonColGen_->getCol()));
        propExprColMap_.emplace(propExpr->toString(), col);
        srcAndEdgePropCols_->addColumn(col);
    }
}

void ExtractPropExprVisitor::visit(TagPropertyExpression* expr) {
    visitVertexEdgePropExpr(expr);
}

void ExtractPropExprVisitor::visit(SourcePropertyExpression* expr) {
    visitVertexEdgePropExpr(expr);
}

void ExtractPropExprVisitor::visit(EdgePropertyExpression* expr) {
    visitVertexEdgePropExpr(expr);
}

void ExtractPropExprVisitor::visit(EdgeSrcIdExpression* expr) {
    visitVertexEdgePropExpr(expr);
}

void ExtractPropExprVisitor::visit(EdgeTypeExpression* expr) {
    visitVertexEdgePropExpr(expr);
}

void ExtractPropExprVisitor::visit(EdgeRankExpression* expr) {
    visitVertexEdgePropExpr(expr);
}

void ExtractPropExprVisitor::visit(EdgeDstIdExpression* expr) {
    visitVertexEdgePropExpr(expr);
}

void ExtractPropExprVisitor::visit(DestPropertyExpression* expr) {
    auto found = propExprColMap_.find(expr->toString());
    if (found == propExprColMap_.end()) {
        auto newExpr = expr->clone();
        auto col =
            new YieldColumn(newExpr.release(), new std::string(vctx_->anonColGen_->getCol()));
        propExprColMap_.emplace(expr->toString(), col);
        dstPropCols_->addColumn(col);
    }
}

void ExtractPropExprVisitor::reportError(const Expression *expr) {
    std::stringstream ss;
    ss << "Not supported expression `" << expr->toString() << "' for ExtractPropsExpression.";
    status_ = Status::SemanticError(ss.str());
}

}   // namespace graph
}   // namespace nebula
