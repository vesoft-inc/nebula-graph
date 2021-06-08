/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <sstream>

#include "context/QueryContext.h"
#include "visitor/DeduceVertexEdgePropsVisitor.h"

namespace nebula {
namespace graph {

void DeduceVertexEdgePropsVisitor::visit(LabelExpression* expr) {
    auto alias = aliases_.find(expr->name());
    if (alias != aliases_.end()) {
        if (alias->second == AliasType::kNode) {
            vertexEdgeProps_.addVertexProp(expr->name());
        } else if (alias->second == AliasType::kEdge) {
            vertexEdgeProps_.addEdgeProp(expr->name());
        }
    }
}

void DeduceVertexEdgePropsVisitor::visit(LabelAttributeExpression* expr) {
    auto alias = aliases_.find(expr->left()->name());
    if (alias != aliases_.end()) {
        if (alias->second == AliasType::kNode) {
            vertexEdgeProps_.addVertexProp(expr->left()->name(), expr->right()->value().getStr());
        } else if (alias->second == AliasType::kEdge) {
            vertexEdgeProps_.addEdgeProp(expr->left()->name(), expr->right()->value().getStr());
        }
    }
}

// optimize the case like `id(v)`
void DeduceVertexEdgePropsVisitor::visit(FunctionCallExpression* expr) {
    if (expr->isFunc("id")) {
        // only get vid so don't need collect properties
        return;
    }
    ExprVisitorImpl::visit(expr);
}

void DeduceVertexEdgePropsVisitor::visit(EdgePropertyExpression* expr) {
    UNUSED(expr);
}

void DeduceVertexEdgePropsVisitor::visit(TagPropertyExpression* expr) {
    UNUSED(expr);
}

void DeduceVertexEdgePropsVisitor::visit(InputPropertyExpression* expr) {
    UNUSED(expr);
}

void DeduceVertexEdgePropsVisitor::visit(VariablePropertyExpression* expr) {
    UNUSED(expr);
}

void DeduceVertexEdgePropsVisitor::visit(SourcePropertyExpression* expr) {
    UNUSED(expr);
}

void DeduceVertexEdgePropsVisitor::visit(DestPropertyExpression* expr) {
    UNUSED(expr);
}

void DeduceVertexEdgePropsVisitor::visit(EdgeSrcIdExpression* expr) {
    UNUSED(expr);
}

void DeduceVertexEdgePropsVisitor::visit(EdgeTypeExpression* expr) {
    UNUSED(expr);
}

void DeduceVertexEdgePropsVisitor::visit(EdgeRankExpression* expr) {
    UNUSED(expr);
}

void DeduceVertexEdgePropsVisitor::visit(EdgeDstIdExpression* expr) {
    UNUSED(expr);
}

void DeduceVertexEdgePropsVisitor::visit(UUIDExpression* expr) {
    UNUSED(expr);
}

void DeduceVertexEdgePropsVisitor::visit(VariableExpression* expr) {
    UNUSED(expr);
}

void DeduceVertexEdgePropsVisitor::visit(VersionedVariableExpression* expr) {
    UNUSED(expr);
}

void DeduceVertexEdgePropsVisitor::visit(AttributeExpression* expr) {
    UNUSED(expr);
}

void DeduceVertexEdgePropsVisitor::visit(ConstantExpression* expr) {
    UNUSED(expr);
}

void DeduceVertexEdgePropsVisitor::visit(VertexExpression* expr) {
    UNUSED(expr);
}

void DeduceVertexEdgePropsVisitor::visit(EdgeExpression* expr) {
    UNUSED(expr);
}

void DeduceVertexEdgePropsVisitor::visit(ColumnExpression* expr) {
    UNUSED(expr);
}

void DeduceVertexEdgePropsVisitor::reportError(const Expression* expr) {
    std::stringstream ss;
    ss << "Not supported expression `" << expr->toString() << "' for props deduction.";
    status_ = Status::SemanticError(ss.str());
}

}   // namespace graph
}   // namespace nebula
