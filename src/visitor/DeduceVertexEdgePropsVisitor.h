/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VISITOR_DEDUCEVERTEXEDGEPROPSVISITOR_H_
#define VISITOR_DEDUCEVERTEXEDGEPROPSVISITOR_H_

#include <unordered_set>

#include "common/base/Status.h"
#include "common/thrift/ThriftTypes.h"
#include "context/ast/CypherAstContext.h"
#include "visitor/ExprVisitorImpl.h"

namespace nebula {

class Expression;

namespace graph {

class QueryContext;

// It's used for cypher properties
class VertexEdgeProps final {
public:
    struct AllProps {
        bool operator==(const AllProps&) const {
            return true;
        }
    };
    using Props = std::variant<AllProps, std::set<folly::StringPiece>>;
    // Alias -> Props
    using AliasProps = std::unordered_map<std::string, Props>;

    VertexEdgeProps() = default;
    VertexEdgeProps(AliasProps vertexProps, AliasProps edgeProps)
        : vertexProps_(std::move(vertexProps)), edgeProps_(std::move(edgeProps)) {}

    bool operator==(const VertexEdgeProps& rhs) const {
        if (vertexProps_ != rhs.vertexProps_) {
            return false;
        }
        return edgeProps_ == rhs.edgeProps_;
    }

    void addVertexProp(const std::string& vertex, const std::string& prop) {
        auto find = vertexProps_.find(vertex);
        if (find == vertexProps_.end()) {
            vertexProps_[vertex] = std::set<folly::StringPiece>();
            std::get<std::set<folly::StringPiece>>(vertexProps_[vertex]).emplace(prop);
        } else {
            if (!std::holds_alternative<AllProps>(find->second)) {
                std::get<std::set<folly::StringPiece>>(find->second).emplace(prop);
            }
        }
    }

    void addVertexProp(const std::string& vertex) {
        vertexProps_[vertex] = AllProps{};
    }

    const auto& vertexProps() const {
        return vertexProps_;
    }

    void addEdgeProp(const std::string& edge, const std::string& prop) {
        auto find = edgeProps_.find(edge);
        if (find == edgeProps_.end()) {
            edgeProps_[edge] = std::set<folly::StringPiece>();
            std::get<std::set<folly::StringPiece>>(edgeProps_[edge]).emplace(prop);
        } else {
            if (!std::holds_alternative<AllProps>(find->second)) {
                std::get<std::set<folly::StringPiece>>(find->second).emplace(prop);
            }
        }
    }

    void addEdgeProp(const std::string& edge) {
        edgeProps_[edge] = AllProps{};
    }

    const auto& edgeProps() const {
        return edgeProps_;
    }

private:
    AliasProps vertexProps_;
    AliasProps edgeProps_;
};

class DeduceVertexEdgePropsVisitor : public ExprVisitorImpl {
public:
    DeduceVertexEdgePropsVisitor(VertexEdgeProps& vertexEdgeProps,
                                 const std::unordered_map<std::string, AliasType>& aliases)
        : vertexEdgeProps_(vertexEdgeProps), aliases_(aliases) {}

    bool ok() const override {
        return status_.ok();
    }

    const Status& status() const {
        return status_;
    }

private:
    using ExprVisitorImpl::visit;
    void visit(LabelExpression* expr) override;
    void visit(LabelAttributeExpression* expr) override;
    void visit(FunctionCallExpression* expr) override;
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
    void visit(AttributeExpression* expr) override;
    void visit(ConstantExpression* expr) override;
    void visit(VertexExpression* expr) override;
    void visit(EdgeExpression* expr) override;
    void visit(ColumnExpression* expr) override;

    void reportError(const Expression* expr);

    VertexEdgeProps& vertexEdgeProps_;
    const std::unordered_map<std::string, AliasType>& aliases_;
    Status status_;
};

}   // namespace graph
}   // namespace nebula

#endif   // VISITOR_DEDUCEVERTEXEDGEPROPSVISITOR_H_
