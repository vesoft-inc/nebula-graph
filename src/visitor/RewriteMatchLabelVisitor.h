/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VISITOR_REWRITEMATCHLABELVISITOR_H_
#define VISITOR_REWRITEMATCHLABELVISITOR_H_

#include <vector>
#include <functional>
#include "visitor/ExprVisitorImpl.h"
#include "context/ast/QueryAstContext.h"

namespace nebula {
namespace graph {

class RewriteMatchLabelVisitor final : public ExprVisitorImpl {
public:
    using Rewriter = std::function<Expression *(const Expression *)>;
    explicit RewriteMatchLabelVisitor(Rewriter rewriter,
                                      const std::unordered_map<std::string, AliasType> aliases = {})
        : rewriter_(std::move(rewriter)), aliases_(aliases) {}

private:
    bool ok() const override {
        return true;
    }

    bool isLabel(const Expression *expr) {
        if (aliases_.empty()) {
            return expr->kind() == Expression::Kind::kLabel ||
                   expr->kind() == Expression::Kind::kLabelAttribute;
        } else if (expr->kind() == Expression::Kind::kLabel) {
            auto lb = static_cast<const LabelExpression *>(expr);
            return std::find_if(aliases_.cbegin(),
                                aliases_.cend(),
                                [lb](const std::pair<std::string, AliasType> &pair) {
                                    return *lb->name() == pair.first;
                                }) != aliases_.cend();
        } else if (expr->kind() == Expression::Kind::kLabelAttribute) {
            auto la = static_cast<const LabelAttributeExpression *>(expr);
            return std::find_if(aliases_.cbegin(),
                                aliases_.cend(),
                                [la](const std::pair<std::string, AliasType> &pair) {
                                    return *la->left()->name() == pair.first;
                                }) != aliases_.cend();
        } else {
            return false;
        }
        return false;
    }

private:
    using ExprVisitorImpl::visit;
    void visit(TypeCastingExpression*) override;
    void visit(UnaryExpression*) override;
    void visit(FunctionCallExpression*) override;
    void visit(ListExpression*) override;
    void visit(SetExpression*) override;
    void visit(MapExpression*) override;
    void visit(CaseExpression *) override;
    void visit(ConstantExpression *) override {}
    void visit(LabelExpression*) override {}
    void visit(AttributeExpression*) override;
    void visit(UUIDExpression*) override {}
    void visit(LabelAttributeExpression*) override {}
    void visit(VariableExpression*) override {}
    void visit(VersionedVariableExpression*) override {}
    void visit(TagPropertyExpression*) override {}
    void visit(EdgePropertyExpression*) override {}
    void visit(InputPropertyExpression*) override {}
    void visit(VariablePropertyExpression*) override {}
    void visit(DestPropertyExpression*) override {}
    void visit(SourcePropertyExpression*) override {}
    void visit(EdgeSrcIdExpression*) override {}
    void visit(EdgeTypeExpression*) override {}
    void visit(EdgeRankExpression*) override {}
    void visit(EdgeDstIdExpression*) override {}
    void visit(VertexExpression*) override {}
    void visit(EdgeExpression*) override {}
    void visit(ColumnExpression*) override {}
    void visit(ListComprehensionExpression*) override;

    void visitBinaryExpr(BinaryExpression *) override;

    std::vector<std::unique_ptr<Expression>>
    rewriteExprList(const std::vector<std::unique_ptr<Expression>> &list);

private:
    Rewriter                                            rewriter_;
    const std::unordered_map<std::string, AliasType>    aliases_;
};

}   // namespace graph
}   // namespace nebula


#endif  // VISITOR_REWRITEMATCHLABELVISITOR_H_
