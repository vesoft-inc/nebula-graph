/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VISITOR_REWRITELISTCOMPREHENSIONLABELVISITOR_H_
#define VISITOR_REWRITELISTCOMPREHENSIONLABELVISITOR_H_

#include <functional>
#include <vector>
#include "visitor/ExprVisitorImpl.h"

namespace nebula {
namespace graph {

class RewriteListComprehensionLabelVisitor final : public ExprVisitorImpl {
public:
    explicit RewriteListComprehensionLabelVisitor(const std::string &oldVarName,
                                                  const std::string &newVarName)
        : oldVarName_(oldVarName), newVarName_(newVarName) {}

private:
    bool ok() const override {
        return true;
    }

    static bool isLabel(const Expression *expr) {
        return expr->kind() == Expression::Kind::kLabel ||
               expr->kind() == Expression::Kind::kLabelAttribute;
    }

private:
    using ExprVisitorImpl::visit;
    void visit(TypeCastingExpression *) override;
    void visit(UnaryExpression *) override;
    void visit(FunctionCallExpression *) override;
    void visit(ListExpression *) override;
    void visit(SetExpression *) override;
    void visit(MapExpression *) override;
    void visit(CaseExpression *) override;
    void visit(ConstantExpression *) override {}
    void visit(LabelExpression *) override {}
    void visit(AttributeExpression *) override;
    void visit(UUIDExpression *) override {}
    void visit(LabelAttributeExpression *) override {}
    void visit(VariableExpression *) override {}
    void visit(VersionedVariableExpression *) override {}
    void visit(TagPropertyExpression *) override {}
    void visit(EdgePropertyExpression *) override {}
    void visit(InputPropertyExpression *) override {}
    void visit(VariablePropertyExpression *) override {}
    void visit(DestPropertyExpression *) override {}
    void visit(SourcePropertyExpression *) override {}
    void visit(EdgeSrcIdExpression *) override {}
    void visit(EdgeTypeExpression *) override {}
    void visit(EdgeRankExpression *) override {}
    void visit(EdgeDstIdExpression *) override {}
    void visit(VertexExpression *) override {}
    void visit(EdgeExpression *) override {}
    void visit(ColumnExpression *) override {}

    void visitBinaryExpr(BinaryExpression *) override;

    std::vector<std::unique_ptr<Expression>> rewriteExprList(
        const std::vector<std::unique_ptr<Expression>> &list);

    Expression *rewriteLabel(const Expression *expr);

private:
    std::string oldVarName_;
    std::string newVarName_;
};

}   // namespace graph
}   // namespace nebula

#endif   // VISITOR_REWRITELISTCOMPREHENSIONLABELVISITOR_H_
