/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/expression/BinaryExpression.h"
#include "common/expression/Expression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/SymbolPropertyExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "common/expression/UnaryExpression.h"

namespace nebula {
namespace graph {

class ExpressionUtils {
public:
    explicit ExpressionUtils(...) = delete;

    // return true for continue, false return directly
    using Visitor = std::function<bool(const Expression*)>;
    using MutableVisitor = std::function<bool(Expression*)>;

    // preorder traverse in fact for tail call optimization
    // if want to do some thing like eval, don't try it
    template <typename T,
              typename V,
              typename = std::enable_if_t<std::is_same<std::remove_const_t<T>, Expression>::value>>
    static bool traverse(T* expr, V visitor) {
        if (!visitor(expr)) {
            return false;
        }
        switch (expr->kind()) {
            case Expression::Kind::kDstProperty:
            case Expression::Kind::kSrcProperty:
            case Expression::Kind::kTagProperty:
            case Expression::Kind::kEdgeProperty:
            case Expression::Kind::kEdgeSrc:
            case Expression::Kind::kEdgeType:
            case Expression::Kind::kEdgeRank:
            case Expression::Kind::kEdgeDst:
            case Expression::Kind::kInputProperty:
            case Expression::Kind::kVarProperty:
            case Expression::Kind::kUUID:
            case Expression::Kind::kVar:
            case Expression::Kind::kVersionedVar:
            case Expression::Kind::kSymProperty:
            case Expression::Kind::kConstant: {
                return true;
            }
            case Expression::Kind::kAdd:
            case Expression::Kind::kMinus:
            case Expression::Kind::kMultiply:
            case Expression::Kind::kDivision:
            case Expression::Kind::kMod:
            case Expression::Kind::kRelEQ:
            case Expression::Kind::kRelNE:
            case Expression::Kind::kRelLT:
            case Expression::Kind::kRelLE:
            case Expression::Kind::kRelGT:
            case Expression::Kind::kRelGE:
            case Expression::Kind::kLogicalAnd:
            case Expression::Kind::kLogicalOr:
            case Expression::Kind::kLogicalXor: {
                auto biExpr = exprCast<BinaryExpression>(expr);
                if (!traverse(biExpr->left(), visitor)) {
                    return false;
                }
                return traverse(biExpr->right(), visitor);
            }
            case Expression::Kind::kRelIn:
            case Expression::Kind::kUnaryIncr:
            case Expression::Kind::kUnaryDecr:
            case Expression::Kind::kUnaryPlus:
            case Expression::Kind::kUnaryNegate:
            case Expression::Kind::kUnaryNot: {
                auto unaryExpr = exprCast<UnaryExpression>(expr);
                return traverse(unaryExpr->operand(), visitor);
            }
            case Expression::Kind::kTypeCasting: {
                auto typeCastingExpr = exprCast<TypeCastingExpression>(expr);
                return traverse(typeCastingExpr->operand(), visitor);
            }
            case Expression::Kind::kFunctionCall: {
                auto funcExpr = exprCast<FunctionCallExpression>(expr);
                for (auto& arg : funcExpr->args()->args()) {
                    if (!traverse(arg.get(), visitor)) {
                        return false;
                    }
                }
                return true;
            }
        }
        DLOG(FATAL) << "Impossible expression kind " << static_cast<int>(expr->kind());
        return false;
    }

    template <typename T, typename = std::enable_if_t<std::is_same<T, Expression::Kind>::value>>
    static inline bool isAnyKind(const Expression* expr, T k) {
        return expr->kind() == k;
    }

    template <typename T,
              typename... Ts,
              typename = std::enable_if_t<std::is_same<T, Expression::Kind>::value>>
    static inline bool isAnyKind(const Expression* expr, T k, Ts... ts) {
        return expr->kind() == k || isAnyKind(expr, ts...);
    }

    // null for not found
    template <typename... Ts,
              typename = std::enable_if_t<
                  std::is_same<Expression::Kind, std::common_type_t<Ts...>>::value>>
    static const Expression* findAnyKind(const Expression* self, Ts... ts) {
        const Expression* found = nullptr;
        traverse(self, [pack = std::make_tuple(ts...), &found](const Expression* expr) -> bool {
            auto bind = [expr](Ts... ts_) { return isAnyKind(expr, ts_...); };
            if (folly::apply(bind, pack)) {
                found = expr;
                return false;   // Already find so return now
            }
            return true;   // Not find so continue traverse
        });
        return found;
    }

    // Find all expression fit any kind
    // Empty for not found any one
    template <typename... Ts,
              typename = std::enable_if_t<
                  std::is_same<Expression::Kind, std::common_type_t<Ts...>>::value>>
    static std::vector<const Expression*> findAnyKindInAll(const Expression* self, Ts... ts) {
        std::vector<const Expression*> exprs;
        traverse(self, [pack = std::make_tuple(ts...), &exprs](const Expression* expr) -> bool {
            auto bind = [expr](Ts... ts_) { return isAnyKind(expr, ts_...); };
            if (folly::apply(bind, pack)) {
                exprs.emplace_back(expr);
            }
            return true;   // Not return always to traverse entire expression tree
        });
        return exprs;
    }

    template <typename... Ts,
              typename = std::enable_if_t<
                  std::is_same<Expression::Kind, std::common_type_t<Ts...>>::value>>
    static bool hasAnyKind(const Expression* expr, Ts... ts) {
        return findAnyKind(expr, ts...) != nullptr;
    }

    // Require data from input/variable
    static bool hasInput(const Expression* expr) {
        return hasAnyKind(expr,
                          Expression::Kind::kInputProperty,
                          Expression::Kind::kVarProperty,
                          Expression::Kind::kVar,
                          Expression::Kind::kVersionedVar);
    }

    // require data from graph storage
    static const Expression* findStorage(const Expression* expr) {
        return findAnyKind(expr,
                           Expression::Kind::kSymProperty,
                           Expression::Kind::kTagProperty,
                           Expression::Kind::kEdgeProperty,
                           Expression::Kind::kDstProperty,
                           Expression::Kind::kSrcProperty,
                           Expression::Kind::kEdgeSrc,
                           Expression::Kind::kEdgeType,
                           Expression::Kind::kEdgeRank,
                           Expression::Kind::kEdgeDst);
    }

    static std::vector<const Expression*> findAllStorage(const Expression* expr) {
        return findAnyKindInAll(expr,
                                Expression::Kind::kSymProperty,
                                Expression::Kind::kTagProperty,
                                Expression::Kind::kEdgeProperty,
                                Expression::Kind::kDstProperty,
                                Expression::Kind::kSrcProperty,
                                Expression::Kind::kEdgeSrc,
                                Expression::Kind::kEdgeType,
                                Expression::Kind::kEdgeRank,
                                Expression::Kind::kEdgeDst);
    }

    static std::vector<const Expression*> findAllInputVariableProp(const Expression* expr) {
        return findAnyKindInAll(
            expr, Expression::Kind::kInputProperty, Expression::Kind::kVarProperty);
    }

    static bool hasStorage(const Expression* expr) {
        return findStorage(expr) != nullptr;
    }

    static bool isStorage(const Expression* expr) {
        return isAnyKind(expr,
                         Expression::Kind::kSymProperty,
                         Expression::Kind::kTagProperty,
                         Expression::Kind::kEdgeProperty,
                         Expression::Kind::kDstProperty,
                         Expression::Kind::kSrcProperty,
                         Expression::Kind::kEdgeSrc,
                         Expression::Kind::kEdgeType,
                         Expression::Kind::kEdgeRank,
                         Expression::Kind::kEdgeDst);
    }

    static bool isConstExpr(const Expression* expr) {
        return !hasAnyKind(expr,
                           Expression::Kind::kInputProperty,
                           Expression::Kind::kVarProperty,
                           Expression::Kind::kVar,
                           Expression::Kind::kVersionedVar,

                           Expression::Kind::kSymProperty,
                           Expression::Kind::kTagProperty,
                           Expression::Kind::kEdgeProperty,
                           Expression::Kind::kDstProperty,
                           Expression::Kind::kSrcProperty,
                           Expression::Kind::kEdgeSrc,
                           Expression::Kind::kEdgeType,
                           Expression::Kind::kEdgeRank,
                           Expression::Kind::kEdgeDst);
    }

    // determine the detail about symbol property expression
    template <typename To,
              typename = std::enable_if_t<std::is_same<To, EdgePropertyExpression>::value ||
                                          std::is_same<To, TagPropertyExpression>::value>>
    static void transAllSymbolPropertyExpr(Expression* expr) {
        traverse(expr, [](Expression* current) -> bool {
            switch (current->kind()) {
                case Expression::Kind::kDstProperty:
                case Expression::Kind::kSrcProperty:
                case Expression::Kind::kSymProperty:
                case Expression::Kind::kTagProperty:
                case Expression::Kind::kEdgeProperty:
                case Expression::Kind::kEdgeSrc:
                case Expression::Kind::kEdgeType:
                case Expression::Kind::kEdgeRank:
                case Expression::Kind::kEdgeDst:
                case Expression::Kind::kInputProperty:
                case Expression::Kind::kVarProperty:
                case Expression::Kind::kUUID:
                case Expression::Kind::kVar:
                case Expression::Kind::kVersionedVar:
                case Expression::Kind::kConstant: {
                    return true;
                }
                case Expression::Kind::kAdd:
                case Expression::Kind::kMinus:
                case Expression::Kind::kMultiply:
                case Expression::Kind::kDivision:
                case Expression::Kind::kMod:
                case Expression::Kind::kRelEQ:
                case Expression::Kind::kRelNE:
                case Expression::Kind::kRelLT:
                case Expression::Kind::kRelLE:
                case Expression::Kind::kRelGT:
                case Expression::Kind::kRelGE:
                case Expression::Kind::kLogicalAnd:
                case Expression::Kind::kLogicalOr:
                case Expression::Kind::kLogicalXor: {
                    auto* biExpr = exprCast<BinaryExpression>(current);
                    if (biExpr->left()->kind() == Expression::Kind::kSymProperty) {
                        auto* symbolExpr = static_cast<SymbolPropertyExpression*>(biExpr->left());
                        biExpr->setLeft(transSymbolPropertyExpression<To>(symbolExpr));
                    }
                    if (biExpr->right()->kind() == Expression::Kind::kSymProperty) {
                        auto* symbolExpr = static_cast<SymbolPropertyExpression*>(biExpr->right());
                        biExpr->setRight(transSymbolPropertyExpression<To>(symbolExpr));
                    }
                    return true;
                }
                case Expression::Kind::kRelIn:
                case Expression::Kind::kUnaryIncr:
                case Expression::Kind::kUnaryDecr:
                case Expression::Kind::kUnaryPlus:
                case Expression::Kind::kUnaryNegate:
                case Expression::Kind::kUnaryNot: {
                    auto* unaryExpr = exprCast<UnaryExpression>(current);
                    if (unaryExpr->operand()->kind() == Expression::Kind::kSymProperty) {
                        auto* symbolExpr =
                            static_cast<SymbolPropertyExpression*>(unaryExpr->operand());
                        unaryExpr->setOperand(transSymbolPropertyExpression<To>(symbolExpr));
                    }
                    return true;
                }
                case Expression::Kind::kTypeCasting: {
                    auto* typeCastingExpr = exprCast<TypeCastingExpression>(current);
                    if (typeCastingExpr->operand()->kind() == Expression::Kind::kSymProperty) {
                        auto* symbolExpr =
                            static_cast<SymbolPropertyExpression*>(typeCastingExpr->operand());
                        typeCastingExpr->setOperand(transSymbolPropertyExpression<To>(symbolExpr));
                    }
                    return true;
                }
                case Expression::Kind::kFunctionCall: {
                    auto* funcExpr = exprCast<FunctionCallExpression>(current);
                    for (auto& arg : funcExpr->args()->args()) {
                        if (arg->kind() == Expression::Kind::kSymProperty) {
                            auto* symbolExpr = static_cast<SymbolPropertyExpression*>(arg.get());
                            arg.reset(transSymbolPropertyExpression<To>(symbolExpr));
                        }
                    }
                    return true;
                }
            }   // switch
            DLOG(FATAL) << "Impossible expression kind " << static_cast<int>(current->kind());
            return false;
        });   // traverse
    }

    template <typename To,
              typename = std::enable_if_t<std::is_same<To, EdgePropertyExpression>::value ||
                                          std::is_same<To, TagPropertyExpression>::value>>
    static To* transSymbolPropertyExpression(SymbolPropertyExpression* expr) {
        return new To(new std::string(std::move(*expr->sym())),
                      new std::string(std::move(*expr->prop())));
    }

private:
    template <typename Expr,
              typename = std::enable_if_t<std::is_same<Expr, UnaryExpression>::value ||
                                          std::is_same<Expr, BinaryExpression>::value ||
                                          std::is_same<Expr, FunctionCallExpression>::value ||
                                          std::is_same<Expr, TypeCastingExpression>::value>>
    static Expr* exprCast(Expression* expr) {
        UNUSED(DCHECK_NOTNULL(dynamic_cast<std::remove_const_t<Expr>*>(expr)));
        return static_cast<std::remove_const_t<Expr>*>(expr);
    }

    template <typename Expr,
              typename = std::enable_if_t<std::is_same<Expr, UnaryExpression>::value ||
                                          std::is_same<Expr, BinaryExpression>::value ||
                                          std::is_same<Expr, FunctionCallExpression>::value ||
                                          std::is_same<Expr, TypeCastingExpression>::value>>
    static const Expr* exprCast(const Expression* expr) {
        UNUSED(DCHECK_NOTNULL(dynamic_cast<const Expr*>(expr)));
        return static_cast<const Expr*>(expr);
    }
};

}   // namespace graph
}   // namespace nebula
