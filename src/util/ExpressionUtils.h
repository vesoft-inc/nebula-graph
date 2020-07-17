/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/expression/Expression.h"

namespace nebula {
namespace graph {

class ExpressionUtils {
public:
    explicit ExpressionUtils(...) = delete;

    // return true for continue, false return directly
    using Visitor = std::function<bool(const Expression*)>;

    // preorder traverse in fact for tail call optimization
    // if want to do some thing like eval, don't try it
    static bool traverse(const Expression* expr, Visitor visitor);

    template <typename T, typename = std::enable_if_t<std::is_same<T, Expression::Kind>::value>>
    static bool isAnyKind(const Expression* expr, T k) {
        return expr->kind() == k;
    }

    template <typename T,
              typename... Ts,
              typename = std::enable_if_t<std::is_same<T, Expression::Kind>::value>>
    static bool isAnyKind(const Expression* expr, T k, Ts... ts) {
        return expr->kind() == k || isAnyKind(expr, ts...);
    }

    // null for not found
    template <typename... Ts,
              typename = std::enable_if_t<
                  std::is_same<Expression::Kind, std::common_type_t<Ts...>>::value>>
    static const Expression* findAnyKind(const Expression* self, Ts... ts) {
        const Expression* found = nullptr;
        traverse(self, [pack = std::make_tuple(ts...), &found](const Expression* expr) {
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
        traverse(self, [pack = std::make_tuple(ts...), &exprs](const Expression* expr) {
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
                           Expression::Kind::kEdgeProperty,
                           Expression::Kind::kDstProperty,
                           Expression::Kind::kSrcProperty,
                           Expression::Kind::kEdgeSrc,
                           Expression::Kind::kEdgeType,
                           Expression::Kind::kEdgeRank,
                           Expression::Kind::kEdgeDst);
    }
};

}   // namespace graph
}   // namespace nebula
