/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef _UTIL_EXPRESSION_UTILS_H_
#define _UTIL_EXPRESSION_UTILS_H_

#include "common/expression/BinaryExpression.h"
#include "common/expression/Expression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/LabelExpression.h"
#include "common/expression/SymbolPropertyExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "common/expression/UnaryExpression.h"
#include "common/meta/SchemaManager.h"

namespace nebula {
namespace graph {

class ExpressionUtils {
public:
    explicit ExpressionUtils(...) = delete;

    // clone expression
    static std::unique_ptr<Expression> clone(const Expression* expr) {
        // TODO(shylock) optimize
        if (expr == nullptr) {
            return nullptr;
        }
        return CHECK_NOTNULL(Expression::decode(expr->encode()));
    }

    static bool evaluableExpr(const Expression* expr);

    // determine the detail about symbol property expression
    template <typename To,
              typename = std::enable_if_t<std::is_same<To, EdgePropertyExpression>::value ||
                                          std::is_same<To, TagPropertyExpression>::value>>
    static void transAllSymbolPropertyExpr(Expression* expr) {
        switch (expr->kind()) {
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
                return;
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
            case Expression::Kind::kRelIn:
            case Expression::Kind::kRelNotIn:
            case Expression::Kind::kContains:
            case Expression::Kind::kLogicalAnd:
            case Expression::Kind::kLogicalOr:
            case Expression::Kind::kLogicalXor: {
                auto* biExpr = static_cast<BinaryExpression*>(expr);
                if (biExpr->left()->kind() == Expression::Kind::kSymProperty) {
                    auto* symbolExpr = static_cast<SymbolPropertyExpression*>(biExpr->left());
                    biExpr->setLeft(transSymbolPropertyExpression<To>(symbolExpr));
                } else {
                    transAllSymbolPropertyExpr<To>(biExpr->left());
                }
                if (biExpr->right()->kind() == Expression::Kind::kSymProperty) {
                    auto* symbolExpr = static_cast<SymbolPropertyExpression*>(biExpr->right());
                    biExpr->setRight(transSymbolPropertyExpression<To>(symbolExpr));
                } else {
                    transAllSymbolPropertyExpr<To>(biExpr->right());
                }
                return;
            }
            case Expression::Kind::kUnaryIncr:
            case Expression::Kind::kUnaryDecr:
            case Expression::Kind::kUnaryPlus:
            case Expression::Kind::kUnaryNegate:
            case Expression::Kind::kUnaryNot: {
                auto* unaryExpr = static_cast<UnaryExpression*>(expr);
                if (unaryExpr->operand()->kind() == Expression::Kind::kSymProperty) {
                    auto* symbolExpr = static_cast<SymbolPropertyExpression*>(unaryExpr->operand());
                    unaryExpr->setOperand(transSymbolPropertyExpression<To>(symbolExpr));
                } else {
                    transAllSymbolPropertyExpr<To>(unaryExpr->operand());
                }
                return;
            }
            case Expression::Kind::kTypeCasting: {
                auto* typeCastingExpr = static_cast<TypeCastingExpression*>(expr);
                if (typeCastingExpr->operand()->kind() == Expression::Kind::kSymProperty) {
                    auto* symbolExpr =
                        static_cast<SymbolPropertyExpression*>(typeCastingExpr->operand());
                    typeCastingExpr->setOperand(transSymbolPropertyExpression<To>(symbolExpr));
                } else {
                    transAllSymbolPropertyExpr<To>(typeCastingExpr->operand());
                }
                return;
            }
            case Expression::Kind::kFunctionCall: {
                auto* funcExpr = static_cast<FunctionCallExpression*>(expr);
                for (auto& arg : funcExpr->args()->args()) {
                    if (arg->kind() == Expression::Kind::kSymProperty) {
                        auto* symbolExpr = static_cast<SymbolPropertyExpression*>(arg.get());
                        arg.reset(transSymbolPropertyExpression<To>(symbolExpr));
                    } else {
                        transAllSymbolPropertyExpr<To>(arg.get());
                    }
                }
                return;
            }
            case Expression::Kind::kList:   // FIXME(dutor)
            case Expression::Kind::kSet:
            case Expression::Kind::kMap:
            case Expression::Kind::kSubscript:
            case Expression::Kind::kLabel: {
                return;
            }
        }   // switch
        DLOG(FATAL) << "Impossible expression kind " << static_cast<int>(expr->kind());
        return;
    }

    template <typename To,
              typename = std::enable_if_t<std::is_same<To, EdgePropertyExpression>::value ||
                                          std::is_same<To, TagPropertyExpression>::value>>
    static To* transSymbolPropertyExpression(SymbolPropertyExpression* expr) {
        return new To(new std::string(std::move(*expr->sym())),
                      new std::string(std::move(*expr->prop())));
    }
};

class Validator;

class ExpressionTrait {
public:
    explicit ExpressionTrait(const Validator* validator);

    // TODO(shylock) rebult the symbol property expression
    StatusOr<Value::Type> accumulate(const Expression* expr);
    void collect(ExpressionTrait&& r);

    bool isConst() const {
        return inputProps_.empty() && varProps_.empty() && srcTagProps_.empty() &&
               dstTagProps_.empty() && edgeProps_.empty() && tagProps_.empty();
    }

    bool hasInputVarProperty() const {
        return (!inputProps_.empty() || !varProps_.empty());
    }

    bool hasStorageProperty() const {
        return (!srcTagProps_.empty() || !dstTagProps_.empty() || !edgeProps_.empty() ||
                !tagProps_.empty());
    }

    bool hasSrcDstProperty() const {
        return (!srcTagProps_.empty() || !dstTagProps_.empty());
    }

    bool isSubsetOfInput(const std::vector<std::string>& props) const;

    bool isSubsetOfVar(const std::unordered_map<std::string, std::vector<std::string>>& props);

    // properties
    std::vector<std::string> inputProps_;                                  // I.E. `$-.prop`
    std::unordered_map<std::string, std::vector<std::string>> varProps_;   // I.E. `$var.prop`
    std::unordered_map<TagID, std::vector<std::string>> srcTagProps_;      // I.E. `$^.tag.prop`
    std::unordered_map<TagID, std::vector<std::string>> dstTagProps_;      // I.E. `$$.tag.prop`
    std::unordered_map<EdgeType, std::vector<std::string>> edgeProps_;     // I.E. `edge.prop`
    std::unordered_map<TagID, std::vector<std::string>> tagProps_;         // I.E. `tag.prop`

private:
    template <typename T, typename = std::enable_if_t<std::is_base_of<Expression, T>::value>>
    static auto* exprCast(Expression* expr) {
        UNUSED(DCHECK_NOTNULL(dynamic_cast<std::remove_const_t<T>*>(expr)));
        return static_cast<std::remove_const_t<T>*>(expr);
    }

    template <typename T, typename = std::enable_if_t<std::is_base_of<Expression, T>::value>>
    static auto* exprCast(const Expression* expr) {
        UNUSED(DCHECK_NOTNULL(dynamic_cast<const std::remove_const_t<T>*>(expr)));
        return static_cast<const std::remove_const_t<T>*>(expr);
    }

    static StatusOr<Value::Type> edgeFieldType(/*const*/ meta::SchemaManager* schemaMgr,
                                               GraphSpaceID spaceId,
                                               EdgeType edgeType,
                                               const std::string& field);
    static StatusOr<Value::Type> tagFieldType(/*const*/ meta::SchemaManager* schemaMgr,
                                              GraphSpaceID spaceId,
                                              TagID tagId,
                                              const std::string& field);

    // context
    const Validator* validator_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif   // _UTIL_EXPRESSION_UTILS_H_
