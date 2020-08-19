/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef _UTIL_EXPRESSION_UTILS_H_
#define _UTIL_EXPRESSION_UTILS_H_

#include "common/expression/BinaryExpression.h"
#include "common/expression/ContainerExpression.h"
#include "common/expression/Expression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/LabelExpression.h"
#include "common/expression/SymbolPropertyExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "common/expression/UnaryExpression.h"
#include "common/meta/SchemaManager.h"

namespace nebula {
namespace graph {

class Validator;

class ExprVisitor {
public:
    enum class VisitOrder { kPre, kPost };

    explicit ExprVisitor(VisitOrder order) : order_(order) {}

    virtual ~ExprVisitor() = default;

    // follow this signature pretend to override it , or you will got a compile error
    // gain performance by static dispatch
    // virtual Status visit(Expression* expr) = 0;
    // virtual Status visit(const Expression* expr) = 0;

    VisitOrder order() const {
        return order_;
    }

    bool isPreOrder() const {
        return order_ == VisitOrder::kPre;
    }

    bool isPostOrder() const {
        return order_ == VisitOrder::kPost;
    }

protected:
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

private:
    const VisitOrder order_;
};

class EvaluableVisitor : public ExprVisitor {
public:
    EvaluableVisitor() : ExprVisitor(VisitOrder::kPre) {}

    Status visit(const Expression* expr);

    bool evaluable() const {
        return evaluable_;
    }

private:
    bool evaluable_{true};
};

class TypeDeduceVisitor : public ExprVisitor {
public:
    explicit TypeDeduceVisitor(const Validator* validator)
        : ExprVisitor(VisitOrder::kPost), validator_(validator) {}

    Status visit(const Expression* expr);

    Value::Type type() const {
        auto type = subTypes_.top();
        subTypes_.top();
        return type;
    }

private:
    static StatusOr<Value::Type> edgeFieldType(/*const*/ meta::SchemaManager* schemaMgr,
                                               GraphSpaceID spaceId,
                                               EdgeType edgeType,
                                               const std::string& field);
    static StatusOr<Value::Type> tagFieldType(/*const*/ meta::SchemaManager* schemaMgr,
                                              GraphSpaceID spaceId,
                                              TagID tagId,
                                              const std::string& field);

    const Validator* validator_{nullptr};

private:
    std::stack<Value::Type> subTypes_;
};

template <typename T>
struct makeMutPtr {
    using type = T*;
};

template <typename T>
struct makeConstPtr {
    using type = const T*;
};

#define PTR(T) typename makePtr<T>::type

template <typename Expr,
          typename V,
          typename = std::enable_if_t<std::is_base_of<ExprVisitor, V>::value>,
          typename = std::enable_if_t<std::is_same<std::remove_const_t<Expr>, Expression>::value>>
static inline Status preVisit(Expr* expr, V& visitor) {
    if (visitor.isPreOrder()) {
        return visitor.visit(expr);
    }
    return Status::OK();
}

template <typename Expr,
          typename V,
          typename... Visitors,
          typename = std::enable_if_t<std::is_base_of<ExprVisitor, V>::value>,
          typename = std::enable_if_t<std::is_same<std::remove_const_t<Expr>, Expression>::value>>
static inline Status preVisit(Expr* expr, V& visitor, Visitors&... visitors) {
    if (visitor.isPreOrder()) {
        NG_RETURN_IF_ERROR(visitor.visit(expr));
    }
    return preVisit(expr, visitors...);
}

template <typename Expr,
          typename V,
          typename = std::enable_if_t<std::is_base_of<ExprVisitor, V>::value>,
          typename = std::enable_if_t<std::is_same<std::remove_const_t<Expr>, Expression>::value>>
static inline Status postVisit(Expr* expr, V& visitor) {
    if (visitor.isPostOrder()) {
        return visitor.visit(expr);
    }
    return Status::OK();
}

template <typename Expr,
          typename V,
          typename... Visitors,
          typename = std::enable_if_t<std::is_base_of<ExprVisitor, V>::value>,
          typename = std::enable_if_t<std::is_same<std::remove_const_t<Expr>, Expression>::value>>
static inline Status postVisit(Expr* expr, V& visitor, Visitors&... visitors) {
    if (visitor.isPostOrder()) {
        NG_RETURN_IF_ERROR(visitor.visit(expr));
    }
    return postVisit(expr, visitors...);
}

template <template <typename> class makePtr, typename... Visitors>
static inline Status traverse(PTR(Expression) expr, Visitors&... visitors) {
    NG_RETURN_IF_ERROR(preVisit(expr, visitors...));
    switch (expr->kind()) {
        case Expression::Kind::kConstant:
        case Expression::Kind::kDstProperty:
        case Expression::Kind::kSrcProperty:
        case Expression::Kind::kTagProperty:
        case Expression::Kind::kEdgeProperty:
        case Expression::Kind::kEdgeSrc:
        case Expression::Kind::kEdgeType:
        case Expression::Kind::kEdgeRank:
        case Expression::Kind::kEdgeDst:
        case Expression::Kind::kUUID:
        case Expression::Kind::kVar:
        case Expression::Kind::kVersionedVar:
        case Expression::Kind::kVarProperty:
        case Expression::Kind::kInputProperty:
        case Expression::Kind::kSymProperty:
        case Expression::Kind::kLabel: {
            // nothing
            break;
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
        case Expression::Kind::kSubscript:
        case Expression::Kind::kLogicalAnd:
        case Expression::Kind::kLogicalOr:
        case Expression::Kind::kLogicalXor: {
            auto biExpr = static_cast<PTR(BinaryExpression)>(expr);
            NG_RETURN_IF_ERROR(traverse<makePtr>(biExpr->left(), visitors...));
            NG_RETURN_IF_ERROR(traverse<makePtr>(biExpr->right(), visitors...));
            break;
        }
        case Expression::Kind::kUnaryPlus:
        case Expression::Kind::kUnaryNegate:
        case Expression::Kind::kUnaryNot:
        case Expression::Kind::kUnaryIncr:
        case Expression::Kind::kUnaryDecr: {
            auto unaryExpr = static_cast<PTR(UnaryExpression)>(expr);
            NG_RETURN_IF_ERROR(traverse<makePtr>(unaryExpr->operand(), visitors...));
            break;
        }
        case Expression::Kind::kFunctionCall: {
            auto funcExpr = static_cast<PTR(FunctionCallExpression)>(expr);
            for (auto& arg : funcExpr->args()->args()) {
                NG_RETURN_IF_ERROR(traverse<makePtr>(arg.get(), visitors...));
            }
            break;
        }
        case Expression::Kind::kTypeCasting: {
            auto typeCastExpr = static_cast<PTR(TypeCastingExpression)>(expr);
            NG_RETURN_IF_ERROR(traverse<makePtr>(typeCastExpr->operand(), visitors...));
            break;
        }
        case Expression::Kind::kList: {
            auto listExpr = static_cast<PTR(ListExpression)>(expr);
            for (auto& item : listExpr->items()) {
                NG_RETURN_IF_ERROR(
                    traverse<makePtr>(const_cast<PTR(Expression)>(item), visitors...));
            }
            break;
        }
        case Expression::Kind::kSet: {
            auto setExpr = static_cast<PTR(SetExpression)>(expr);
            for (auto& item : setExpr->items()) {
                NG_RETURN_IF_ERROR(
                    traverse<makePtr>(const_cast<PTR(Expression)>(item), visitors...));
            }
            break;
        }
        case Expression::Kind::kMap: {
            auto mapExpr = static_cast<PTR(MapExpression)>(expr);
            for (auto& item : mapExpr->items()) {
                NG_RETURN_IF_ERROR(
                    traverse<makePtr>(const_cast<PTR(Expression)>(item.second), visitors...));
            }
            break;
        }
    }
    NG_RETURN_IF_ERROR(postVisit(expr, visitors...));
    return Status::OK();
}

class PropsCollectVisitor : public ExprVisitor {
public:
    explicit PropsCollectVisitor(const Validator* validator)
        : ExprVisitor(VisitOrder::kPre), validator_(validator) {}

    Status visit(const Expression* expr);

    void collect(PropsCollectVisitor&& r);

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
    const Validator* validator_{nullptr};
};

template <typename To,
          typename = std::enable_if_t<std::is_same<To, EdgePropertyExpression>::value ||
                                      std::is_same<To, TagPropertyExpression>::value>>
class SymbolPropExprTransformer : public ExprVisitor {
public:
    SymbolPropExprTransformer() : ExprVisitor(VisitOrder::kPre) {}

    Status visit(Expression* expr) {
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
            case Expression::Kind::kLabel:
            case Expression::Kind::kConstant: {
                return Status::OK();
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
                    biExpr->setLeft(transSymbolPropertyExpression(symbolExpr));
                }
                if (biExpr->right()->kind() == Expression::Kind::kSymProperty) {
                    auto* symbolExpr = static_cast<SymbolPropertyExpression*>(biExpr->right());
                    biExpr->setRight(transSymbolPropertyExpression(symbolExpr));
                }
                return Status::OK();
            }
            case Expression::Kind::kUnaryIncr:
            case Expression::Kind::kUnaryDecr:
            case Expression::Kind::kUnaryPlus:
            case Expression::Kind::kUnaryNegate:
            case Expression::Kind::kUnaryNot: {
                auto* unaryExpr = static_cast<UnaryExpression*>(expr);
                if (unaryExpr->operand()->kind() == Expression::Kind::kSymProperty) {
                    auto* symbolExpr = static_cast<SymbolPropertyExpression*>(unaryExpr->operand());
                    unaryExpr->setOperand(transSymbolPropertyExpression(symbolExpr));
                }
                return Status::OK();
            }
            case Expression::Kind::kTypeCasting: {
                auto* typeCastingExpr = static_cast<TypeCastingExpression*>(expr);
                if (typeCastingExpr->operand()->kind() == Expression::Kind::kSymProperty) {
                    auto* symbolExpr =
                        static_cast<SymbolPropertyExpression*>(typeCastingExpr->operand());
                    typeCastingExpr->setOperand(transSymbolPropertyExpression(symbolExpr));
                }
                return Status::OK();
            }
            case Expression::Kind::kFunctionCall: {
                auto* funcExpr = static_cast<FunctionCallExpression*>(expr);
                for (auto& arg : funcExpr->args()->args()) {
                    if (arg->kind() == Expression::Kind::kSymProperty) {
                        auto* symbolExpr = static_cast<SymbolPropertyExpression*>(arg.get());
                        arg.reset(transSymbolPropertyExpression(symbolExpr));
                    }
                }
                return Status::OK();
            }
            case Expression::Kind::kList:   // FIXME(dutor)
            case Expression::Kind::kSet:
            case Expression::Kind::kMap:
            case Expression::Kind::kSubscript: {
                DLOG(FATAL) << "Not support transfer symbol property in expr " << expr->kind();
                return Status::SemanticError("Not support transfer symbol property in expr %d.",
                                             static_cast<int>(expr->kind()));
            }
        }   // switch
        DLOG(FATAL) << "Impossible expression kind " << static_cast<int>(expr->kind());
        return Status::Error("Impossible expression kind %d.", static_cast<int>(expr->kind()));
    }

private:
    static To* transSymbolPropertyExpression(SymbolPropertyExpression* expr) {
        return new To(new std::string(std::move(*expr->sym())),
                      new std::string(std::move(*expr->prop())));
    }
};

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

    static bool evaluableExpr(const Expression* expr) {
        EvaluableVisitor visitor;
        traverse<makeConstPtr>(expr, visitor);
        return visitor.evaluable();
    }

    template <typename To,
            typename = std::enable_if_t<std::is_same<To, EdgePropertyExpression>::value ||
                                        std::is_same<To, TagPropertyExpression>::value>>
    static To* transSymbolPropertyExpression(SymbolPropertyExpression* expr) {
        return new To(new std::string(std::move(*expr->sym())),
                      new std::string(std::move(*expr->prop())));
    }
};

#undef PTR   // not a generic macro

}   // namespace graph
}   // namespace nebula

#endif   // _UTIL_EXPRESSION_UTILS_H_
