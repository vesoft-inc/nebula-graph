/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/DeduceTypeVisitor.h"

#include <sstream>
#include <unordered_map>

#include "common/datatypes/DataSet.h"
#include "common/datatypes/Edge.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Map.h"
#include "common/datatypes/Path.h"
#include "common/datatypes/Set.h"
#include "common/expression/ArithmeticExpression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/RelationalExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "common/expression/UnaryExpression.h"
#include "common/function/FunctionManager.h"
#include "context/QueryContext.h"
#include "context/QueryExpressionContext.h"
#include "context/ValidateContext.h"
#include "util/SchemaUtil.h"
#include "visitor/CheckExprEvaluableVisitor.h"

namespace nebula {
namespace graph {

static const std::unordered_map<Value::Type, Value> kConstantValues = {
    {Value::Type::__EMPTY__, Value()},
    {Value::Type::NULLVALUE, Value(NullType::__NULL__)},
    {Value::Type::BOOL, Value(true)},
    {Value::Type::INT, Value(1)},
    {Value::Type::FLOAT, Value(1.0)},
    {Value::Type::STRING, Value("123")},
    {Value::Type::DATE, Value(Date())},
    {Value::Type::DATETIME, Value(DateTime())},
    {Value::Type::VERTEX, Value(Vertex())},
    {Value::Type::EDGE, Value(Edge())},
    {Value::Type::PATH, Value(Path())},
    {Value::Type::LIST, Value(List())},
    {Value::Type::MAP, Value(Map())},
    {Value::Type::SET, Value(Set())},
    {Value::Type::DATASET, Value(DataSet())},
};

#define DETECT_BIEXPR_TYPE(OP)                                                                     \
    expr->left()->accept(this);                                                                    \
    if (!ok()) return;                                                                             \
    auto left = type_;                                                                             \
    expr->right()->accept(this);                                                                   \
    if (!ok()) return;                                                                             \
    auto right = type_;                                                                            \
    auto detectVal = kConstantValues.at(left) OP kConstantValues.at(right);                        \
    if (detectVal.isBadNull()) {                                                                   \
        std::stringstream ss;                                                                      \
        ss << "`" << expr->toString() << "' is not a valid expression, "                           \
           << "can not apply `" << #OP << "' to `" << left << "' and `" << right << "'.";          \
        status_ = Status::SemanticError(ss.str());                                                 \
        return;                                                                                    \
    }                                                                                              \
    type_ = detectVal.type()

#define DETECT_UNARYEXPR_TYPE(OP)                                                                  \
    auto detectVal = OP kConstantValues.at(type_);                                                 \
    if (detectVal.isBadNull()) {                                                                   \
        std::stringstream ss;                                                                      \
        ss << "`" << expr->toString() << "' is not a valid expression, "                           \
           << "can not apply `" << #OP << "' to " << type_ << ".";                                 \
        status_ = Status::SemanticError(ss.str());                                                 \
        return;                                                                                    \
    }                                                                                              \
    type_ = detectVal.type()

DeduceTypeVisitor::DeduceTypeVisitor(QueryContext *qctx,
                                     ValidateContext *vctx,
                                     const ColsDef &inputs,
                                     GraphSpaceID space)
    : qctx_(qctx), vctx_(vctx), inputs_(inputs), space_(space) {
    DCHECK(qctx != nullptr);
    DCHECK(vctx != nullptr);
}

void DeduceTypeVisitor::visitArithmeticExpr(const ArithmeticExpression *expr) {
    switch (expr->kind()) {
        case Expression::Kind::kAdd: {
            DETECT_BIEXPR_TYPE(+);
            break;
        }
        case Expression::Kind::kMinus: {
            DETECT_BIEXPR_TYPE(-);
            break;
        }
        case Expression::Kind::kMultiply: {
            DETECT_BIEXPR_TYPE(*);
            break;
        }
        case Expression::Kind::kDivision: {
            DETECT_BIEXPR_TYPE(/);
            break;
        }
        case Expression::Kind::kMod: {
            DETECT_BIEXPR_TYPE(%);
            break;
        }
        default: {
            LOG(FATAL) << "Invalid arithmetic expression kind: "
                       << static_cast<uint8_t>(expr->kind());
            break;
        }
    }
}

void DeduceTypeVisitor::visitRelationalExpr(const RelationalExpression *expr) {
    expr->left()->accept(this);
    if (!ok()) return;
    expr->right()->accept(this);
    if (!ok()) return;
    auto right = type_;

    if (right != Value::Type::LIST &&
        (expr->kind() == Expression::Kind::kRelIn || expr->kind() == Expression::Kind::kRelNotIn)) {
        // FIXME(dutor)
        std::stringstream ss;
        ss << "`" << expr->toString() << "' is not a valid expression, "
           << "expected `LIST' but `" << right << "' was given.";
        status_ = Status::SemanticError(ss.str());
        return;
    }

    type_ = Value::Type::BOOL;
}

void DeduceTypeVisitor::visitLogicalExpr(const LogicalExpression *expr) {
    switch (expr->kind()) {
        case Expression::Kind::kLogicalAnd: {
            DETECT_BIEXPR_TYPE(&&);
            break;
        }
        case Expression::Kind::kLogicalXor:
        case Expression::Kind::kLogicalOr: {
            DETECT_BIEXPR_TYPE(||);
            break;
        }
        default: {
            LOG(FATAL) << "Invalid logical expression kind: " << static_cast<uint8_t>(expr->kind());
            break;
        }
    }
}

void DeduceTypeVisitor::visitUnaryExpr(const UnaryExpression *expr) {
    expr->operand()->accept(this);
    if (!ok()) return;
    switch (expr->kind()) {
        case Expression::Kind::kUnaryPlus:
            break;
        case Expression::Kind::kUnaryNegate: {
            DETECT_UNARYEXPR_TYPE(-);
            break;
        }
        case Expression::Kind::kUnaryNot: {
            DETECT_UNARYEXPR_TYPE(!);
            break;
        }
        case Expression::Kind::kUnaryIncr: {
            auto detectVal = kConstantValues.at(type_) + 1;
            if (detectVal.isBadNull()) {
                std::stringstream ss;
                ss << "`" << expr->toString() << "' is not a valid expression, "
                   << "can not apply `++' to " << type_ << ".";
                status_ = Status::SemanticError(ss.str());
                return;
            }
            type_ = detectVal.type();
            break;
        }
        case Expression::Kind::kUnaryDecr: {
            auto detectVal = kConstantValues.at(type_) - 1;
            if (detectVal.isBadNull()) {
                std::stringstream ss;
                ss << "`" << expr->toString() << "' is not a valid expression, "
                   << "can not apply `--' to " << type_ << ".";
                status_ = Status::SemanticError(ss.str());
                return;
            }
            type_ = detectVal.type();
            break;
        }
        default: {
            LOG(FATAL) << "Invalid unary expression kind: " << static_cast<uint8_t>(expr->kind());
            break;
        }
    }
}

void DeduceTypeVisitor::visitFunctionCallExpr(const FunctionCallExpression *expr) {
    std::vector<Value::Type> argsTypeList;
    argsTypeList.reserve(expr->args()->numArgs());
    for (auto &arg : expr->args()->args()) {
        arg->accept(this);
        if (!ok()) return;
        argsTypeList.push_back(type_);
    }
    auto result = FunctionManager::getReturnType(*expr->name(), argsTypeList);
    if (!result.ok()) {
        status_ = Status::SemanticError("`%s` is not a valid expression : %s",
                                        expr->toString().c_str(),
                                        result.status().toString().c_str());
        return;
    }
    type_ = result.value();
    status_ = Status::OK();
}

void DeduceTypeVisitor::visitTypeCastingExpr(const TypeCastingExpression *expr) {
    expr->operand()->accept(this);
    if (!ok()) return;

    CheckExprEvaluableVisitor visitor;
    expr->operand()->accept(&visitor);

    if (!visitor.ok()) {
        if (TypeCastingExpression::validateTypeCast(type_, expr->type())) {
            type_ = expr->type();
            return;
        }
        std::stringstream out;
        out << "Can not convert " << expr->operand() << " 's type : " << type_ << " to "
            << expr->type();
        status_ = Status::SemanticError(out.str());
        return;
    }
    QueryExpressionContext ctx(nullptr, nullptr);
    auto *typeCastExpr = const_cast<TypeCastingExpression *>(expr);
    auto val = typeCastExpr->eval(ctx);
    if (val.isNull()) {
        status_ =
            Status::SemanticError("`%s` is not a valid expression ", expr->toString().c_str());
        return;
    }
    type_ = val.type();
    status_ = Status::OK();
}

void DeduceTypeVisitor::visitTagPropertyExpr(const TagPropertyExpression *expr) {
    visitTagPropExpr(expr);
}

void DeduceTypeVisitor::visitSourcePropertyExpr(const SourcePropertyExpression *expr) {
    visitTagPropExpr(expr);
}

void DeduceTypeVisitor::visitDestPropertyExpr(const DestPropertyExpression *expr) {
    visitTagPropExpr(expr);
}

void DeduceTypeVisitor::visitEdgePropertyExpr(const EdgePropertyExpression *expr) {
    auto *edge = expr->sym();
    auto edgeType = qctx_->schemaMng()->toEdgeType(space_, *edge);
    if (!edgeType.ok()) {
        status_ = edgeType.status();
        return;
    }
    auto schema = qctx_->schemaMng()->getEdgeSchema(space_, edgeType.value());
    if (!schema) {
        status_ = Status::SemanticError(
            "`%s', not found edge `%s'.", expr->toString().c_str(), edge->c_str());
        return;
    }

    auto *prop = expr->prop();
    auto *field = schema->field(*prop);
    if (field == nullptr) {
        status_ = Status::SemanticError(
            "`%s', not found the property `%s'.", expr->toString().c_str(), prop->c_str());
        return;
    }
    type_ = SchemaUtil::propTypeToValueType(field->type());
}

void DeduceTypeVisitor::visitVariablePropertyExpr(const VariablePropertyExpression *expr) {
    auto *var = expr->sym();
    if (!vctx_->existVar(*var)) {
        status_ = Status::SemanticError(
            "`%s', not exist variable `%s'", expr->toString().c_str(), var->c_str());
        return;
    }
    auto *prop = expr->prop();
    auto cols = vctx_->getVar(*var);
    auto found =
        std::find_if(cols.begin(), cols.end(), [&prop](auto &col) { return *prop == col.first; });
    if (found == cols.end()) {
        status_ = Status::SemanticError(
            "`%s', not exist prop `%s'", expr->toString().c_str(), prop->c_str());
        return;
    }
    type_ = found->second;
}

void DeduceTypeVisitor::visitInputPropertyExpr(const InputPropertyExpression *expr) {
    auto *prop = expr->prop();
    auto found = std::find_if(
        inputs_.cbegin(), inputs_.cend(), [&prop](auto &col) { return *prop == col.first; });
    if (found == inputs_.cend()) {
        status_ = Status::SemanticError(
            "`%s', not exist prop `%s'", expr->toString().c_str(), prop->c_str());
        return;
    }
    type_ = found->second;
}

void DeduceTypeVisitor::visitSymbolPropertyExpr(const SymbolPropertyExpression *expr) {
    status_ = Status::SemanticError("SymbolPropertyExpression can not be instantiated: %s.",
                                    expr->toString().c_str());
}

void DeduceTypeVisitor::visitLabelExpr(const LabelExpression *expr) {
    status_ = Status::SemanticError("LabelExpression can not be instantiated: %s.",
                                    expr->toString().c_str());
}

void DeduceTypeVisitor::visitConstantExpr(const ConstantExpression *expr) {
    QueryExpressionContext ctx(nullptr, nullptr);
    auto *mutableExpr = const_cast<ConstantExpression *>(expr);
    type_ = mutableExpr->eval(ctx).type();
}

void DeduceTypeVisitor::visitEdgeSrcIdExpr(const EdgeSrcIdExpression *) {
    type_ = Value::Type::STRING;
}

void DeduceTypeVisitor::visitEdgeTypeExpr(const EdgeTypeExpression *) {
    type_ = Value::Type::INT;
}

void DeduceTypeVisitor::visitEdgeRankExpr(const EdgeRankExpression *) {
    type_ = Value::Type::INT;
}

void DeduceTypeVisitor::visitEdgeDstIdExpr(const EdgeDstIdExpression *) {
    type_ = Value::Type::STRING;
}

void DeduceTypeVisitor::visitUUIDExpr(const UUIDExpression *) {
    type_ = Value::Type::STRING;
}

void DeduceTypeVisitor::visitVariableExpr(const VariableExpression *) {
    // TODO: not only dataset
    type_ = Value::Type::DATASET;
}

void DeduceTypeVisitor::visitVersionedVariableExpr(const VersionedVariableExpression *) {
    // TODO: not only dataset
    type_ = Value::Type::DATASET;
}

void DeduceTypeVisitor::visitListExpr(const ListExpression *) {
    type_ = Value::Type::LIST;
}

void DeduceTypeVisitor::visitSetExpr(const SetExpression *) {
    type_ = Value::Type::SET;
}

void DeduceTypeVisitor::visitMapExpr(const MapExpression *) {
    type_ = Value::Type::MAP;
}

void DeduceTypeVisitor::visitSubscriptExpr(const SubscriptExpression *) {
    type_ = Value::Type::LIST;   // FIXME(dutor)
}

void DeduceTypeVisitor::visitTagPropExpr(const SymbolPropertyExpression *expr) {
    auto *tag = expr->sym();
    auto tagId = qctx_->schemaMng()->toTagID(space_, *tag);
    if (!tagId.ok()) {
        status_ = tagId.status();
        return;
    }
    auto schema = qctx_->schemaMng()->getTagSchema(space_, tagId.value());
    if (!schema) {
        status_ = Status::SemanticError(
            "`%s', not found tag `%s'.", expr->toString().c_str(), tag->c_str());
        return;
    }
    auto *prop = expr->prop();
    auto *field = schema->field(*prop);
    if (field == nullptr) {
        status_ = Status::SemanticError(
            "`%s', not found the property `%s'.", expr->toString().c_str(), prop->c_str());
        return;
    }
    type_ = SchemaUtil::propTypeToValueType(field->type());
}

#undef DETECT_UNARYEXPR_TYPE
#undef DETECT_BIEXPR_TYPE

}   // namespace graph
}   // namespace nebula
