/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/ContainerExpression.h"
#include "common/function/FunctionManager.h"

#include "context/QueryExpressionContext.h"
#include "util/ExpressionUtils.h"
#include "util/SchemaUtil.h"
#include "validator/Validator.h"

namespace nebula {
namespace graph {

Status EvaluableVisitor::visit(const Expression *expr) {
    switch (expr->kind()) {
        case Expression::Kind::kConstant:
        case Expression::Kind::kUUID:
            return Status::OK();
        case Expression::Kind::kLabel:
        case Expression::Kind::kDstProperty:
        case Expression::Kind::kSrcProperty:
        case Expression::Kind::kTagProperty:
        case Expression::Kind::kEdgeProperty:
        case Expression::Kind::kEdgeSrc:
        case Expression::Kind::kEdgeType:
        case Expression::Kind::kEdgeRank:
        case Expression::Kind::kEdgeDst:
        case Expression::Kind::kVar:
        case Expression::Kind::kVersionedVar:
        case Expression::Kind::kVarProperty:
        case Expression::Kind::kInputProperty:
        case Expression::Kind::kSymProperty: {
            evaluable_ = false;
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
        case Expression::Kind::kSubscript:
        case Expression::Kind::kLogicalAnd:
        case Expression::Kind::kLogicalOr:
        case Expression::Kind::kLogicalXor:
        case Expression::Kind::kUnaryPlus:
        case Expression::Kind::kUnaryNegate:
        case Expression::Kind::kUnaryNot:
        case Expression::Kind::kUnaryIncr:
        case Expression::Kind::kUnaryDecr:
        case Expression::Kind::kFunctionCall:
        case Expression::Kind::kTypeCasting:
        case Expression::Kind::kList:
        case Expression::Kind::kSet:
        case Expression::Kind::kMap: {
            return Status::OK();
        }
    }
    DLOG(FATAL) << "Unknown expression " << static_cast<int>(expr->kind());
    return Status::Error("Unknown expression %d", static_cast<int>(expr->kind()));
}

#define DETECT_BIEXPR_TYPE(OP)                                                                     \
    auto rightType = subTypes_.top();                                                              \
    subTypes_.pop();                                                                               \
    auto leftType = subTypes_.top();                                                               \
    subTypes_.pop();                                                                               \
    auto detectVal = kConstantValues.at(leftType) OP kConstantValues.at(rightType);                \
    if (detectVal.isBadNull()) {                                                                   \
        std::stringstream ss;                                                                      \
        ss << "`" << expr->toString() << "' is not a valid expression, "                           \
           << "can not apply `" << #OP << "' to `" << leftType << "' and `" << rightType << "'.";  \
        return Status::SemanticError(ss.str());                                                    \
    }                                                                                              \
    subTypes_.emplace(detectVal.type());                                                           \
    return Status::OK();

#define DETECT_UNARYEXPR_TYPE(OP)                                                                  \
    auto operandType = subTypes_.top();                                                            \
    subTypes_.pop();                                                                               \
    auto detectVal = OP kConstantValues.at(operandType);                                           \
    if (detectVal.isBadNull()) {                                                                   \
        std::stringstream ss;                                                                      \
        ss << "`" << expr->toString() << "' is not a valid expression, "                           \
           << "can not apply `" << #OP << "' to " << operandType << ".";                           \
        return Status::SemanticError(ss.str());                                                    \
    }                                                                                              \
    subTypes_.emplace(detectVal.type());                                                           \
    return Status::OK();

Status TypeDeduceVisitor::visit(const Expression *expr) {
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
    static QueryExpressionContext dummyExprCtx(nullptr);

    auto schemaMgr = validator_->qctx()->schemaMng();
    auto spaceId = validator_->space().id;

    switch (DCHECK_NOTNULL(expr)->kind()) {
        case Expression::Kind::kDstProperty:
        case Expression::Kind::kSrcProperty:
        case Expression::Kind::kTagProperty: {
            auto *tagPropExpr = exprCast<SymbolPropertyExpression>(expr);
            auto tagIdResult = schemaMgr->toTagID(spaceId, *tagPropExpr->sym());
            NG_RETURN_IF_ERROR(tagIdResult);
            auto tagId = tagIdResult.value();
            auto typeResult = tagFieldType(schemaMgr, spaceId, tagId, *tagPropExpr->prop());
            NG_RETURN_IF_ERROR(typeResult);
            subTypes_.emplace(typeResult.value());
            return Status::OK();
        }
        case Expression::Kind::kEdgeSrc:
        case Expression::Kind::kEdgeType:
        case Expression::Kind::kEdgeRank:
        case Expression::Kind::kEdgeDst:
        case Expression::Kind::kEdgeProperty: {
            auto edgePropExpr = exprCast<SymbolPropertyExpression>(expr);
            auto edgeTypeResult = schemaMgr->toEdgeType(spaceId, *edgePropExpr->sym());
            NG_RETURN_IF_ERROR(edgeTypeResult);
            auto edgeType = edgeTypeResult.value();
            auto typeResult = edgeFieldType(schemaMgr, spaceId, edgeType, *edgePropExpr->prop());
            NG_RETURN_IF_ERROR(typeResult);
            subTypes_.emplace(typeResult.value());
            return Status::OK();
        }
        case Expression::Kind::kInputProperty: {
            auto inputPropExpr = exprCast<InputPropertyExpression>(expr);
            const auto &inputCols = validator_->inputColsRef();
            const auto &inputCol =
                std::find_if(inputCols.begin(), inputCols.end(), [inputPropExpr](const auto &col) {
                    return col.first == *inputPropExpr->prop();
                });
            if (inputCol == inputCols.end()) {
                std::stringstream err;
                err << "No input property named '" << *inputPropExpr->prop() << "`";
                LOG(WARNING) << err.str();
                return Status::SemanticError(err.str());
            }
            subTypes_.emplace(inputCol->second);
            return Status::OK();
        }
        case Expression::Kind::kVarProperty: {
            auto varPropExpr = exprCast<VariablePropertyExpression>(expr);
            const auto &var = validator_->vctx()->getVar(*varPropExpr->sym());
            if (var.empty()) {
                return Status::SemanticError("Not exits variable %s.", varPropExpr->sym()->c_str());
            }
            const auto &varCol =
                std::find_if(var.begin(), var.end(), [varPropExpr](const auto &col) {
                    return col.first == *varPropExpr->prop();
                });
            if (varCol == var.end()) {
                std::stringstream err;
                err << "No property named '" << *varPropExpr->prop() << "`"
                    << " in $" << *varPropExpr->sym();
                LOG(WARNING) << err.str();
                return Status::SemanticError(err.str());
            }
            subTypes_.emplace(varCol->second);
            return Status::OK();
        }
        case Expression::Kind::kUUID: {
            subTypes_.emplace(Value::Type::STRING);
            return Status::OK();
        }
        case Expression::Kind::kConstant: {
            subTypes_.emplace(const_cast<Expression *>(expr)->eval(dummyExprCtx).type());
            return Status::OK();
        }
        case Expression::Kind::kLabel:
        case Expression::Kind::kSymProperty:
        case Expression::Kind::kVar:
        case Expression::Kind::kVersionedVar: {
            std::stringstream err;
            err << "Unsupported expression `" << expr->toString() << "'";
            return Status::SemanticError(err.str());
        }
        case Expression::Kind::kSubscript: {
            // TODO
            subTypes_.pop();
            subTypes_.pop();
            subTypes_.emplace(Value::Type::NULLVALUE);
            return Status::OK();
        }
        case Expression::Kind::kList: {
            auto listExpr = exprCast<ListExpression>(expr);
            auto itemSize = listExpr->items().size();
            for (std::size_t i = 0; i < itemSize; ++i) {
                subTypes_.pop();
            }
            subTypes_.emplace(Value::Type::LIST);
            return Status::OK();
        }
        case Expression::Kind::kSet: {
            auto setExpr = exprCast<SetExpression>(expr);
            auto itemSize = setExpr->items().size();   // TODO(shylock) get size directly
            for (std::size_t i = 0; i < itemSize; ++i) {
                subTypes_.pop();
            }
            subTypes_.emplace(Value::Type::SET);
            return Status::OK();
        }
        case Expression::Kind::kMap: {
            auto mapExpr = exprCast<MapExpression>(expr);
            auto itemSize = mapExpr->items().size();
            for (std::size_t i = 0; i < itemSize; ++i) {
                subTypes_.pop();
            }
            subTypes_.emplace(Value::Type::MAP);
            return Status::OK();
        }
        case Expression::Kind::kAdd: {
            DETECT_BIEXPR_TYPE(+);
        }
        case Expression::Kind::kMinus: {
            DETECT_BIEXPR_TYPE(-);
        }
        case Expression::Kind::kMultiply: {
            DETECT_BIEXPR_TYPE(*);
        }
        case Expression::Kind::kDivision: {
            DETECT_BIEXPR_TYPE(/);
        }
        case Expression::Kind::kMod: {
            DETECT_BIEXPR_TYPE(%);
        }
        case Expression::Kind::kContains:
        case Expression::Kind::kRelEQ:
        case Expression::Kind::kRelNE:
        case Expression::Kind::kRelLT:
        case Expression::Kind::kRelLE:
        case Expression::Kind::kRelGT:
        case Expression::Kind::kRelGE: {
            subTypes_.pop();
            subTypes_.pop();
            // TODO add check
            subTypes_.emplace(Value::Type::BOOL);
            return Status::OK();
        }
        case Expression::Kind::kLogicalAnd: {
            DETECT_BIEXPR_TYPE(&&);
        }
        case Expression::Kind::kLogicalOr:
        case Expression::Kind::kLogicalXor: {
            DETECT_BIEXPR_TYPE(||);
        }
        case Expression::Kind::kRelIn: {
            auto biExpr = exprCast<BinaryExpression>(expr);
            subTypes_.pop();
            auto rightType = subTypes_.top();
            subTypes_.pop();
            if (rightType != Value::Type::LIST && rightType != Value::Type::SET &&
                rightType != Value::Type::MAP) {
                std::stringstream ss;
                ss << "`" << biExpr->toString() << "' is not a valid expression, "
                   << "expected collection but `" << rightType << "' was given.";
                return Status::SemanticError(ss.str());
            }
            subTypes_.emplace(Value::Type::BOOL);
            return Status::OK();
        }
        case Expression::Kind::kRelNotIn: {
            auto biExpr = exprCast<BinaryExpression>(expr);
            subTypes_.pop();
            auto rightType = subTypes_.top();
            subTypes_.pop();
            if (rightType != Value::Type::LIST && rightType != Value::Type::SET &&
                rightType != Value::Type::MAP) {
                std::stringstream ss;
                ss << "`" << biExpr->toString() << "' is not a valid expression, "
                   << "expected collection but `" << rightType << "' was given.";
                return Status::SemanticError(ss.str());
            }
            subTypes_.emplace(Value::Type::BOOL);
            return Status::OK();
        }
        case Expression::Kind::kUnaryIncr: {
            auto operandType = subTypes_.top();
            subTypes_.pop();
            auto detectVal = kConstantValues.at(operandType) + 1;
            if (detectVal.isBadNull()) {
                std::stringstream ss;
                ss << "`" << expr->toString() << "' is not a valid expression, "
                   << "can not apply `++' to " << operandType << ".";
                return Status::SemanticError(ss.str());
            }
            subTypes_.emplace(detectVal.type());
            return Status::OK();
        }
        case Expression::Kind::kUnaryDecr: {
            auto operandType = subTypes_.top();
            subTypes_.pop();
            auto detectVal = kConstantValues.at(operandType) + 1;
            if (detectVal.isBadNull()) {
                std::stringstream ss;
                ss << "`" << expr->toString() << "' is not a valid expression, "
                   << "can not apply `--' to " << operandType << ".";
                return Status::SemanticError(ss.str());
            }
            subTypes_.emplace(detectVal.type());
            return Status::OK();
        }
        case Expression::Kind::kUnaryPlus: {
            auto operandType = subTypes_.top();
            subTypes_.pop();
            subTypes_.emplace(operandType);
            return Status::OK();
        }
        case Expression::Kind::kUnaryNegate: {
            DETECT_UNARYEXPR_TYPE(-);
        }
        case Expression::Kind::kUnaryNot: {
            DETECT_UNARYEXPR_TYPE(!);
        }
        case Expression::Kind::kTypeCasting: {
            auto typeCastingExpr = exprCast<TypeCastingExpression>(expr);
            auto operandType = subTypes_.top();
            subTypes_.pop();
            if (!ExpressionUtils::evaluableExpr(typeCastingExpr)) {
                if (TypeCastingExpression::validateTypeCast(operandType, typeCastingExpr->type())) {
                    subTypes_.emplace(typeCastingExpr->type());
                    return Status::OK();
                }
                std::stringstream out;
                out << "Can not convert " << typeCastingExpr->operand()
                    << " 's type : " << operandType << " to " << typeCastingExpr->type();
                return Status::SemanticError(out.str());
            }

            auto mutExpr = const_cast<TypeCastingExpression *>(typeCastingExpr);
            const auto &result = mutExpr->eval(dummyExprCtx);
            if (result.isNull()) {
                return Status::SemanticError("`%s` is not a valid expression ",
                                             typeCastingExpr->toString().c_str());
            }
            subTypes_.emplace(result.type());
            return Status::OK();
        }
        case Expression::Kind::kFunctionCall: {
            auto funcExpr = exprCast<FunctionCallExpression>(expr);
            auto argSize = funcExpr->args()->numArgs();
            std::vector<Value::Type> argsType;
            argsType.resize(argSize);
            for (std::size_t i = 0; i < argSize; ++i) {
                argsType[argSize - i - 1] = subTypes_.top();
                subTypes_.pop();
            }
            auto result = FunctionManager::getReturnType(*(funcExpr->name()), argsType);
            if (!result.ok()) {
                return Status::SemanticError("`%s` is not a valid expression : %s",
                                             funcExpr->toString().c_str(),
                                             result.status().toString().c_str());
            }
            subTypes_.emplace(result.value());
            return Status::OK();
        }
    }
    DLOG(FATAL) << "Impossible expression kind " << static_cast<int>(expr->kind());
    return Status::Error("Impossible expression kind %d.", static_cast<int>(expr->kind()));
}

/*static*/ StatusOr<Value::Type> TypeDeduceVisitor::edgeFieldType(
    /*const*/ meta::SchemaManager *schemaMgr,
    GraphSpaceID spaceId,
    EdgeType edgeType,
    const std::string &field) {
    // TODO(shylock) move it to schema manager
    static const std::unordered_map<std::string, Value::Type> reservedPropertiesType{
        {kSrc, Value::Type::STRING},
        {kType, Value::Type::INT},
        {kRank, Value::Type::INT},
        {kDst, Value::Type::STRING},
    };
    const auto &re = reservedPropertiesType.find(field);
    if (re != reservedPropertiesType.end()) {
        return re->second;
    }

    auto schema = schemaMgr->getEdgeSchema(spaceId, edgeType);
    if (!schema) {
        return Status::SemanticError("Not found edge %d", edgeType);
    }
    auto *fieldSchema = schema->field(field);
    if (fieldSchema == nullptr) {
        return Status::SemanticError("Not found the property `%s'.", field.c_str());
    }
    return SchemaUtil::propTypeToValueType(fieldSchema->type());
}

/*static*/ StatusOr<Value::Type> TypeDeduceVisitor::tagFieldType(
    /*const*/ meta::SchemaManager *schemaMgr,
    GraphSpaceID spaceId,
    TagID tagId,
    const std::string &field) {
    auto schema = schemaMgr->getTagSchema(spaceId, tagId);
    if (!schema) {
        return Status::SemanticError("Not found tag %d", tagId);
    }
    auto *fieldSchema = schema->field(field);
    if (fieldSchema == nullptr) {
        return Status::SemanticError("Not found the property `%s'.", field.c_str());
    }
    return SchemaUtil::propTypeToValueType(fieldSchema->type());
}

Status PropsCollectVisitor::visit(const Expression *expr) {
    auto schemaMgr = validator_->qctx()->schemaMng();
    auto spaceId = validator_->space().id;

    switch (DCHECK_NOTNULL(expr)->kind()) {
        case Expression::Kind::kDstProperty: {
            auto *dstPropExpr = exprCast<DestPropertyExpression>(expr);
            auto tagIdResult = schemaMgr->toTagID(spaceId, *dstPropExpr->sym());
            NG_RETURN_IF_ERROR(tagIdResult);
            auto tagId = tagIdResult.value();
            auto &tagProp = dstTagProps_[tagId];
            tagProp.emplace_back(*dstPropExpr->prop());
            return Status::OK();
        }
        case Expression::Kind::kSrcProperty: {
            auto *srcPropExpr = exprCast<SourcePropertyExpression>(expr);
            auto tagIdResult = schemaMgr->toTagID(spaceId, *srcPropExpr->sym());
            NG_RETURN_IF_ERROR(tagIdResult);
            auto tagId = tagIdResult.value();
            auto &tagProp = srcTagProps_[tagId];
            tagProp.emplace_back(*srcPropExpr->prop());
            return Status::OK();
        }
        case Expression::Kind::kTagProperty: {
            auto *tagPropExpr = exprCast<TagPropertyExpression>(expr);
            auto tagIdResult = schemaMgr->toTagID(spaceId, *tagPropExpr->sym());
            NG_RETURN_IF_ERROR(tagIdResult);
            auto tagId = tagIdResult.value();
            auto &tagProp = tagProps_[tagId];
            tagProp.emplace_back(*tagPropExpr->prop());
            return Status::OK();
        }
        case Expression::Kind::kEdgeSrc:
        case Expression::Kind::kEdgeType:
        case Expression::Kind::kEdgeRank:
        case Expression::Kind::kEdgeDst:
        case Expression::Kind::kEdgeProperty: {
            auto edgePropExpr = exprCast<SymbolPropertyExpression>(expr);
            auto edgeTypeResult = schemaMgr->toEdgeType(spaceId, *edgePropExpr->sym());
            NG_RETURN_IF_ERROR(edgeTypeResult);
            auto edgeType = edgeTypeResult.value();
            auto &edgeProp = edgeProps_[edgeType];
            edgeProp.emplace_back(*edgePropExpr->prop());
            return Status::OK();
        }
        case Expression::Kind::kInputProperty: {
            auto inputPropExpr = exprCast<InputPropertyExpression>(expr);
            const auto &inputCols = validator_->inputColsRef();
            const auto &inputCol =
                std::find_if(inputCols.begin(), inputCols.end(), [inputPropExpr](const auto &col) {
                    return col.first == *inputPropExpr->prop();
                });
            if (inputCol == inputCols.end()) {
                std::stringstream err;
                err << "No input property named '" << *inputPropExpr->prop() << "`";
                LOG(WARNING) << err.str();
                return Status::SemanticError(err.str());
            }
            inputProps_.emplace_back(inputCol->first);
            return Status::OK();
        }
        case Expression::Kind::kVarProperty: {
            auto varPropExpr = exprCast<VariablePropertyExpression>(expr);
            const auto &var = validator_->vctx()->getVar(*varPropExpr->sym());
            if (var.empty()) {
                return Status::SemanticError("Not exits variable %s.", varPropExpr->sym()->c_str());
            }
            const auto &varCol =
                std::find_if(var.begin(), var.end(), [varPropExpr](const auto &col) {
                    return col.first == *varPropExpr->prop();
                });
            if (varCol == var.end()) {
                std::stringstream err;
                err << "No property named '" << *varPropExpr->prop() << "`"
                    << " in $" << *varPropExpr->sym();
                LOG(WARNING) << err.str();
                return Status::SemanticError(err.str());
            }
            auto &varProp = varProps_[*varPropExpr->sym()];
            varProp.emplace_back(*varPropExpr->prop());
            return Status::OK();
        }
        case Expression::Kind::kUUID:
        case Expression::Kind::kConstant:
        case Expression::Kind::kLabel:
        case Expression::Kind::kSymProperty:
        case Expression::Kind::kVar:
        case Expression::Kind::kVersionedVar: {
            return Status::OK();
        }
        case Expression::Kind::kList:
        case Expression::Kind::kSet:
        case Expression::Kind::kMap:
        case Expression::Kind::kSubscript:
        case Expression::Kind::kAdd:
        case Expression::Kind::kMinus:
        case Expression::Kind::kMultiply:
        case Expression::Kind::kDivision:
        case Expression::Kind::kMod:
        case Expression::Kind::kContains:
        case Expression::Kind::kRelEQ:
        case Expression::Kind::kRelNE:
        case Expression::Kind::kRelLT:
        case Expression::Kind::kRelLE:
        case Expression::Kind::kRelGT:
        case Expression::Kind::kRelGE:
        case Expression::Kind::kLogicalAnd:
        case Expression::Kind::kLogicalOr:
        case Expression::Kind::kLogicalXor:
        case Expression::Kind::kRelIn:
        case Expression::Kind::kRelNotIn:
        case Expression::Kind::kUnaryIncr:
        case Expression::Kind::kUnaryDecr:
        case Expression::Kind::kUnaryPlus:
        case Expression::Kind::kUnaryNegate:
        case Expression::Kind::kUnaryNot:
        case Expression::Kind::kTypeCasting:
        case Expression::Kind::kFunctionCall: {
            return Status::OK();
        }
    }
    DLOG(FATAL) << "Impossible expression kind " << static_cast<int>(expr->kind());
    return Status::Error("Impossible expression kind %d.", static_cast<int>(expr->kind()));
}

void PropsCollectVisitor::collect(PropsCollectVisitor &&r) {
    inputProps_.insert(inputProps_.end(),
                       std::make_move_iterator(r.inputProps_.begin()),
                       std::make_move_iterator(r.inputProps_.end()));

    for (auto &iter : r.srcTagProps_) {
        srcTagProps_[iter.first].insert(srcTagProps_[iter.first].end(),
                                        std::make_move_iterator(iter.second.begin()),
                                        std::make_move_iterator(iter.second.end()));
    }

    for (auto &iter : r.dstTagProps_) {
        dstTagProps_[iter.first].insert(dstTagProps_[iter.first].end(),
                                        std::make_move_iterator(iter.second.begin()),
                                        std::make_move_iterator(iter.second.end()));
    }

    for (auto &iter : r.tagProps_) {
        tagProps_[iter.first].insert(tagProps_[iter.first].end(),
                                     std::make_move_iterator(iter.second.begin()),
                                     std::make_move_iterator(iter.second.end()));
    }

    for (auto &iter : r.varProps_) {
        varProps_[iter.first].insert(varProps_[iter.first].end(),
                                     std::make_move_iterator(iter.second.begin()),
                                     std::make_move_iterator(iter.second.end()));
    }

    for (auto &iter : r.edgeProps_) {
        edgeProps_[iter.first].insert(edgeProps_[iter.first].end(),
                                      std::make_move_iterator(iter.second.begin()),
                                      std::make_move_iterator(iter.second.end()));
    }
}

bool PropsCollectVisitor::isSubsetOfInput(const std::vector<std::string> &props) const {
    for (auto &prop : props) {
        if (std::find(inputProps_.begin(), inputProps_.end(), prop) == inputProps_.end()) {
            return false;
        }
    }
    return true;
}

bool PropsCollectVisitor::isSubsetOfVar(
    const std::unordered_map<std::string, std::vector<std::string>> &props) {
    for (auto &iter : props) {
        if (varProps_.find(iter.first) == varProps_.end()) {
            return false;
        }
        for (auto &prop : iter.second) {
            if (std::find(varProps_[iter.first].begin(), varProps_[iter.first].end(), prop) ==
                varProps_[iter.first].end()) {
                return false;
            }
        }
    }
    return true;
}

}   // namespace graph
}   // namespace nebula
