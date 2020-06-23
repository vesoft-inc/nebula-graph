/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "validator/GoValidator.h"
#include "parser/TraverseSentences.h"
#include "common/interface/gen-cpp2/storage_types.h"

namespace nebula {
namespace graph {
Status GoValidator::validateImpl() {
    Status status;
    auto* goSentence = static_cast<GoSentence*>(sentence_);
    do {
        status = validateStep(goSentence->stepClause());
        if (!status.ok()) {
            break;
        }

        status = validateFrom(goSentence->fromClause());
        if (!status.ok()) {
            break;
        }

        status = validateOver(goSentence->overClause());
        if (!status.ok()) {
            break;
        }

        status = validateWhere(goSentence->whereClause());
        if (!status.ok()) {
            break;
        }

        status = validateYield(goSentence->yieldClause());
        if (!status.ok()) {
            break;
        }
    } while (false);

    return status;
}

Status GoValidator::validateStep(const StepClause* step) {
    if (step == nullptr) {
        return Status::Error("Step clause nullptr.");
    }
    auto steps = step->steps();
    if (steps > 1) {
        // TODO
        return Status::Error("Not support n steps yet.");
    }
    return Status::OK();
}

Status GoValidator::validateFrom(const FromClause* from) {
    if (from == nullptr) {
        return Status::Error("From clause nullptr.");
    }
    if (from->isRef()) {
        auto* src = from->ref();
        if (src->kind() != Expression::Kind::kInputProperty
                || src->kind() != Expression::Kind::kVarProperty) {
            return Status::Error(
                    "Only input and variable expression is acceptable"
                    "when starts are evaluated at runtime..");
        } else {
            auto status = deduceProps(src);
            if (!status.ok()) {
                return status;
            }
            src_ = src;
            // TODO: validate if a str
        }
    } else {
        auto vidList = from->vidList();
        ExpressionContextImpl ctx(qctx_->ectx(), nullptr);
        for (auto* expr : vidList) {
            if (!evaluableExpr(expr)) {
                return Status::Error("`%s' is not an evaluable expression.",
                        expr->toString().c_str());
            }
            auto vid = expr->eval(ctx);
            if (!vid.isStr()) {
                return Status::Error("Vid should be a string.");
            }
            starts_.emplace_back(std::move(vid));
        }
    }
    return Status::OK();
}

Status GoValidator::validateOver(const OverClause* over) {
    if (over == nullptr) {
        return Status::Error("Over clause nullptr.");
    }

    direction_ = over->direction();
    if (over->isOverAll()) {
        isOverAll_ = true;
        return Status::OK();
    }
    auto edges = over->edges();
    auto* schemaMng = qctx_->schemaMng();
    for (auto* edge : edges) {
        auto edgeName = *edge->edge();
        auto edgeType = schemaMng->toEdgeType(space_.id, edgeName);
        if (!edgeType.ok()) {
            return Status::Error("%s not found in space [%s].",
                    edgeName.c_str(), space_.name.c_str());
        }
        edgeTypes_.emplace_back(edgeType.value());
    }
    return Status::OK();
}

Status GoValidator::validateWhere(const WhereClause* where) {
    if (where == nullptr) {
        return Status::OK();
    }

    filter_ = where->filter();
    auto status = deduceProps(filter_);
    if (!status.ok()) {
        return status;
    }
    auto typeStatus = deduceExprType(filter_);
    if (!typeStatus.ok()) {
        return typeStatus.status();
    }

    auto type = typeStatus.value();
    if (type != Value::Type::BOOL || type != Value::Type::NULLVALUE) {
        return Status::Error("Filter only accept bool/null value.");
    }
    return Status::OK();
}

Status GoValidator::validateYield(const YieldClause* yield) {
    if (yield == nullptr) {
        return Status::Error("Yield clause nullptr.");
    }

    auto cols = yield->columns();
    for (auto col : cols) {
        auto status = deduceProps(col->expr());
        if (!status.ok()) {
            return status;
        }

        auto colName = deduceColName(col);
        colNames_.emplace_back(colName);

        auto typeStatus = deduceExprType(col->expr());
        if (!typeStatus.ok()) {
            return typeStatus.status();
        }
        auto type = typeStatus.value();
        outputs_.emplace_back(colName, type);
    }
    return Status::OK();
}

Status GoValidator::deduceProps(const Expression* expr) {
    switch (expr->kind()) {
        case Expression::Kind::kConstant: {
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
        case Expression::Kind::kLogicalAnd:
        case Expression::Kind::kLogicalOr:
        case Expression::Kind::kLogicalXor: {
            auto biExpr = static_cast<const BinaryExpression*>(expr);
            auto leftStatus = deduceProps(biExpr->left());
            if (!leftStatus.ok()) {
                return leftStatus;
            }
            auto rightStatus = deduceProps(biExpr->right());
            if (!rightStatus.ok()) {
                return rightStatus;
            }
            break;
        }
        case Expression::Kind::kUnaryPlus:
        case Expression::Kind::kUnaryNegate:
        case Expression::Kind::kUnaryNot: {
            auto unaryExpr = static_cast<const UnaryExpression*>(expr);
            auto status = deduceProps(unaryExpr->operand());
            if (status.ok()) {
                return status;
            }
            break;
        }
        case Expression::Kind::kFunctionCall: {
            auto funcExpr = static_cast<const FunctionCallExpression*>(expr);
            for (auto& arg : funcExpr->args()->args()) {
                auto status = deduceProps(arg.get());
                if (!status.ok()) {
                    return status;
                }
            }
            break;
        }
        case Expression::Kind::kDstProperty: {
            auto* tagPropExpr = static_cast<const SymbolPropertyExpression*>(expr);
            auto status = qctx_->schemaMng()->toTagID(space_.id, *tagPropExpr->sym());
            if (!status.ok()) {
                return status.status();
            }
            auto& props = dstTagProps_[status.value()];
            props.emplace_back(*tagPropExpr->prop());
            break;
        }
        case Expression::Kind::kSrcProperty: {
            auto* tagPropExpr = static_cast<const SymbolPropertyExpression*>(expr);
            auto status = qctx_->schemaMng()->toTagID(space_.id, *tagPropExpr->sym());
            if (!status.ok()) {
                return status.status();
            }
            auto& props = srcTagProps_[status.value()];
            props.emplace_back(*tagPropExpr->prop());
            break;
        }
        case Expression::Kind::kEdgeProperty:
        case Expression::Kind::kEdgeSrc:
        case Expression::Kind::kEdgeType:
        case Expression::Kind::kEdgeRank:
        case Expression::Kind::kEdgeDst: {
            auto* edgePropExpr = static_cast<const SymbolPropertyExpression*>(expr);
            auto status = qctx_->schemaMng()->toEdgeType(space_.id, *edgePropExpr->sym());
            if (!status.ok()) {
                return status.status();
            }
            auto& props = edgeProps_[status.value()];
            props.emplace_back(*edgePropExpr->prop());
            break;
        }
        case Expression::Kind::kUUID:
        case Expression::Kind::kVar:
        case Expression::Kind::kVersionedVar:
        case Expression::Kind::kVarProperty:
        case Expression::Kind::kInputProperty:
        case Expression::Kind::kSymProperty:
        case Expression::Kind::kTypeCasting:
        case Expression::Kind::kUnaryIncr:
        case Expression::Kind::kUnaryDecr:
        case Expression::Kind::kRelIn: {
            // TODO:
            std::stringstream ss;
            ss << "Not support " << expr->kind();
            return Status::Error(ss.str());
        }
    }
    return Status::OK();
}

bool GoValidator::evaluableExpr(const Expression* expr) const {
    switch (expr->kind()) {
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
        case Expression::Kind::kRelIn:
        case Expression::Kind::kLogicalAnd:
        case Expression::Kind::kLogicalOr:
        case Expression::Kind::kLogicalXor: {
            auto biExpr = static_cast<const BinaryExpression*>(expr);
            return evaluableExpr(biExpr->left()) && evaluableExpr(biExpr->right());
        }
        case Expression::Kind::kUnaryPlus:
        case Expression::Kind::kUnaryNegate:
        case Expression::Kind::kUnaryNot: {
            auto unaryExpr = static_cast<const UnaryExpression*>(expr);
            return evaluableExpr(unaryExpr->operand());
        }
        case Expression::Kind::kFunctionCall: {
            auto funcExpr = static_cast<const FunctionCallExpression*>(expr);
            for (auto& arg : funcExpr->args()->args()) {
                if (!evaluableExpr(arg.get())) {
                    return false;
                }
            }
            return true;
        }
        case Expression::Kind::kTypeCasting: {
            auto castExpr = static_cast<const TypeCastingExpression*>(expr);
            return evaluableExpr(castExpr->operand());
        }
        case Expression::Kind::kDstProperty:
        case Expression::Kind::kSrcProperty:
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
        case Expression::Kind::kUnaryIncr:
        case Expression::Kind::kUnaryDecr: {
            return false;
        }
    }
    return false;
}

Status GoValidator::toPlan() {
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
