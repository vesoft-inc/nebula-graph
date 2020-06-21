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
            // TODO: validate if the column name is available.
            src_ = src;
        }
    } else {
        auto vidList = from->vidList();
        ExpressionContextImpl ctx(qctx_->ectx(), nullptr);
        // TODO: the exprs should be a evaluated one in validate phase.
        for (auto* expr : vidList) {
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
    auto space = vctx_->whichSpace();
    for (auto* edge : edges) {
        auto edgeName = *edge->edge();
        auto edgeType = schemaMng->toEdgeType(space.id, edgeName);
        if (!edgeType.ok()) {
            return Status::Error("%s not found in space [%s].",
                    edgeName.c_str(), space.name.c_str());
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
    // TODO: validate filter.
    return Status::OK();
}

Status GoValidator::validateYield(const YieldClause* yield) {
    if (yield == nullptr) {
        return Status::Error("Yield clause nullptr.");
    }

    auto cols = yield->columns();
    UNUSED(cols);
    return Status::OK();
}
/*
Status deduceProps(Expression* expr) {
    switch (expr->kind()) {
        case Expression::Kind::kConstant:
            os << "Constant";
            break;
        case Expression::Kind::kAdd:
        case Expression::Kind::kMinus:
        case Expression::Kind::kMultiply:
        case Expression::Kind::kDivision:
        case Expression::Kind::kMod: {
            auto left = deduceProps(expr->left());
            if (!left.ok()) {
                return left;
            }
            auto
        }
        case Expression::Kind::kUnaryPlus:
            os << "UnaryPlus";
            break;
        case Expression::Kind::kUnaryNegate:
            os << "UnaryNegate";
            break;
        case Expression::Kind::kUnaryNot:
            os << "UnaryNot";
            break;
        case Expression::Kind::kUnaryIncr:
            os << "AutoIncrement";
            break;
        case Expression::Kind::kUnaryDecr:
            os << "AutoDecrement";
            break;
        case Expression::Kind::kRelEQ:
            os << "Equal";
            break;
        case Expression::Kind::kRelNE:
            os << "NotEuqal";
            break;
        case Expression::Kind::kRelLT:
            os << "LessThan";
            break;
        case Expression::Kind::kRelLE:
            os << "LessEqual";
            break;
        case Expression::Kind::kRelGT:
            os << "GreaterThan";
            break;
        case Expression::Kind::kRelGE:
            os << "GreaterEqual";
            break;
        case Expression::Kind::kRelIn:
            os << "In";
            break;
        case Expression::Kind::kLogicalAnd:
            os << "LogicalAnd";
            break;
        case Expression::Kind::kLogicalOr:
            os << "LogicalOr";
            break;
        case Expression::Kind::kLogicalXor:
            os << "LogicalXor";
            break;
        case Expression::Kind::kTypeCasting:
            os << "TypeCasting";
            break;
        case Expression::Kind::kFunctionCall:
            os << "FunctionCall";
            break;
        case Expression::Kind::kSymProperty:
            os << "SymbolProp";
            break;
        case Expression::Kind::kEdgeProperty:
            os << "EdgeProp";
            break;
        case Expression::Kind::kInputProperty:
            os << "InputProp";
            break;
        case Expression::Kind::kVarProperty:
            os << "VarProp";
            break;
        case Expression::Kind::kDstProperty:
            os << "DstProp";
            break;
        case Expression::Kind::kSrcProperty:
            os << "SrcProp";
            break;
        case Expression::Kind::kEdgeSrc:
            os << "EdgeSrc";
            break;
        case Expression::Kind::kEdgeType:
            os << "EdgeType";
            break;
        case Expression::Kind::kEdgeRank:
            os << "EdgeRank";
            break;
        case Expression::Kind::kEdgeDst:
            os << "EdgeDst";
            break;
        case Expression::Kind::kUUID:
            os << "UUID";
            break;
        case Expression::Kind::kVar:
            os << "Variable";
            break;
        case Expression::Kind::kVersionedVar:
            os << "VersionedVariable";
            break;
    }
    return Status::OK()
}
*/

Status GoValidator::toPlan() {
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
