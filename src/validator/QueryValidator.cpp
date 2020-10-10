/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/Expression.h"
#include "validator/QueryValidator.h"
#include "visitor/DeduceTypeVisitor.h"
#include "util/SchemaUtil.h"

namespace nebula {
namespace graph {

StatusOr<std::string> QueryValidator::checkInputVarProperty(const Expression* ref,
                                                            Value::Type type) const {
    if (ref->kind() == Expression::Kind::kInputProperty) {
        const auto* propExpr = static_cast<const PropertyExpression*>(ref);
        ColDef col(*propExpr->prop(), type);
        const auto find = std::find(inputs_.begin(), inputs_.end(), col);
        if (find == inputs_.end()) {
            return Status::Error("No input property %s", propExpr->prop()->c_str());
        }
        return std::string();
    } else if (ref->kind() == Expression::Kind::kVarProperty) {
        const auto* propExpr = static_cast<const PropertyExpression*>(ref);
        ColDef col(*propExpr->prop(), type);
        const auto &outputVar = *propExpr->sym();
        const auto &var = vctx_->getVar(outputVar);
        if (var.empty()) {
            return Status::Error("No variable %s", outputVar.c_str());
        }
        const auto find = std::find(var.begin(), var.end(), col);
        if (find == var.end()) {
            return Status::Error("No property %s in variable %s",
                                 propExpr->prop()->c_str(),
                                 outputVar.c_str());
        }
        return outputVar;
    } else {
        // it's guranteed by parser
        DLOG(FATAL) << "Unexpected expression " << ref->kind();
        return Status::Error("Unexpected expression.");
    }
}

StatusOr<Value::Type> QueryValidator::deduceExprType(const Expression* expr) const {
    DeduceTypeVisitor visitor(qctx_, vctx_, inputs_, space_.id);
    const_cast<Expression*>(expr)->accept(&visitor);
    if (!visitor.ok()) {
        return std::move(visitor).status();
    }
    return visitor.type();
}

Status QueryValidator::validateVertices(const VerticesClause* clause, Starts& starts) {
    if (clause == nullptr) {
        return Status::Error("From clause nullptr.");
    }
    if (clause->isRef()) {
        auto* src = clause->ref();
        if (src->kind() != Expression::Kind::kInputProperty
                && src->kind() != Expression::Kind::kVarProperty) {
            return Status::Error(
                    "`%s', Only input and variable expression is acceptable"
                    " when starts are evaluated at runtime.", src->toString().c_str());
        } else {
            starts.fromType = src->kind() == Expression::Kind::kInputProperty ? kPipe : kVariable;
            auto type = deduceExprType(src);
            if (!type.ok()) {
                return type.status();
            }
            auto vidType = space_.spaceDesc.vid_type.get_type();
            if (type.value() != SchemaUtil::propTypeToValueType(vidType)) {
                std::stringstream ss;
                ss << "`" << src->toString() << "', the srcs should be type of "
                   << meta::cpp2::_PropertyType_VALUES_TO_NAMES.at(vidType) << ", but was`"
                   << type.value() << "'";
                return Status::Error(ss.str());
            }
            starts.srcRef = src;
            auto* propExpr = static_cast<PropertyExpression*>(src);
            if (starts.fromType == kVariable) {
                starts.userDefinedVarName = *(propExpr->sym());
            }
            starts.firstBeginningSrcVidColName = *(propExpr->prop());
        }
    } else {
        auto vidList = clause->vidList();
        QueryExpressionContext ctx;
        for (auto* expr : vidList) {
            // It's guranteed by parser so only check when debug
            DCHECK(evaluableExpr(expr));
            auto vid = expr->eval(ctx(nullptr));
            auto vidType = space_.spaceDesc.vid_type.get_type();
            if (!SchemaUtil::isValidVid(vid, vidType)) {
                std::stringstream ss;
                ss << "Vid should be a " << meta::cpp2::_PropertyType_VALUES_TO_NAMES.at(vidType);
                return Status::Error(ss.str());
            }
            starts.vids.emplace_back(std::move(vid));
            startVidList_->add(expr->clone().release());
        }
    }
    return Status::OK();
}

std::string QueryValidator::buildConstantInput(Expression* &src, Starts &starts) {
    auto input = vctx_->anonVarGen()->getVar();
    DataSet ds({kVid});
    for (auto& vid : starts.vids) {
        ds.emplace_back(Row({std::move(vid)}));
    }
    qctx_->ectx()->setResult(input, ResultBuilder().value(Value(std::move(ds))).finish());

    auto *vids = qctx_->objPool()->makeAndAdd<VariablePropertyExpression>(new std::string(input),
                                                                          new std::string(kVid));
    src = vids;
    return input;
}


}   // namespace graph
}   // namespace nebula
