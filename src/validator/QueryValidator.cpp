/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/Expression.h"
#include "validator/QueryValidator.h"
#include "visitor/DeduceTypeVisitor.h"

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


}   // namespace graph
}   // namespace nebula
