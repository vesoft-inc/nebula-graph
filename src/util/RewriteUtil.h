/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTIL_REWRITEUTIL_H_
#define UTIL_REWRITEUTIL_H_

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "parser/MaintainSentences.h"
#include "visitor/RewriteMatchLabelVisitor.h"

namespace nebula {
namespace graph {

class RewriteUtil final {
public:
    RewriteUtil() = delete;

    static bool isLabel(const Expression *expr) {
        return expr->kind() == Expression::Kind::kLabel ||
               expr->kind() == Expression::Kind::kLabelAttribute;
    }

    static RewriteMatchLabelVisitor::Rewriter rewriteLabel(const std::string &oldVarName,
                                                           const std::string &newVarName) {
        return [&oldVarName, &newVarName](const Expression *expr) {
            Expression *ret = nullptr;
            if (expr->kind() == Expression::Kind::kLabel) {
                auto *label = static_cast<const LabelExpression *>(expr);
                if (*label->name() == oldVarName) {
                    ret = new VariableExpression(new std::string(newVarName));
                } else {
                    ret = label->clone().release();
                }
            } else {
                DCHECK(expr->kind() == Expression::Kind::kLabelAttribute);
                auto *la = static_cast<const LabelAttributeExpression *>(expr);
                if (*la->left()->name() == oldVarName) {
                    const auto &value = la->right()->value();
                    ret =
                        new AttributeExpression(new VariableExpression(new std::string(newVarName)),
                                                new ConstantExpression(value));
                } else {
                    ret = la->clone().release();
                }
            }
            return ret;
        };
    }
};

}   // namespace graph
}   // namespace nebula
#endif   // UTIL_REWRITEUTIL_H_
