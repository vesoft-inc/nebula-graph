/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTIL_PARSERUTIL_H_
#define UTIL_PARSERUTIL_H_

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "parser/MaintainSentences.h"
#include "visitor/RewriteMatchLabelVisitor.h"

namespace nebula {
namespace graph {

class ParserUtil final {
public:
    ParserUtil() = delete;

    static bool isLabel(const Expression *expr) {
        return expr->kind() == Expression::Kind::kLabel ||
               expr->kind() == Expression::Kind::kLabelAttribute;
    }

    static void rewriteLC(ListComprehensionExpression *lc,
                          const std::string &oldVarName,
                          const std::string &newVarName) {
        auto rewriter = [&oldVarName, &newVarName](const Expression *expr) {
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

        RewriteMatchLabelVisitor visitor(rewriter);

        lc->setNewInnerVar(new std::string(newVarName));
        if (lc->hasFilter()) {
            Expression *filter = lc->filter();
            Expression *newFilter = nullptr;
            if (isLabel(filter)) {
                newFilter = rewriter(filter);
            } else {
                newFilter = filter->clone().release();
                newFilter->accept(&visitor);
            }
            lc->setNewFilter(newFilter);
        }
        if (lc->hasMapping()) {
            Expression *mapping = lc->mapping();
            Expression *newMapping = nullptr;
            if (isLabel(mapping)) {
                newMapping = rewriter(mapping);
            } else {
                newMapping = mapping->clone().release();
                newMapping->accept(&visitor);
            }
            lc->setNewMapping(newMapping);
        }
    }
};

}   // namespace graph
}   // namespace nebula
#endif   // UTIL_PARSERUTIL_H_
