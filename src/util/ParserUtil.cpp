/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "util/ParserUtil.h"
#include "common/base/Status.h"
#include "common/base/StatusOr.h"

namespace nebula {
namespace graph {

// static
bool ParserUtil::isLabel(const Expression *expr) {
    return expr->kind() == Expression::Kind::kLabel ||
           expr->kind() == Expression::Kind::kLabelAttribute;
}

// static
void ParserUtil::rewriteLC(QueryContext *qctx,
                           ListComprehensionExpression *lc,
                           const std::string &oldVarName) {
    const auto &newVarName = qctx->vctx()->anonVarGen()->getVar();
    qctx->ectx()->setValue(newVarName, Value());
    auto *pool = qctx->objPool();

    auto matcher = [](const Expression *expr) -> bool {
        return expr->kind() == Expression::Kind::kLabel ||
               expr->kind() == Expression::Kind::kLabelAttribute;
    };

    auto rewriter = [&, pool, newVarName](const Expression *expr) {
        Expression *ret = nullptr;
        if (expr->kind() == Expression::Kind::kLabel) {
            auto *label = static_cast<const LabelExpression *>(expr);
            if (label->name() == oldVarName) {
                ret = VariableExpression::make(pool, newVarName, true);
            } else {
                ret = label->clone();
            }
        } else {
            DCHECK(expr->kind() == Expression::Kind::kLabelAttribute);
            auto *la = static_cast<const LabelAttributeExpression *>(expr);
            if (la->left()->name() == oldVarName) {
                const auto &value = la->right()->value();
                ret = AttributeExpression::make(pool,
                                                VariableExpression::make(pool, newVarName, true),
                                                ConstantExpression::make(pool, value));
            } else {
                ret = la->clone();
            }
        }
        return ret;
    };

    lc->setOriginString(lc->toString());
    lc->setInnerVar(newVarName);
    if (lc->hasFilter()) {
        Expression *filter = lc->filter();
        auto *newFilter = RewriteVisitor::transform(filter, matcher, rewriter);
        lc->setFilter(newFilter);
    }
    if (lc->hasMapping()) {
        Expression *mapping = lc->mapping();
        auto *newMapping =
            RewriteVisitor::transform(mapping, std::move(matcher), std::move(rewriter));
        lc->setMapping(newMapping);
    }
}

// static
void ParserUtil::rewritePred(QueryContext *qctx,
                             PredicateExpression *pred,
                             const std::string &oldVarName) {
    const auto &newVarName = qctx->vctx()->anonVarGen()->getVar();
    qctx->ectx()->setValue(newVarName, Value());
    auto *pool = qctx->objPool();

    auto matcher = [](const Expression *expr) -> bool {
        return expr->kind() == Expression::Kind::kLabel ||
               expr->kind() == Expression::Kind::kLabelAttribute;
    };

    auto rewriter = [&](const Expression *expr) {
        Expression *ret = nullptr;
        if (expr->kind() == Expression::Kind::kLabel) {
            auto *label = static_cast<const LabelExpression *>(expr);
            if (label->name() == oldVarName) {
                ret = VariableExpression::make(pool, newVarName, true);
            } else {
                ret = label->clone();
            }
        } else {
            DCHECK(expr->kind() == Expression::Kind::kLabelAttribute);
            auto *la = static_cast<const LabelAttributeExpression *>(expr);
            if (la->left()->name() == oldVarName) {
                const auto &value = la->right()->value();
                ret = AttributeExpression::make(pool,
                                                VariableExpression::make(pool, newVarName, true),
                                                ConstantExpression::make(pool, value));
            } else {
                ret = la->clone();
            }
        }
        return ret;
    };

    pred->setOriginString(pred->toString());
    pred->setInnerVar(newVarName);

    auto *newFilter =
        RewriteVisitor::transform(pred->filter(), std::move(matcher), std::move(rewriter));
    pred->setFilter(newFilter);
}

// static
void ParserUtil::rewriteReduce(QueryContext *qctx,
                               ReduceExpression *reduce,
                               const std::string &oldAccName,
                               const std::string &oldVarName) {
    const auto &newAccName = qctx->vctx()->anonVarGen()->getVar();
    qctx->ectx()->setValue(newAccName, Value());
    const auto &newVarName = qctx->vctx()->anonVarGen()->getVar();
    qctx->ectx()->setValue(newVarName, Value());
    auto *pool = qctx->objPool();

    auto matcher = [](const Expression *expr) -> bool {
        return expr->kind() == Expression::Kind::kLabel ||
               expr->kind() == Expression::Kind::kLabelAttribute;
    };
    auto rewriter = [&](const Expression *expr) {
        Expression *ret = nullptr;
        if (expr->kind() == Expression::Kind::kLabel) {
            auto *label = static_cast<const LabelExpression *>(expr);
            if (label->name() == oldAccName) {
                ret = VariableExpression::make(pool, newAccName, true);
            } else if (label->name() == oldVarName) {
                ret = VariableExpression::make(pool, newVarName, true);
            } else {
                ret = label->clone();
            }
        } else {
            DCHECK(expr->kind() == Expression::Kind::kLabelAttribute);
            auto *la = static_cast<const LabelAttributeExpression *>(expr);
            if (la->left()->name() == oldAccName) {
                const auto &value = la->right()->value();
                ret = AttributeExpression::make(pool,
                                                VariableExpression::make(pool, newAccName, true),
                                                ConstantExpression::make(pool, value));
            } else if (la->left()->name() == oldVarName) {
                const auto &value = la->right()->value();
                ret = AttributeExpression::make(pool,
                                                VariableExpression::make(pool, newVarName, true),
                                                ConstantExpression::make(pool, value));
            } else {
                ret = la->clone();
            }
        }
        return ret;
    };

    reduce->setOriginString(reduce->toString());
    reduce->setAccumulator(newAccName);
    reduce->setInnerVar(newVarName);

    auto *newMapping =
        RewriteVisitor::transform(reduce->mapping(), std::move(matcher), std::move(rewriter));
    reduce->setMapping(newMapping);
}

// static
void ParserUtil::rewriteMapProjection(QueryContext *qctx,
                                      MapProjectionExpression *mapProj) {
    auto *pool = qctx->objPool();
    auto *newItems = MapItemList::make(pool);
    // map_variable {.prop, v, key:val, .*} => {prop: map_variable.prop, v:v, key:val ...}
    // .* means projects all key-value pairs from the map_variable value
    for (auto &item : mapProj->items()) {
        auto& k = item.first;
        auto* v = item.second;
        DCHECK(!k.empty());
        if (k[0] == '.') {  // .prop, .*
            DCHECK(v == nullptr);
            auto *label = LabelExpression::make(pool, mapProj->mapVarName());
            auto propName = k.substr(1);
            auto *attr = ConstantExpression::make(pool, propName);
            auto *la = LabelAttributeExpression::make(pool, label, attr);
            newItems->add(propName, la);
        } else if (v != nullptr) {  // key:val
            newItems->add(k, v);
        } else {  // v
            auto *label = LabelExpression::make(pool, k);
            newItems->add(k, label);
        }
    }
    mapProj->setItems(newItems->get());
}


}   // namespace graph
}   // namespace nebula
