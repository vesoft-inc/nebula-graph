/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/planners/MatchVertexIndexSeekPlanner.h"

#include "util/ExpressionUtils.h"
#include "validator/MatchValidator.h"

namespace nebula {
namespace graph {
bool MatchVertexIndexSeekPlanner::match(AstContext* astCtx) {
    if (astCtx->sentence->kind() != Sentence::Kind::kMatch) {
        return false;
    }
    auto* matchCtx = static_cast<MatchAstContext*>(astCtx);

    auto& head = matchCtx->nodeInfos[0];
    if (head.label == nullptr) {
        return false;
    }

    Expression *filter = nullptr;
    if (filter != nullptr) {
        filter = makeIndexFilter(*head.label, *head.alias, matchCtx->filter.get(), matchCtx->qctx);
    }
    if (filter == nullptr) {
        if (head.props != nullptr && !head.props->items().empty()) {
            filter = makeIndexFilter(*head.label, head.props, matchCtx->qctx);
        }
    }

    if (filter == nullptr) {
        return false;
    }

    matchCtx->scanInfo.filter = filter;
    matchCtx->scanInfo.schemaId = head.tid;

    return true;
}

Expression* MatchVertexIndexSeekPlanner::makeIndexFilter(const std::string &label,
                                                         const MapExpression *map,
                                                         QueryContext* qctx) {
    auto &items = map->items();
    Expression *root = new RelationalExpression(Expression::Kind::kRelEQ,
            new TagPropertyExpression(
                new std::string(label),
                new std::string(*items[0].first)),
            items[0].second->clone().release());
    for (auto i = 1u; i < items.size(); i++) {
        auto *left = root;
        auto *right = new RelationalExpression(Expression::Kind::kRelEQ,
                new TagPropertyExpression(
                    new std::string(label),
                    new std::string(*items[i].first)),
                items[i].second->clone().release());
        root = new LogicalExpression(Expression::Kind::kLogicalAnd, left, right);
    }
    return qctx->objPool()->add(root);
}

Expression* MatchVertexIndexSeekPlanner::makeIndexFilter(const std::string &label,
                                                         const std::string &alias,
                                                         const Expression *filter,
                                                         QueryContext* qctx) {
    static const std::unordered_set<Expression::Kind> kinds = {
        Expression::Kind::kRelEQ,
        Expression::Kind::kRelLT,
        Expression::Kind::kRelLE,
        Expression::Kind::kRelGT,
        Expression::Kind::kRelGE
    };

    std::vector<const Expression*> ands;
    auto kind = filter->kind();
    if (kinds.count(kind) == 1) {
        ands.emplace_back(filter);
    } else if (kind == Expression::Kind::kLogicalAnd) {
        ands = ExpressionUtils::pullAnds(filter);
    } else {
        return nullptr;
    }

    std::vector<Expression*> relationals;
    for (auto *item : ands) {
        if (kinds.count(item->kind()) != 1) {
            continue;
        }

        auto *binary = static_cast<const BinaryExpression*>(item);
        auto *left = binary->left();
        auto *right = binary->right();
        const LabelAttributeExpression *la = nullptr;
        const ConstantExpression *constant = nullptr;
        if (left->kind() == Expression::Kind::kLabelAttribute &&
                right->kind() == Expression::Kind::kConstant) {
            la = static_cast<const LabelAttributeExpression*>(left);
            constant = static_cast<const ConstantExpression*>(right);
        } else if (right->kind() == Expression::Kind::kLabelAttribute &&
                left->kind() == Expression::Kind::kConstant) {
            la = static_cast<const LabelAttributeExpression*>(right);
            constant = static_cast<const ConstantExpression*>(left);
        } else {
            continue;
        }

        if (*la->left()->name() != alias) {
            continue;
        }

        auto *tpExpr = new TagPropertyExpression(
                new std::string(label),
                new std::string(*la->right()->name()));
        auto *newConstant = constant->clone().release();
        if (left->kind() == Expression::Kind::kLabelAttribute) {
            auto *rel = new RelationalExpression(item->kind(), tpExpr, newConstant);
            relationals.emplace_back(rel);
        } else {
            auto *rel = new RelationalExpression(item->kind(), newConstant, tpExpr);
            relationals.emplace_back(rel);
        }
    }

    if (relationals.empty()) {
        return nullptr;
    }

    auto *root = relationals[0];
    for (auto i = 1u; i < relationals.size(); i++) {
        auto *left = root;
        root = new LogicalExpression(Expression::Kind::kLogicalAnd, left, relationals[i]);
    }

    return qctx->objPool()->add(root);
}

StatusOr<SubPlan> MatchVertexIndexSeekPlanner::transform(AstContext* astCtx) {
    // TODO:
    UNUSED(astCtx);
    return Status::Error();
}
}  // namespace graph
}  // namespace nebula
