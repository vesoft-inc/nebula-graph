/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rules/IndexScanRule.h"

namespace nebula {
namespace graph {

bool IndexScanRule::match(const std::shared_ptr<PlanNode>& node) const {
    return node->kind() == PlanNode::Kind::kIndexScan;
}

StatusOr<std::shared_ptr<PlanNode>> IndexScanRule::transform(std::shared_ptr<PlanNode> node) {
    Status ret = Status::OK();
    ret = prepareOptimize(node);
    NG_RETURN_IF_ERROR(ret);

    ret = doOptimize();
    NG_RETURN_IF_ERROR(ret);

    ret = postOptimize();
    NG_RETURN_IF_ERROR(ret);

    return node;
}

Status IndexScanRule::prepareOptimize(const std::shared_ptr<PlanNode>& node) {
    if (node == nullptr) {
        return Status::Error("Plan node is null");
    }
    node_ = dynamic_cast<IndexScan*>(node.get());
    space_ = node_->schemaId();
    isEdgeIndex_ = node_->isEdge();
    schemaId_ = node_->schemaId();
    auto* ctxs = node_->queryContext();
    // The initial IndexScan plan node has only one queryContext.
    if (ctxs->size() != 1) {
        return Status::Error("Index Scan plan node error");
    }
    rootFilter_.reset(std::move((*ctxs)[0].filter_).get());
    return Status::OK();
}

Status IndexScanRule::doOptimize() {
    Status ret = Status::OK();
    ret = analyzeExpression(rootFilter_.get());
    NG_RETURN_IF_ERROR(ret);

    ret = analyzeIndexes();
    NG_RETURN_IF_ERROR(ret);
    return ret;
}

Status IndexScanRule::analyzeExpression(Expression* expr) {
    // TODO (sky) : Currently only simple logical expressions are supported,
    //              such as all AND or all OR expressions, example :
    //              where c1 > 1 and c1 < 2 and c2 == 1
    //              where c1 == 1 or c2 == 1 or c3 == 1
    //              Complex logical expressions are not supported yet, example :
    //              where c1 > 1 and c2 >1 or c3 > 1
    //              where c1 > 1 and c1 < 2 or c2 > 1
    //              where c1 < 1 and (c2 == 1 or c2 == 2)
    switch (expr->kind()) {
        case Expression::Kind::kLogicalOr :
        case Expression::Kind::kLogicalAnd : {
            auto lExpr = dynamic_cast<LogicalExpression*>(expr);
            auto kind = expr->kind() == Expression::Kind::kLogicalAnd
                        ? ScanKind::LOGICAL_AND : ScanKind::LOGICAL_OR;
            if (rootFilterkind_ == ScanKind::UNKNOWN) {
                rootFilterkind_ = kind;
            }
            auto ret = analyzeExpression(lExpr->left());
            NG_RETURN_IF_ERROR(ret);
            ret = analyzeExpression(lExpr->right());
            NG_RETURN_IF_ERROR(ret);
            break;
        }
        case Expression::Kind::kRelLE:
        case Expression::Kind::kRelIn:
        case Expression::Kind::kRelGE:
        case Expression::Kind::kRelEQ:
        case Expression::Kind::kRelLT:
        case Expression::Kind::kRelGT: {
            auto* rExpr = dynamic_cast<RelationalExpression*>(expr);
            auto ret = writeRelationalExpr(rExpr);
            NG_RETURN_IF_ERROR(ret);
            break;
        }
        case Expression::Kind::kRelNE:
        default: {
            auto errorMsg = folly::StringPiece("Filter not support yet : %s",
                                               Expression::encode(*expr).c_str());
            return Status::NotSupported(errorMsg);
        }
    }
    return Status::OK();
}

Status IndexScanRule::analyzeIndexes() {
    auto* mc = queryContext()->getMetaClient();
    auto indexesRet = isEdgeIndex_
                      ? mc->getEdgeIndexesFromCache(space_)
                      : mc->getTagIndexesFromCache(space_);
    if (!indexesRet.ok()) {
        return indexesRet.status();
    }

    auto indexes = indexesRet.value();
    if (indexes.empty()) {
        return Status::IndexNotFound();
    }

    switch (rootFilterkind_) {
        case ScanKind::LOGICAL_OR : {
            NG_RETURN_IF_ERROR(analyzeIndexesWithOR(std::move(indexes)));
            break;
        }
        case ScanKind::LOGICAL_AND : {
            NG_RETURN_IF_ERROR(analyzeIndexesWithAND(std::move(indexes)));
            break;
        }
        // ScanKind::UNKNOWN means that no logical expression appears,
        // Only a single relational expression appears
        case ScanKind::UNKNOWN : {
            NG_RETURN_IF_ERROR(analyzeIndexesWithSimple(std::move(indexes)));
            break;
        }
    }
    return Status::OK();
}

Status IndexScanRule::analyzeIndexesWithOR(
    std::vector<std::shared_ptr<meta::cpp2::IndexItem>>&& indexes) {
    for (const auto& item : relationalOpList_) {
        auto hintIndex = matchIndex({item}, indexes);
        if (!hintIndex.ok()) {
            return hintIndex.status();
        }
        NG_RETURN_IF_ERROR(assembleIndexQueryCtx(hintIndex.value(), {item}));
    }
    return Status::OK();
}

Status IndexScanRule::analyzeIndexesWithAND(
    std::vector<std::shared_ptr<meta::cpp2::IndexItem>>&& indexes) {

    auto comparator = [&] (const IndexOptItem &lhs, const IndexOptItem &rhs) {
        return std::get<1>(lhs) < std::get<1>(rhs);
    };
    std::sort(relationalOpList_.begin(), relationalOpList_.end(), comparator);

    auto hintIndex = matchIndex(relationalOpList_, indexes);
    if (!hintIndex.ok()) {
        return hintIndex.status();
    }
    NG_RETURN_IF_ERROR(assembleIndexQueryCtx(hintIndex.value(), relationalOpList_));
    return Status::OK();
}

Status IndexScanRule::analyzeIndexesWithSimple(
    std::vector<std::shared_ptr<meta::cpp2::IndexItem>>&& indexes) {
    return analyzeIndexesWithAND(std::move(indexes));
}

StatusOr<meta::cpp2::IndexItem*> IndexScanRule::matchIndex(
    const IndexOptItems& items,
    const std::vector<std::shared_ptr<meta::cpp2::IndexItem>>& indexes) {
    if (items.empty()) {
        return Status::Error("The optimization item for the index is empty");
    }

    // This optimization item must hit the first column of the index when only one optimization item
    // If there are multiple index hits, only the first found is returned
    // TODO (sky) : CBO , If there are multiple index hits, pick the best one.
    if (items.size() == 1) {
        const auto& field = std::get<0>(items[0]);
        auto it = std::find_if(indexes.cbegin(), indexes.cend(), [&field](auto &index) {
            const auto& fields = index->get_fields();
            if (field == fields[0].get_name()) {
                return true;
            }
            return false;
        });
        if (it != indexes.cend()) {
            return it->get();
        }
    } else {
        // If there are multiple optimization items, means that multiple field matches are required
        // Priority match "==" , the second match "<, >, <=, >=".
        // Allow some fields to have no index, but at least one field hit index.
        std::set<meta::cpp2::IndexItem*> hitIndexes;
        for (const auto& item : items) {
            const auto& field = std::get<0>(item);
            auto it = std::find_if(indexes.cbegin(), indexes.cend(), [&field](auto &index) {
                const auto& fields = index->get_fields();
                auto it2 = std::find_if(fields.cbegin(), fields.cend(), [&field](auto &col) {
                    if (field == col.get_name()) {
                        return true;
                    }
                    return false;
                });
                return it2 == fields.cend();
            });
            hitIndexes.emplace(it->get());
        }
        if (!hitIndexes.empty()) {
            return findBestIndex(items, hitIndexes);
        }
    }
    return Status::IndexNotFound();
}

// TODO : CBO , Only one index is returned through RBO.
StatusOr<meta::cpp2::IndexItem*> IndexScanRule::findBestIndex(
    const IndexOptItems& items, const std::set<meta::cpp2::IndexItem*>& indexes) {
    IndexOptItems relEQItems;
    IndexOptItems relOtherItems;
    for (const auto& item : items) {
        if (std::get<1>(item) == Expression::Kind::kRelEQ) {
            relEQItems.emplace_back(item);
        } else {
            relOtherItems.emplace_back(item);
        }
    }
    // If relEQItems is empty, means this is range scan. can use any index.
    if (relEQItems.empty()) {
        return *indexes.rbegin();
    } else {
        std::map<size_t, meta::cpp2::IndexItem*> indexHint;
        for (auto& index : indexes) {
            size_t hintCount = 0;
            for (const auto& field : index->get_fields()) {
                auto it = std::find_if(relEQItems.begin(), relEQItems.end(),
                    [field](const auto &rel) {
                        return field.get_name() == std::get<0>(rel);
                });
                if (it == relEQItems.end()) {
                    continue;
                }
                ++hintCount;
            }
            indexHint[hintCount] = index;
        }

        // TODO (sky) : The most matching index will be used. to do matching relOtherItems.
        // here only matching relEQItems, This can be further optimized. for example :
        // index1 (c1, c2, c3)
        // index2 (c1, c2, c4)
        // where c1 == 1 and c2 == 2 and c4 > 1
        // So the index2 is best index, Current may be return index1.
        return indexHint.rbegin()->second;
    }
}

Status IndexScanRule::assembleIndexQueryCtx(
    const meta::cpp2::IndexItem* index, const IndexOptItems& items) {
    // need a list copy.
    IndexOptItems newItems = items;
    IndexScan::IndexQueryCtx ctx;
    ctx.setIndexId(index->get_index_id());
    if (newItems.size() == 1) {
        auto fieldName = std::get<0>(newItems[0]);
        const auto & fields = index->get_fields();
        auto it = std::find_if(fields.begin(), fields.end(),
                               [fieldName](const auto &field) {
                                   return field.get_name() == fieldName;
                               });
        if (it == fields.end()) {
            return Status::Error("Optimizer error : field not found in index %s",
                                 index->get_index_name().c_str());
        }

        NG_RETURN_IF_ERROR(assembleIndexColumnHint({newItems[0]}, *it, ctx));
        newItems.clear();
    } else {
        for (const auto& field : index->get_fields()) {
            std::unordered_set<IndexOptItem> sameNameItems;
            for (auto it = newItems.begin(); it != newItems.end();) {
                // find same field name conditions, for example :
                // col1 > 1 and col2 > 2 and col1 < 5
                // then "col1 > 1" and "col1 < 5" should be add to colCtx.
                // and then remove these items from IndexOptItems.
                if (field.get_name() == std::get<0>(*it)) {
                    sameNameItems.emplace(*it);
                    it = newItems.erase(it);
                } else {
                    ++it;
                }
            }
            if (sameNameItems.size() == 1 and
                std::get<1>(*sameNameItems.begin()) == Expression::Kind::kRelEQ) {
                NG_RETURN_IF_ERROR(assembleIndexColumnHint({*sameNameItems.begin()}, field, ctx));
            } else {
                NG_RETURN_IF_ERROR(assembleIndexColumnHint(sameNameItems, field, ctx));
                break;
            }
        }
    }
    if (!newItems.empty()) {
        auto filter = assembleIndexFilter(index->get_schema_name(), newItems);
        if (filter == nullptr) {
            return Status::Error("Optimizer error : assemble index filter error");
        }
        ctx.setFilter(std::move(filter));
    }
    return Status::OK();
}

Status IndexScanRule::assembleIndexColumnHint(const std::unordered_set<IndexOptItem>& items,
                                              const meta::cpp2::ColumnDef& col,
                                              IndexScan::IndexQueryCtx& ctx) {
    storage::cpp2::IndexColumnHint hint;
    if (items.size() == 1) {
        hint.set_column_name(std::get<0>(*items.begin()));
        auto isPrefixScan = std::get<1>(*items.begin()) == Expression::Kind::kRelEQ;
        if (isPrefixScan) {
            hint.set_begin_value(std::get<2>(*items.begin()));
        } else {
            auto pair = rangScanPair(col, *items.begin());
            if (!pair.ok()) {
                return Status::Error("Optimizer error : assemble index column hint");
            }
            hint.set_begin_value(pair.value().first);
            hint.set_end_value(pair.value().second);
        }
        auto scanType = isPrefixScan
                        ? storage::cpp2::ScanType::PREFIX
                        : storage::cpp2::ScanType::RANGE;
        hint.set_scan_type(scanType);
    } else {
        Value max, min;
        for (const auto& item : items) {
            switch (std::get<1>(item)) {
                case Expression::Kind::kRelGT: {
                    auto v = OptimizerUtils::boundValue(
                        col, BoundaryValueType::GREATER_THAN, std::get<2>(item));
                    if (min > v) {
                        min = v;
                    }
                    break;
                }
                case Expression::Kind::kRelGE: {
                    if (min > std::get<2>(item)) {
                        min = std::get<2>(item);
                    }
                    break;
                }
                case Expression::Kind::kRelLT: {
                    auto v = OptimizerUtils::boundValue(
                        col, BoundaryValueType::LESS_THAN, std::get<2>(item));
                    if (max < v) {
                        max = v;
                    }
                    break;
                }
                case Expression::Kind::kRelLE: {
                    if (max < std::get<2>(item)) {
                        max = std::get<2>(item);
                    }
                    break;
                }
                default: {
                    return Status::Error("Optimizer error when get range scan pair");
                }
            }
        }
        if (min.empty()) {
            min = OptimizerUtils::boundValue(col, BoundaryValueType::MIN);
        }
        if (max.empty()) {
            max = OptimizerUtils::boundValue(col, BoundaryValueType::MAX);
        }
        // TODO (sky) : subtraction not support yet.
        // if max < min , means scan range error , example where c1 > 5 and c1 < 2,
        if (max < min) {
            return Status::Error("Optimizer error when get range scan pair");
        }
    }
    ctx.colHints_.emplace_back(std::move(hint));
    return Status::OK();
}

StatusOr<std::pair<Value, Value>>
IndexScanRule::rangScanPair(const meta::cpp2::ColumnDef& col, const IndexOptItem& item) {
    switch (std::get<1>(item)) {
        case Expression::Kind::kRelGT: {
            auto begin = OptimizerUtils::boundValue(col, BoundaryValueType::MAX);
            auto end =
                OptimizerUtils::boundValue(col, BoundaryValueType::GREATER_THAN, std::get<2>(item));
            return std::make_pair(begin, end);
        }
        case Expression::Kind::kRelGE: {
            auto begin = OptimizerUtils::boundValue(col, BoundaryValueType::MAX);
            return std::make_pair(begin, std::get<2>(item));
        }
        case Expression::Kind::kRelLE: {
            auto end = OptimizerUtils::boundValue(col, BoundaryValueType::MIN);
            return std::make_pair(std::get<2>(item), end);
        }
        case Expression::Kind::kRelLT: {
            auto begin =
                OptimizerUtils::boundValue(col, BoundaryValueType::LESS_THAN, std::get<2>(item));
            auto end = OptimizerUtils::boundValue(col, BoundaryValueType::MIN);
            return std::make_pair(begin, end);
        }
        default: {
            return Status::Error("Optimizer error when get range scan pair");
        }
    }
}

std::unique_ptr<Expression> IndexScanRule::assembleIndexFilter(const std::string& schema,
                                                               const IndexOptItems& items) {
    std::vector<std::unique_ptr<RelationalExpression>> exprs;
    for (const auto& item : items) {
        std::unique_ptr<RelationalExpression> rExp;
        if (isEdgeIndex_) {
            rExp = std::make_unique<RelationalExpression>(std::get<1>(item),
                new EdgePropertyExpression(new std::string(schema),
                                           new std::string(std::get<0>(item))),
                new ConstantExpression(std::get<2>(item)));
        } else {
            rExp = std::make_unique<RelationalExpression>(std::get<1>(item),
                new TagPropertyExpression(new std::string(schema),
                                          new std::string(std::get<0>(item))),
                new ConstantExpression(std::get<2>(item)));
        }
        exprs.emplace_back(std::move(rExp));
    }
    if (exprs.size() == 1) {
        return std::move(exprs[0]);
    } else {
        std::unique_ptr<LogicalExpression> lExpr =
            std::make_unique<LogicalExpression>(Expression::Kind::kLogicalAnd,
                                                exprs[0].get(),
                                                exprs[1].get());
        for (size_t i = 2; i < exprs.size(); i += 2) {
            if (i < exprs.size() - 1) {
                auto e1 = std::make_unique<LogicalExpression>(Expression::Kind::kLogicalAnd,
                                                              exprs[i].get(),
                                                              exprs[i+1].get());
                auto e2 = std::make_unique<LogicalExpression>(Expression::Kind::kLogicalAnd,
                                                              std::move(lExpr).get(),
                                                              std::move(e1).get());
                lExpr = std::move(e2);
            }
            if (i == exprs.size() -1) {
                auto e = std::make_unique<LogicalExpression>(Expression::Kind::kLogicalAnd,
                                                             std::move(lExpr).get(),
                                                             exprs[i].get());
                lExpr = std::move(e);
            }
        }
        return std::move(lExpr);
    }
}

Status IndexScanRule::postOptimize() {
    if (ctxs_->empty()) {
        return Status::Error("Optimizer error");
    }
    node_->setQueryContext(std::move(ctxs_));
    return Status::OK();
}

Status IndexScanRule::writeRelationalExpr(RelationalExpression* expr) {
    if (expr->left()->kind() == Expression::Kind::kSymProperty &&
        expr->right()->kind() == Expression::Kind::kConstant) {
        auto* l = dynamic_cast<const SymbolPropertyExpression*>(expr->left());
        auto* r = dynamic_cast<ConstantExpression*>(expr->right());
        QueryExpressionContext ctx(nullptr);
        relationalOpList_.emplace_back(std::make_tuple(*l->prop(), expr->kind(), r->eval(ctx)));
    } else if (expr->left()->kind() == Expression::Kind::kConstant &&
               expr->right()->kind() == Expression::Kind::kSymProperty) {
        auto* r = dynamic_cast<const SymbolPropertyExpression*>(expr->right());
        auto* l = dynamic_cast<ConstantExpression*>(expr->left());
        QueryExpressionContext ctx(nullptr);
        relationalOpList_.emplace_back(std::make_tuple(*r->prop(),
                                       reverseRelationalExprKind(expr->kind()),
                                       l->eval(ctx)));
    } else {
      return Status::Error("Optimizer error, when rewrite relational expression");
    }

    return Status::OK();
}

Expression::Kind IndexScanRule::reverseRelationalExprKind(Expression::Kind kind) {
    switch (kind) {
        case Expression::Kind::kRelGE: {
            return Expression::Kind::kRelLE;
        }
        case Expression::Kind::kRelGT: {
            return Expression::Kind::kRelLT;
        }
        case Expression::Kind::kRelLE: {
            return Expression::Kind::kRelGE;
        }
        case Expression::Kind::kRelLT: {
            return Expression::Kind::kRelGT;
        }
        default: {
            return kind;
        }
    }
}

}   // namespace graph
}   // namespace nebula
