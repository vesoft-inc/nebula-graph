/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/IndexScanRule.h"
#include "common/expression/LabelAttributeExpression.h"
#include "optimizer/OptGroup.h"
#include "planner/PlanNode.h"
#include "planner/Query.h"

namespace nebula {
namespace opt {
std::unique_ptr<OptRule> IndexScanRule::kInstance =
    std::unique_ptr<IndexScanRule>(new IndexScanRule());

IndexScanRule::IndexScanRule() {
    RuleSet::queryRules().addRule(this);
}

bool IndexScanRule::match(const OptGroupExpr *groupExpr) const {
    return groupExpr->node()->kind() == PlanNode::Kind::kIndexScan;
}

Status IndexScanRule::transform(graph::QueryContext *qctx,
                                const OptGroupExpr *groupExpr,
                                TransformResult *result) const {
    FilterItems items;
    ScanKind kind;
    auto filter = filterExpr(groupExpr);
    if (filter == nullptr) {
        return Status::SemanticError();
    }
    auto ret = analyzeExpression(filter.get(), &items, &kind);
    NG_RETURN_IF_ERROR(ret);

    IndexQueryCtx iqctx = std::make_unique<std::vector<IndexQueryContext>>();
    ret = createIndexQueryCtx(iqctx, kind, items, qctx, groupExpr);
    NG_RETURN_IF_ERROR(ret);

    auto newIG = cloneIndexScan(qctx, groupExpr);
    newIG->setQueryContext(std::move(iqctx));
    auto newGroupExpr = OptGroupExpr::create(qctx, newIG, nullptr);
    result->newGroupExprs.emplace_back(newGroupExpr);
    return Status::OK();
}

std::string IndexScanRule::toString() const {
    return "IndexScanRule";
}

Status IndexScanRule::createIndexQueryCtx(IndexQueryCtx &iqctx,
                                          ScanKind kind,
                                          const FilterItems& items,
                                          graph::QueryContext *qctx,
                                          const OptGroupExpr *groupExpr) const {
    return kind.isLogicalAnd()
           ? createIQCWithLogicAnd(iqctx, items, qctx, groupExpr)
           : createIQCWithLogicOR(iqctx, items, qctx, groupExpr);
}

Status IndexScanRule::createIQCWithLogicAnd(IndexQueryCtx &iqctx,
                                            const FilterItems& items,
                                            graph::QueryContext *qctx,
                                            const OptGroupExpr *groupExpr) const {
    auto index = findOptimalIndex(qctx, groupExpr, items);
    if (index == nullptr) {
        return Status::IndexNotFound();
    }

    return appendIQCtx(index, items, iqctx);
}

Status IndexScanRule::createIQCWithLogicOR(IndexQueryCtx &iqctx,
                                           const FilterItems& items,
                                           graph::QueryContext *qctx,
                                           const OptGroupExpr *groupExpr) const {
    for (auto const& item : items.items) {
        auto index = findOptimalIndex(qctx, groupExpr, FilterItems({item}));
        if (index == nullptr) {
            return Status::IndexNotFound();
        }
        auto ret = appendIQCtx(index, FilterItems({item}), iqctx);
        NG_RETURN_IF_ERROR(ret);
    }
    return Status::OK();
}

Status IndexScanRule::appendIQCtx(const IndexItem& index,
                                  const FilterItems& items,
                                  IndexQueryCtx &iqctx) const {
    auto fields = index->get_fields();
    IndexQueryContext ctx;
    decltype(ctx.column_hints) hints;
    for (const auto& field : fields) {
        bool found = false;
        FilterItems fieldItems;
        for (const auto& item : items.items) {
            if (std::get<0>(item) != field.get_name()) {
                continue;
            }
            fieldItems.addItem(std::get<0>(item), std::get<1>(item), std::get<2>(item));
            found = true;
        }
        if (!found) break;
        // TODO (sky) : rewrite filter expr. NE expr should be add filter expr .
        auto it = std::find_if(fieldItems.items.begin(), fieldItems.items.end(),
                               [](const auto &ite) {
                                   return std::get<1>(ite) == RelationalExpression::Kind::kRelNE;
                               });
        if (it != fieldItems.items.end()) {
            break;
        }
        auto ret = appendColHint(hints, fieldItems, field);
        NG_RETURN_IF_ERROR(ret);
    }
    ctx.set_index_id(index->get_index_id());
    // TODO (sky) : rewrite expr and set filter
    ctx.set_column_hints(std::move(hints));
    iqctx->emplace_back(std::move(ctx));
    return Status::OK();
}

#define CHECK_BOUND_VALUE(v)                         \
if (v == Value(NullType::BAD_TYPE)) {                \
    LOG(ERROR) << "Encode value error . field : "    \
           << col.get_name();                        \
    return Status::Error();                          \
}

Status IndexScanRule::appendColHint(std::vector<IndexColumnHint>& hints,
                                    const FilterItems& items,
                                    const meta::cpp2::ColumnDef& col) const {
    IndexColumnHint hint;
    Value begin, end;
    bool isRangeScan = true;
    for (const auto& item : items.items) {
        if (std::get<1>(item) == Expression::Kind::kRelEQ) {
            // check the items, don't allow where c1 == 1 and c1 == 2 and c1 > 3....
            // If EQ item appears, only one element is allowed
            if (items.items.size() > 1) {
                return Status::SemanticError();
            }
            isRangeScan = false;
            begin = std::get<2>(item);
            break;
        }
        auto ret = boundValue(item, col, begin, end);
        NG_RETURN_IF_ERROR(ret);
    }

    if (isRangeScan) {
        if (begin == Value()) {
            begin = OptimizerUtils::boundValue(col, BVO::MIN, Value());
            CHECK_BOUND_VALUE(begin);
        }
        if (end == Value()) {
            end = OptimizerUtils::boundValue(col, BVO::MAX, Value());
            CHECK_BOUND_VALUE(end);
        }
        hint.set_scan_type(storage::cpp2::ScanType::RANGE);
        hint.set_end_value(std::move(end));
    } else {
        hint.set_scan_type(storage::cpp2::ScanType::PREFIX);
    }
    hint.set_begin_value(std::move(begin));
    hint.set_column_name(col.get_name());
    hints.emplace_back(std::move(hint));
    return Status::OK();
}

Status IndexScanRule::boundValue(const FilterItem& item,
                                 const meta::cpp2::ColumnDef& col,
                                 Value& begin, Value& end) const {
    auto val = std::get<2>(item);
    if (val.type() != graph::SchemaUtil::propTypeToValueType(col.get_type())) {
        return Status::SemanticError("Data type of field : %s", col.get_name().c_str());
    }
    switch (std::get<1>(item)) {
        case Expression::Kind::kRelLE: {
            // if c1 <= int(5) , the range pair should be (min, 6)
            // if c1 < int(5), the range pair should be (min, 5)
            auto v = OptimizerUtils::boundValue(col, BVO::GREATER_THAN, val);
            CHECK_BOUND_VALUE(v);
            // where c <= 1 and c <= 2 , 1 should be valid.
            if (end == Value()) {
                end = v;
            } else {
                end = v < end ? v : end;
            }
            break;
        }
        case Expression::Kind::kRelGE: {
            // where c >= 1 and c >= 2 , 2 should be valid.
            if (begin == Value()) {
                begin = val;
            } else {
                begin = val < begin ? begin : val;
            }
            break;
        }
        case Expression::Kind::kRelLT: {
            // c < 5 and c < 6 , 5 should be valid.
            if (end == Value()) {
                end = val;
            } else {
                end = val < end ? val : end;
            }
            break;
        }
        case Expression::Kind::kRelGT: {
            // if c >= 5, the range pair should be (5, max)
            // if c > 5, the range pair should be (6, max)
            auto v = OptimizerUtils::boundValue(col, BVO::GREATER_THAN, val);
            CHECK_BOUND_VALUE(v);
            // where c > 1 and c > 2 , 2 should be valid.
            if (begin == Value()) {
                begin = v;
            } else {
                begin = v < begin ? begin : v;
            }
            break;
        }
        default:
            return Status::SemanticError();
    }
    return Status::OK();
}

IndexScan* IndexScanRule::cloneIndexScan(graph::QueryContext *qctx,
                                         const OptGroupExpr *groupExpr) const {
    auto ig = dynamic_cast<const IndexScan *>(groupExpr->node());
    auto ctx = std::make_unique<std::vector<storage::cpp2::IndexQueryContext>>();
    auto returnCols = std::make_unique<std::vector<std::string>>(*ig->returnColumns());
    auto indexScan = IndexScan::make(qctx,
                                     nullptr,
                                     ig->space(),
                                     std::move(ctx),
                                     std::move(returnCols),
                                     ig->isEdge(),
                                     ig->schemaId());
    return indexScan;
}

bool IndexScanRule::isEdge(const OptGroupExpr *groupExpr) const {
    auto ig = dynamic_cast<const IndexScan *>(groupExpr->node());
    return ig->isEdge();
}

int32_t IndexScanRule::schemaId(const OptGroupExpr *groupExpr) const {
    auto ig = dynamic_cast<const IndexScan *>(groupExpr->node());
    return ig->schemaId();
}

GraphSpaceID IndexScanRule::spaceId(const OptGroupExpr *groupExpr) const {
    auto ig = dynamic_cast<const IndexScan *>(groupExpr->node());
    return ig->space();
}

std::unique_ptr<Expression>
IndexScanRule::filterExpr(const OptGroupExpr *groupExpr) const {
    auto ig = dynamic_cast<const IndexScan *>(groupExpr->node());
    auto qct = ig->queryContext();
    // The initial IndexScan plan node has only one queryContext.
    if (qct->size() != 1) {
        LOG(ERROR) << "Index Scan plan node error";
        return nullptr;
    }
    return Expression::decode(qct->begin()->get_filter());
}

Status IndexScanRule::analyzeExpression(Expression* expr,
                                        FilterItems* items,
                                        ScanKind* kind) const {
    // TODO (sky) : Currently only simple logical expressions are supported,
    //              such as all AND or all OR expressions, example :
    //              where c1 > 1 and c1 < 2 and c2 == 1
    //              where c1 == 1 or c2 == 1 or c3 == 1
    //
    //              Hybrid logical expressions are not supported yet, example :
    //              where c1 > 1 and c2 >1 or c3 > 1
    //              where c1 > 1 and c1 < 2 or c2 > 1
    //              where c1 < 1 and (c2 == 1 or c2 == 2)
    switch (expr->kind()) {
        case Expression::Kind::kLogicalOr :
        case Expression::Kind::kLogicalAnd : {
            auto lExpr = dynamic_cast<LogicalExpression*>(expr);
            auto k = expr->kind() == Expression::Kind::kLogicalAnd
                     ? ScanKind::Kind::LOGICAL_AND : ScanKind::Kind::LOGICAL_OR;
            if (kind->getKind() == ScanKind::Kind::UNKNOWN) {
                kind->setKind(k);
            } else if (kind->getKind() != k) {
                auto errorMsg = folly::StringPiece("Condition not support yet : %s",
                                                   Expression::encode(*expr).c_str());
                return Status::NotSupported(errorMsg);
            }
            auto ret = analyzeExpression(lExpr->left(), items, kind);
            NG_RETURN_IF_ERROR(ret);
            ret = analyzeExpression(lExpr->right(), items, kind);
            NG_RETURN_IF_ERROR(ret);
            break;
        }
        case Expression::Kind::kRelLE:
        case Expression::Kind::kRelGE:
        case Expression::Kind::kRelEQ:
        case Expression::Kind::kRelLT:
        case Expression::Kind::kRelGT:
        case Expression::Kind::kRelNE: {
            auto* rExpr = dynamic_cast<RelationalExpression*>(expr);
            auto ret = writeRelationalExpr(rExpr, items);
            NG_RETURN_IF_ERROR(ret);
            break;
        }
        default: {
            auto errorMsg = folly::StringPiece("Filter not support yet : %s",
                                               Expression::encode(*expr).c_str());
            return Status::NotSupported(errorMsg);
        }
    }
    return Status::OK();
}


Status IndexScanRule::writeRelationalExpr(RelationalExpression* expr, FilterItems* items) const {
    graph::QueryExpressionContext ctx(nullptr);
    if (expr->left()->kind() == Expression::Kind::kLabelAttribute &&
        expr->right()->kind() == Expression::Kind::kConstant) {
        auto* l = dynamic_cast<const LabelAttributeExpression*>(expr->left());
        auto* r = dynamic_cast<ConstantExpression*>(expr->right());
        items->addItem(*l->left()->name(), expr->kind(), r->eval(ctx));
    } else if (expr->left()->kind() == Expression::Kind::kConstant &&
               expr->right()->kind() == Expression::Kind::kLabelAttribute) {
        auto* r = dynamic_cast<const LabelAttributeExpression*>(expr->right());
        auto* l = dynamic_cast<ConstantExpression*>(expr->left());

        items->addItem(*r->left()->name(), reverseRelationalExprKind(expr->kind()), l->eval(ctx));
    } else {
        return Status::Error("Optimizer error, when rewrite relational expression");
    }

    return Status::OK();
}

Expression::Kind IndexScanRule::reverseRelationalExprKind(Expression::Kind kind) const {
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

IndexItem IndexScanRule::findOptimalIndex(graph::QueryContext *qctx,
                                          const OptGroupExpr *groupExpr,
                                          const FilterItems& items) const {
    // The rule of priority is '==' --> '< > <= >=' --> '!='
    // Step 1 : find out all valid indexes for where condition.
    auto validIndexes = findValidIndex(qctx, groupExpr, items);
    if (validIndexes.empty()) {
        LOG(ERROR) << "No valid index found";
        return nullptr;
    }
    // Step 2 : find optimal indexes for equal condition.
    auto indexesEq = findIndexForEqualScan(validIndexes, items);
    if (indexesEq.size() == 1) {
        return indexesEq[0];
    }
    // Step 3 : find optimal indexes for range condition.
    auto indexesRange = findIndexForRangeScan(indexesEq, items);

    // At this stage, all the optimizations are done.
    // Because the storage layer only needs one. So return first one of indexesRange.
    return indexesRange[0];
}

std::vector<IndexItem>
IndexScanRule::allIndexesBySchema(graph::QueryContext *qctx,
                                  const OptGroupExpr *groupExpr) const {
    auto ret = isEdge(groupExpr)
               ? qctx->getMetaClient()->getEdgeIndexesFromCache(spaceId(groupExpr))
               : qctx->getMetaClient()->getTagIndexesFromCache(spaceId(groupExpr));
    if (!ret.ok()) {
        LOG(ERROR) << "No index was found";
        return {};
    }
    std::vector<IndexItem> indexes;
    for (auto& index : ret.value()) {
        // TODO (sky) : ignore rebuilding indexes
        auto id = isEdge(groupExpr)
                  ? index->get_schema_id().get_edge_type()
                  : index->get_schema_id().get_tag_id();
        if (id == schemaId(groupExpr)) {
            indexes.emplace_back(index);
        }
    }
    if (indexes.empty()) {
        LOG(ERROR) << "No index was found";
        return {};
    }
    return indexes;
}

std::vector<IndexItem>
IndexScanRule::findValidIndex(graph::QueryContext *qctx,
                              const OptGroupExpr *groupExpr,
                              const FilterItems& items) const {
    auto indexes = allIndexesBySchema(qctx, groupExpr);
    if (indexes.empty()) {
        return indexes;
    }
    std::vector<IndexItem> validIndexes;
    // Find indexes for match all fields by where condition.
    for (const auto& index : indexes) {
        bool allColsHint = true;
        const auto& fields = index->get_fields();
        for (const auto& item : items.items) {
            auto it = std::find_if(fields.begin(), fields.end(),
                                   [item](const auto &field) {
                                       return field.get_name() == std::get<0>(item);
                                   });
            if (it == fields.end()) {
                allColsHint = false;
                break;
            }
        }
        if (allColsHint) {
            validIndexes.emplace_back(index);
        }
    }
    // If the first field of the index does not match any condition, the index is invalid.
    // remove it from validIndexes.
    if (!validIndexes.empty()) {
        auto index = validIndexes.begin();
        while (index != validIndexes.end()) {
            const auto& fields = index->get()->get_fields();
            auto it = std::find_if(items.items.begin(), items.items.end(),
                                   [fields](const auto &item) {
                                       return std::get<0>(item) == fields[0].get_name();
                                   });
            if (it == items.items.end()) {
                validIndexes.erase(index);
            } else {
                index++;
            }
        }
    }
    return validIndexes;
}

std::vector<IndexItem>
IndexScanRule::findIndexForEqualScan(const std::vector<IndexItem>& indexes,
                                     const FilterItems& items) const {
    std::vector<std::pair<int32_t, IndexItem>> eqIndexHint;
    for (auto& index : indexes) {
        int32_t hintCount = 0;
        for (const auto& field : index->get_fields()) {
            auto it = std::find_if(items.items.begin(), items.items.end(),
                                   [field](const auto &item) {
                                       return std::get<0>(item) == field.get_name();
                                   });
            if (it == items.items.end()) {
                break;
            }
            if (std::get<1>(*it) == RelationalExpression::Kind::kRelEQ) {
                ++hintCount;
            } else {
                break;
            }
        }
        eqIndexHint.emplace_back(hintCount, index);
    }
    // Sort the priorityIdxs for equivalent condition.
    std::vector<IndexItem> priorityIdxs;
    auto comp = [] (std::pair<int32_t, IndexItem>& lhs,
                    std::pair<int32_t, IndexItem>& rhs) {
        return lhs.first > rhs.first;
    };
    std::sort(eqIndexHint.begin(), eqIndexHint.end(), comp);
    // Get the index with the highest hit rate from eqIndexHint.
    int32_t maxHint = eqIndexHint[0].first;
    for (const auto& hint : eqIndexHint) {
        if (hint.first < maxHint) {
            break;
        }
        priorityIdxs.emplace_back(hint.second);
    }
    return priorityIdxs;
}

std::vector<IndexItem>
IndexScanRule::findIndexForRangeScan(const std::vector<IndexItem>& indexes,
                                     const FilterItems& items) const {
    std::map<int32_t, IndexItem> rangeIndexHint;
    for (const auto& index : indexes) {
        int32_t hintCount = 0;
        for (const auto& field : index->get_fields()) {
            auto fi = std::find_if(items.items.begin(), items.items.end(),
                                   [field](const auto &item) {
                                       return std::get<0>(item) == field.get_name();
                                   });
            if (fi == items.items.end()) {
                break;
            }
            if (std::get<1>(*fi) == RelationalExpression::Kind::kRelEQ) {
                continue;
            }
            if (std::get<1>(*fi) == RelationalExpression::Kind::kRelGE ||
                std::get<1>(*fi) == RelationalExpression::Kind::kRelGT ||
                std::get<1>(*fi) == RelationalExpression::Kind::kRelLE ||
                std::get<1>(*fi) == RelationalExpression::Kind::kRelLT) {
                hintCount++;
            } else {
                break;
            }
        }
        rangeIndexHint[hintCount] = index;
    }
    std::vector<IndexItem> priorityIdxs;
    int32_t maxHint = rangeIndexHint.rbegin()->first;
    for (auto iter = rangeIndexHint.rbegin(); iter != rangeIndexHint.rend(); iter++) {
        if (iter->first < maxHint) {
            break;
        }
        priorityIdxs.emplace_back(iter->second);
    }
    return priorityIdxs;
}

}   // namespace opt
}   // namespace nebula
