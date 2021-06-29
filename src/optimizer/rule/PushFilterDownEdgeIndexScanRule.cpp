/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/PushFilterDownEdgeIndexScanRule.h"
#include <algorithm>
#include <memory>
#include <vector>

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/expression/Expression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/PropertyExpression.h"
#include "common/expression/RelationalExpression.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "context/QueryContext.h"
#include "optimizer/OptContext.h"
#include "optimizer/OptGroup.h"
#include "optimizer/OptimizerUtils.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Query.h"

using nebula::Expression;
using nebula::graph::EdgeIndexFullScan;
using nebula::graph::EdgeIndexPrefixScan;
using nebula::graph::EdgeIndexRangeScan;
using nebula::graph::EdgeIndexScan;
using nebula::graph::Filter;
using nebula::graph::OptimizerUtils;
using nebula::graph::QueryContext;
using nebula::meta::cpp2::IndexItem;
using nebula::storage::cpp2::IndexQueryContext;

using Kind = nebula::graph::PlanNode::Kind;
using ExprKind = nebula::Expression::Kind;
using TransformResult = nebula::opt::OptRule::TransformResult;
using IndexItems = std::vector<std::shared_ptr<IndexItem>>;
using IndexResult = nebula::graph::OptimizerUtils::IndexResult;
using IndexPriority = nebula::graph::OptimizerUtils::IndexPriority;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushFilterDownEdgeIndexScanRule::kInstance =
    std::unique_ptr<PushFilterDownEdgeIndexScanRule>(new PushFilterDownEdgeIndexScanRule());

PushFilterDownEdgeIndexScanRule::PushFilterDownEdgeIndexScanRule() {
    RuleSet::QueryRules().addRule(this);
}

const Pattern& PushFilterDownEdgeIndexScanRule::pattern() const {
    static Pattern pattern =
        Pattern::create(Kind::kFilter, {Pattern::create(Kind::kEdgeIndexFullScan)});
    return pattern;
}

bool PushFilterDownEdgeIndexScanRule::match(OptContext* ctx, const MatchedResult& matched) const {
    UNUSED(ctx);
    auto filter = static_cast<const Filter*>(matched.planNode());
    auto condition = filter->condition();
    if (condition->isRelExpr()) {
        auto relExpr = static_cast<const RelationalExpression*>(condition);
        return relExpr->left()->kind() == ExprKind::kEdgeProperty &&
               relExpr->right()->kind() == ExprKind::kConstant;
    }
    if (condition->isLogicalExpr()) {
        return condition->kind() == Expression::Kind::kLogicalAnd;
    }

    return false;
}

EdgeIndexScan* makeEdgeIndexScan(QueryContext* qctx, const EdgeIndexScan* scan, bool isPrefixScan) {
    EdgeIndexScan* scanNode = nullptr;
    if (isPrefixScan) {
        scanNode = EdgeIndexPrefixScan::make(qctx, nullptr, scan->edgeType());
    } else {
        scanNode = EdgeIndexRangeScan::make(qctx, nullptr, scan->edgeType());
    }
    scanNode->setDedup(scan->dedup());
    scanNode->setFilter(scan->filter());
    scanNode->setLimit(scan->limit());
    scanNode->setOrderBy(scan->orderBy());
    scanNode->setReturnCols(scan->returnColumns());
    scanNode->setSchemaId(scan->schemaId());
    scanNode->setEmptyResultSet(scan->isEmptyResultSet());
    scanNode->setSpace(scan->space());
    return scanNode;
}

StatusOr<TransformResult> PushFilterDownEdgeIndexScanRule::transform(
    OptContext* ctx,
    const MatchedResult& matched) const {
    auto filter = static_cast<const Filter*>(matched.planNode());
    auto scan = static_cast<const EdgeIndexFullScan*>(matched.planNode({0, 0}));

    auto metaClient = ctx->qctx()->getMetaClient();
    auto status = metaClient->getEdgeIndexesFromCache(scan->space());
    NG_RETURN_IF_ERROR(status);
    auto indexItems = std::move(status).value();

    // Erase invalid index items
    for (auto iter = indexItems.begin(); iter != indexItems.end();) {
        if ((*iter)->get_schema_id().get_edge_type() != scan->schemaId()) {
            iter = indexItems.erase(iter);
        } else {
            iter++;
        }
    }

    // Return directly if there is not valid index to use.
    if (indexItems.empty()) {
        return TransformResult::noTransform();
    }

    auto condition = filter->condition();
    std::vector<IndexResult> results;
    for (auto& index : indexItems) {
        auto resStatus = OptimizerUtils::selectIndex(condition, *index);
        if (resStatus.ok()) {
            results.emplace_back(std::move(resStatus).value());
        }
    }

    if (results.empty()) {
        return TransformResult::noTransform();
    }

    std::sort(results.begin(), results.end());

    auto& index = results.front();
    if (index.hints.empty()) {
        return TransformResult::noTransform();
    }

    bool isPrefixScan = false;
    std::vector<IndexQueryContext> idxCtxs;
    IndexQueryContext ictx;
    ictx.set_index_id(index.index->get_index_id());
    std::vector<storage::cpp2::IndexColumnHint> hints;
    hints.reserve(index.hints.size());
    auto iter = index.hints.begin();
    for (; iter != index.hints.end(); ++iter) {
        auto& hint = *iter;
        if (hint.priority == IndexPriority::kPrefix) {
            hints.emplace_back(std::move(hint.hint));
            isPrefixScan = true;
            continue;
        }
        if (hint.priority == IndexPriority::kRange) {
            hints.emplace_back(std::move(hint.hint));
            break;
        }
    }
    if (iter != index.hints.end() || index.unusedExpr) {
        ictx.set_filter(condition->encode());
    }
    ictx.set_column_hints(std::move(hints));
    idxCtxs.emplace_back(std::move(ictx));

    EdgeIndexScan* scanNode = makeEdgeIndexScan(ctx->qctx(), scan, isPrefixScan);
    scanNode->setIndexQueryContext(std::move(idxCtxs));
    scanNode->setOutputVar(filter->outputVar());
    scanNode->setColNames(filter->colNames());
    auto filterGroup = matched.node->group();
    auto optScanNode = OptGroupNode::create(ctx, scanNode, filterGroup);
    for (auto group : matched.dependencies[0].node->dependencies()) {
        optScanNode->dependsOn(group);
    }
    TransformResult result;
    result.newGroupNodes.emplace_back(optScanNode);
    result.eraseCurr = true;
    return result;
}

std::string PushFilterDownEdgeIndexScanRule::toString() const {
    return "PushFilterDownEdgeIndexScanRule";
}

}   // namespace opt
}   // namespace nebula
