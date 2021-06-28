/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/PushFilterDownEdgeIndexScanRule.h"
#include <memory>
#include <vector>

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/expression/Expression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/PropertyExpression.h"
#include "common/expression/RelationalExpression.h"
#include "common/interface/gen-cpp2/meta_types.h"
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
using nebula::graph::Filter;
using nebula::graph::OptimizerUtils;
using nebula::meta::cpp2::IndexItem;

using Kind = nebula::graph::PlanNode::Kind;
using ExprKind = nebula::Expression::Kind;
using TransformResult = nebula::opt::OptRule::TransformResult;
using IndexItems = std::vector<std::shared_ptr<IndexItem>>;

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

    return true;
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
    for (auto& index : indexItems) {
        auto status = OptimizerUtils::selectIndex(condition, *index);
        UNUSED(status);
    }

    return Status::OK();
}

std::string PushFilterDownEdgeIndexScanRule::toString() const {
    return "PushFilterDownEdgeIndexScanRule";
}

}   // namespace opt
}   // namespace nebula
