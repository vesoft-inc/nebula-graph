/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/planners/match/ValueIndexSeek.h"

#include "planner/Query.h"
#include "planner/planners/match/MatchSolver.h"
#include "util/ExpressionUtils.h"

namespace nebula {
namespace graph {
bool ValueIndexSeek::match(PatternContext* patternCtx) {
    if (patternCtx->kind == PatternKind::kNode) {
        auto* nodeCtx = static_cast<NodeContext*>(patternCtx);
        return matchNode(nodeCtx);
    }

    if (patternCtx->kind == PatternKind::kEdge) {
        // TODO
        return false;
    }

    return false;
}

bool ValueIndexSeek::matchNode(NodeContext* nodeCtx) {
    auto& node = *nodeCtx->info;
    if (node.label == nullptr) {
        return false;
    }

    auto* matchClauseCtx = nodeCtx->matchClauseCtx;
    Expression* filter = nullptr;
    if (matchClauseCtx->where->filter != nullptr) {
        filter = MatchSolver::makeIndexFilter(
            *node.label, *node.alias, matchClauseCtx->where->filter.get(), matchClauseCtx->qctx);
    }
    if (filter == nullptr) {
        if (node.props != nullptr && !node.props->items().empty()) {
            filter = MatchSolver::makeIndexFilter(*node.label, node.props, matchClauseCtx->qctx);
        }
    }

    if (filter == nullptr) {
        return false;
    }

    nodeCtx->scanInfo.filter = filter;
    nodeCtx->scanInfo.schemaId = node.tid;

    return true;
}

StatusOr<SubPlan> ValueIndexSeek::transform(PatternContext* patternCtx) {
    if (patternCtx->kind == PatternKind::kNode) {
        auto* nodeCtx = static_cast<NodeContext*>(patternCtx);
        return transformNode(nodeCtx);
    }

    if (patternCtx->kind == PatternKind::kEdge) {
        // TODO
        return Status::Error();
    }

    return Status::Error();
}

StatusOr<SubPlan> ValueIndexSeek::transformNode(NodeContext* nodeCtx) {
    SubPlan plan;
    auto* matchClauseCtx = nodeCtx->matchClauseCtx;
    using IQC = nebula::storage::cpp2::IndexQueryContext;
    IQC iqctx;
    iqctx.set_filter(Expression::encode(*nodeCtx->scanInfo.filter));
    auto contexts = std::make_unique<std::vector<IQC>>();
    contexts->emplace_back(std::move(iqctx));
    auto columns = std::make_unique<std::vector<std::string>>();
    auto scan = IndexScan::make(matchClauseCtx->qctx,
                                nullptr,
                                matchClauseCtx->space.id,
                                std::move(contexts),
                                std::move(columns),
                                false,
                                nodeCtx->scanInfo.schemaId);
    plan.tail = scan;
    plan.root = scan;

    // initialize start expression in project node
    nodeCtx->initialExpr = ExpressionUtils::newVarPropExpr(kVid);
    return plan;
}
}  // namespace graph
}  // namespace nebula
