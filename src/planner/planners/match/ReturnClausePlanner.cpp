/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/planners/match/ReturnClausePlanner.h"

#include "planner/Query.h"
#include "planner/planners/match/MatchSolver.h"
#include "planner/planners/match/OrderByClausePlanner.h"
#include "planner/planners/match/PaginationPlanner.h"
#include "planner/planners/match/SegmentsConnector.h"
#include "visitor/RewriteMatchLabelVisitor.h"

namespace nebula {
namespace graph {
StatusOr<SubPlan> ReturnClausePlanner::transform(CypherClauseContextBase* clauseCtx) {
    if (clauseCtx->kind != CypherClauseKind::kReturn) {
        return Status::Error("Not a valid context for ReturnClausePlanner.");
    }
    auto* returnClauseCtx = static_cast<ReturnClauseContext*>(clauseCtx);

    SubPlan returnPlan;
    NG_RETURN_IF_ERROR(buildReturn(returnClauseCtx, returnPlan));
    return returnPlan;
}

Status ReturnClausePlanner::buildReturn(ReturnClauseContext* rctx, SubPlan& subPlan) {
    auto *yields = new YieldColumns();
    std::vector<std::string> colNames;
    PlanNode *current = subPlan.root;

    DCHECK(rctx->aliases != nullptr);
    auto rewriter = [rctx](const Expression *expr) {
        return MatchSolver::doRewrite(*rctx->aliases, expr);
    };

    for (auto *col : rctx->yieldColumns->columns()) {
        auto kind = col->expr()->kind();
        YieldColumn *newColumn = nullptr;
        if (kind == Expression::Kind::kLabel || kind == Expression::Kind::kLabelAttribute) {
            newColumn = new YieldColumn(rewriter(col->expr()));
        } else {
            auto newExpr = col->expr()->clone();
            RewriteMatchLabelVisitor visitor(rewriter);
            newExpr->accept(&visitor);
            newColumn = new YieldColumn(newExpr.release());
        }
        yields->addColumn(newColumn);
        if (col->alias() != nullptr) {
            colNames.emplace_back(*col->alias());
        } else {
            colNames.emplace_back(col->expr()->toString());
        }
    }

    auto *project = Project::make(rctx->qctx, current, yields);
    project->setInputVar(current->outputVar());
    project->setColNames(std::move(colNames));
    current = project;

    if (rctx->distinct) {
        auto *dedup = Dedup::make(rctx->qctx, current);
        dedup->setInputVar(current->outputVar());
        dedup->setColNames(current->colNames());
        current = dedup;
    }

    subPlan.root = current;

    if (rctx->order != nullptr) {
        auto orderPlan = std::make_unique<OrderByClausePlanner>()->transform(rctx->order.get());
        NG_RETURN_IF_ERROR(orderPlan);
        auto plan = std::move(orderPlan).value();
        SegmentsConnector::addDependency(plan.tail, subPlan.root);
        subPlan.root = plan.root;
    }

    if (rctx->pagination != nullptr) {
        auto paginationPlan =
            std::make_unique<PaginationPlanner>()->transform(rctx->pagination.get());
        NG_RETURN_IF_ERROR(paginationPlan);
        auto plan = std::move(paginationPlan).value();
        SegmentsConnector::addDependency(plan.tail, subPlan.root);
        subPlan.root = plan.root;
    }

    // TODO: Handle grouping

    return Status::OK();
}

}  // namespace graph
}  // namespace nebula