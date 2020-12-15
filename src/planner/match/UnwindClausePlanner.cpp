/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/UnwindClausePlanner.h"

#include "planner/Query.h"
#include "planner/match/MatchSolver.h"
#include "planner/match/OrderByClausePlanner.h"
#include "planner/match/PaginationPlanner.h"
#include "planner/match/SegmentsConnector.h"
#include "visitor/RewriteMatchLabelVisitor.h"

namespace nebula {
namespace graph {
StatusOr<SubPlan> UnwindClausePlanner::transform(CypherClauseContextBase* clauseCtx) {
    if (clauseCtx->kind != CypherClauseKind::kUnwind) {
        return Status::Error("Not a valid context for UnwindClausePlanner.");
    }
    auto* unwindClauseCtx = static_cast<UnwindClauseContext*>(clauseCtx);

    SubPlan unwindPlan;
    NG_RETURN_IF_ERROR(buildUnwind(unwindClauseCtx, unwindPlan));
    return unwindPlan;
}

Status UnwindClausePlanner::buildUnwind(UnwindClauseContext* uctx, SubPlan& subPlan) {
    auto expr = uctx->expr;
    auto kind = expr->kind();
    auto rewriter = [uctx, this](const Expression* e) { return doRewrite(e); };
    Expression* newExpr = nullptr;
    if (kind == Expression::Kind::kLabel || kind == Expression::Kind::kLabelAttribute) {
        newExpr = rewriter(expr);
    } else {
        newExpr = expr->clone().release();
        RewriteMatchLabelVisitor visitor(rewriter);
        newExpr->accept(&visitor);
    }

    auto* unwind = Unwind::make(uctx->qctx, nullptr, newExpr);
    unwind->setColNames({uctx->aliases.begin()->first});
    subPlan.root = unwind;
    subPlan.tail = unwind;

    return Status::OK();
}

Expression* UnwindClausePlanner::doRewrite(const Expression* expr) {
    if (expr->kind() == Expression::Kind::kLabel) {
        return MatchSolver::rewrite(static_cast<const LabelExpression*>(expr));
    } else {
        DCHECK_EQ(expr->kind(), Expression::Kind::kLabelAttribute);
        return MatchSolver::rewrite(static_cast<const LabelAttributeExpression*>(expr));
    }
}

}   // namespace graph
}   // namespace nebula
