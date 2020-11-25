/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/planners/match/MatchPlanner.h"

#include "context/ast/QueryAstContext.h"
#include "planner/planners/match/MatchClausePlanner.h"
#include "planner/planners/match/ReturnClausePlanner.h"
#include "planner/planners/match/UnwindClausePlanner.h"
#include "planner/planners/match/WithClausePlanner.h"

namespace nebula {
namespace graph {
bool MatchPlanner::match(AstContext* astCtx) {
    if (astCtx->sentence->kind() == Sentence::Kind::kMatch) {
        return true;
    } else {
        return false;
    }
}

StatusOr<SubPlan> transform(AstContext* astCtx) {
    if (astCtx->sentence->kind() != Sentence::Kind::kMatch) {
        return Status::Error("Only MATCH is accepted for match planner.");
    }
    auto* matchCtx = static_cast<MatchAstContext*>(astCtx);

    std::vector<SubPlan> subplans;
    for (auto& clauseCtx : matchCtx->clauses) {
        switch (clauseCtx->kind) {
            case CypherClauseKind::kMatch: {
                auto subplan = std::make_unique<MatchClausePlanner>()->transform(clauseCtx.get());
                if (!subplan.ok()) {
                    return subplan.status();
                }
                subplans.emplace_back(std::move(subplan).value());
                break;
            }
            case CypherClauseKind::kUnwind: {
                auto subplan = std::make_unique<UnwindClausePlanner>()->transform(clauseCtx.get());
                if (!subplan.ok()) {
                    return subplan.status();
                }
                subplans.emplace_back(std::move(subplan).value());
                break;
            }
            case CypherClauseKind::kWith: {
                auto subplan = std::make_unique<WithClausePlanner>()->transform(clauseCtx.get());
                if (!subplan.ok()) {
                    return subplan.status();
                }
                subplans.emplace_back(std::move(subplan).value());
                break;
            }
            case CypherClauseKind::kReturn: {
                auto subplan = std::make_unique<ReturnClausePlanner>()->transform(clauseCtx.get());
                if (!subplan.ok()) {
                    return subplan.status();
                }
                subplans.emplace_back(std::move(subplan).value());
                break;
            }
            default: { return Status::Error("Unsupported clause."); }
        }
    }

    UNUSED(subplans);
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
