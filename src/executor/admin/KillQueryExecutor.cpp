/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/admin/KillQueryExecutor.h"

#include "context/QueryContext.h"
#include "planner/plan/Admin.h"

namespace nebula {
namespace graph {
folly::Future<Status> KillQueryExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto *killQuery = asNode<KillQuery>(node());
    auto inputVar = killQuery->inputVar();
    auto iter = ectx_->getResult(inputVar).iter();
    DCHECK(!!iter);
    QueryExpressionContext ctx(ectx_);
    auto sessionExpr = killQuery->sessionId();
    auto epExpr = killQuery->epId();

    // TODO: permision check

    std::vector<meta::cpp2::Session> sessionsInMeta;
    auto listSessionStatus = qctx()->getMetaClient()->listSessions().via(runner()).thenValue(
        [&sessionsInMeta](StatusOr<meta::cpp2::ListSessionsResp> resp) {
            if (!resp.ok()) {
                return resp.status();
            }
            sessionsInMeta = std::move(resp.value()).get_sessions();
            return Status::OK();
        }).get();
    if (!listSessionStatus.ok()) {
        // Ignore the errors.
        LOG(ERROR) << "List sessions error.";
    }

    auto* session = qctx()->rctx()->session();
    auto* sessionMgr = qctx_->rctx()->sessionMgr();
    std::unordered_map<SessionID, std::unordered_set<ExecutionPlanID>> killQueries;
    for (; iter->valid(); iter->next()) {
        auto& sessionVal = sessionExpr->eval(ctx(iter.get()));
        if (!sessionVal.isInt()) {
            std::stringstream ss;
            ss << "Session `" << sessionExpr->toString() << "' is not kind of"
                << " int, but was " << sessionVal.type();
            return Status::Error(ss.str());
        }
        auto& epVal = epExpr->eval(ctx(iter.get()));
        if (!epVal.isInt()) {
            std::stringstream ss;
            ss << "ExecutionPlanID `" << epExpr->toString() << "' is not kind of"
                << " int, but was " << epVal.type();
            return Status::Error(ss.str());
        }

        auto sessionId = sessionVal.getInt();
        auto epId = epVal.getInt();
        if (sessionId == session->id() || sessionId < 0) {
            if (!session->findQuery(epId)) {
                return Status::Error("ExecutionPlanId[%ld] does not exist in current Session.",
                                     epId);
            }
            session->markQueryKilled(epId);
            killQueries[session->id()].emplace(epId);
        } else {
            auto sessionPtr = sessionMgr->findSessionFromCache(sessionId);
            if (sessionPtr == nullptr) {
                auto found = std::find_if(
                    sessionsInMeta.begin(), sessionsInMeta.end(), [sessionId](auto& val) {
                        return val.get_session_id() == sessionId;
                    });
                if (found == sessionsInMeta.end()) {
                    return Status::Error("SessionId[%ld] does not exist", sessionId);
                }
                if (found->get_queries().find(epId) == found->get_queries().end()) {
                    return Status::Error("ExecutionPlanId[%ld] does not exist.", epId);
                }
            } else if (!sessionPtr->findQuery(epId)) {
                return Status::Error("ExecutionPlanId[%ld] does not exist.", epId);
            }
            killQueries[sessionId].emplace(epId);
        }
    }

    return qctx()
        ->getMetaClient()
        ->killQuery(std::move(killQueries))
        .via(runner())
        .thenValue([this](auto&& resp) {
            SCOPED_TIMER(&execTime_);
            if (!resp.ok()) {
                return Status::Error("Kill query failed.");
            }
            return Status::OK();
        });
}
}  // namespace graph
}  // namespace nebula
