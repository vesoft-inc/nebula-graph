/* Copyright (c) 2021 vesoft inc. All rights reserved.
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

    // TODO: permision check

    QueriesMap toBeVerifiedQueries;
    KillQueriesMap killQueries;
    NG_RETURN_IF_ERROR(verifyTheQueriesByLocalCache(toBeVerifiedQueries, killQueries));

    folly::Promise<Status> pro;
    result = pro.getFuture();
    qctx()->getMetaClient()->listSessions().via(runner()).thenValue(
        [toBeVerifiedQueries = std::move(toBeVerifiedQueries),
         killQueries = std::move(killQueries),
         pro = std::move(pro),
         this](StatusOr<meta::cpp2::ListSessionsResp> listResp) mutable {
            std::vector<meta::cpp2::Session> sessionsInMeta;
            if (listResp.ok()) {
                sessionsInMeta = std::move(listResp.value()).get_sessions();
            } else {
                LOG(ERROR) << listResp.status();
            }

            auto status = verifyTheQueriesByMetaInfo(toBeVerifiedQueries, sessionsInMeta);
            if (!status.ok()) {
                pro.setValue(std::move(status));
                return;
            }

            killCurrentHostQueries(killQueries);

            // upload all queries to be killed to meta.
            qctx()
                ->getMetaClient()
                ->killQuery(std::move(killQueries))
                .via(runner())
                .thenValue([pro = std::move(pro), this](auto&& resp) mutable {
                    SCOPED_TIMER(&execTime_);
                    pro.setValue(resp.status());
                });
        });
    return result;
}

Status KillQueryExecutor::verifyTheQueriesByLocalCache(QueriesMap& toBeVerifiedQueries,
                                                       KillQueriesMap& killQueries) {
    auto* killQuery = asNode<KillQuery>(node());
    auto inputVar = killQuery->inputVar();
    auto iter = ectx_->getResult(inputVar).iter();
    DCHECK(!!iter);
    QueryExpressionContext ctx(ectx_);
    auto sessionExpr = killQuery->sessionId();
    auto epExpr = killQuery->epId();

    auto* session = qctx()->rctx()->session();
    auto* sessionMgr = qctx_->rctx()->sessionMgr();
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
            killQueries[session->id()].emplace(epId);
        } else {
            auto sessionPtr = sessionMgr->findSessionFromCache(sessionId);
            if (sessionPtr == nullptr) {
                toBeVerifiedQueries[sessionId].emplace(epId);
            } else if (!sessionPtr->findQuery(epId)) {
                return Status::Error("ExecutionPlanId[%ld] does not exist in Session[%ld].",
                                     epId, sessionId);
            }
            killQueries[sessionId].emplace(epId);
        }
    }
    return Status::OK();
}

void KillQueryExecutor::killCurrentHostQueries(const QueriesMap& killQueries) {
    auto* session = qctx()->rctx()->session();
    auto* sessionMgr = qctx_->rctx()->sessionMgr();
    for (auto& s : killQueries) {
        auto sessionId = s.first;
        if (sessionId == session->id()) {
            session->markQueryKilled(epId);
        } else {
            auto sessionPtr = sessionMgr->findSessionFromCache(sessionId);
            if (sessionPtr != nullptr) {
                sessionPtr->markQueryKilled(epId);
            }
        }
    }
}

Status KillQueryExecutor::verifyTheQueriesByMetaInfo(
    const QueriesMap& toBeVerifiedQueries,
    const std::vector<meta::cpp2::Session>& sessionsInMeta) {
    for (auto& s : toBeVerifiedQueries) {
        auto sessionId = s.first;
        auto found =
            std::find_if(sessionsInMeta.begin(), sessionsInMeta.end(), [sesionId](auto& val) {
                return val.get_session_id() == sessionId;
            });
        if (found == sessionsInMeta.end()) {
            return Status::Error("SessionId[%ld] does not exist", sessionId);
        }
        for (auto& epId : s.second) {
            if (found->get_queries().find(epId) == found->get_queries().end()) {
                return Status::Error(
                    "ExecutionPlanId[%ld] does not exist in Session[%ld].", epId, sessionId);
            }
        }
    }
    return Status::OK();
}
}   // namespace graph
}   // namespace nebula
