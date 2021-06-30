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

    folly::Promise<Status> pro;
    auto result = pro.getFuture();
    qctx()->getMetaClient()->listSessions().via(runner()).thenValue(
        [pro = std::move(pro), this](StatusOr<meta::cpp2::ListSessionsResp> listResp) mutable {
            std::vector<meta::cpp2::Session> sessionsInMeta;
            if (listResp.ok()) {
                sessionsInMeta = std::move(listResp.value()).get_sessions();
            }
            KillQueriesMap killQueries;
            auto retStatus = buildKillQueries(sessionsInMeta, killQueries);
            if (!retStatus.ok()) {
                pro.setValue(retStatus);
                return;
            }

            qctx()
                ->getMetaClient()
                ->killQuery(std::move(killQueries))
                .via(runner())
                .thenValue([pro = std::move(pro), this](auto&& resp) mutable {
                    SCOPED_TIMER(&execTime_);
                    Status status = Status::OK();
                    if (!resp.ok()) {
                        status = Status::Error("Kill query failed.");
                    }
                    pro.setValue(std::move(status));
                });
        });
    return result;
}

Status KillQueryExecutor::buildKillQueries(const std::vector<meta::cpp2::Session>& sessionsInMeta,
                                           KillQueriesMap& killQueries) {
    SCOPED_TIMER(&execTime_);
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
    return Status::OK();
}
}   // namespace graph
}   // namespace nebula
