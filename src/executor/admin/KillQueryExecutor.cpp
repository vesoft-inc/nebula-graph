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
            auto cb = [sessionId, epId] (StatusOr<std::shared_ptr<ClientSession>> ret) {
                if (!ret.ok()) {
                    LOG(ERROR) << "Get session for sessionId: " << sessionId
                                << " failed: " << ret.status();
                    return Status::Error("SessionId[%ld] does not exist", sessionId);
                }
                auto sessionPtr = std::move(ret).value();
                if (sessionPtr == nullptr) {
                    LOG(ERROR) << "Get session for sessionId: " << sessionId << " is nullptr";
                    return Status::Error("SessionId[%ld] does not exist", sessionId);
                }

                if (!sessionPtr->findQuery(epId)) {
                    return Status::Error("ExecutionPlanId[%ld] does not exist.", epId);
                }
                return Status::OK();
            };
            auto findSessionStatus = sessionMgr->findSession(sessionId, qctx_->rctx()->runner())
                .thenValue(cb).get();
            NG_RETURN_IF_ERROR(findSessionStatus);
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
