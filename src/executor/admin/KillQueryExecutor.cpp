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
    auto sessionId = killQuery->sessionId();
    auto epId = killQuery->epId();

    // TODO: permision check

    auto* session = qctx()->rctx()->session();
    if (sessionId == session->id()) {
        session->markQueryKilled(epId);
    }

    return qctx()
        ->getMetaClient()
        ->killQuery(sessionId, epId)
        .via(runner())
        .thenValue([sessionId, epId, this](auto&& resp) {
            SCOPED_TIMER(&execTime_);
            if (!resp.ok()) {
                return Status::Error("Kill query (session=%ld, plan=%ld) failed.", sessionId, epId);
            }
            return Status::OK();
        });
}
}  // namespace graph
}  // namespace nebula
