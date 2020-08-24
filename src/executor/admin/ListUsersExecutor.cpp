/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/admin/ListUsersExecutor.h"
#include "planner/Admin.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

folly::Future<GraphStatus> ListUsersExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return listUsers();
}

folly::Future<GraphStatus> ListUsersExecutor::listUsers() {
    return qctx()->getMetaClient()->listUsers()
        .via(runner())
        .then([this](StatusOr<meta::cpp2::ListUsersResp> &&resp) {
            SCOPED_TIMER(&execTime_);
            auto gStatus = checkMetaResp(resp);
            if (!gStatus.ok()) {
                return gStatus;
            }
            nebula::DataSet v({"Account"});
            auto items = resp.value().get_users();
            for (const auto &item : items) {
                v.emplace_back(nebula::Row(
                    {
                        std::move(item).first,
                    }));
            }
            return finish(std::move(v));
        });
}

}   // namespace graph
}   // namespace nebula
