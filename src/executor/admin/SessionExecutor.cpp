/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/interface/gen-cpp2/common_types.h"
#include "common/interface/gen-cpp2/meta_types.h"

#include "executor/admin/SessionExecutor.h"
#include "planner/Admin.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> ShowSessionsExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return qctx()->getMetaClient()->listSessions()
            .via(runner())
            .then([this](StatusOr<meta::cpp2::ListSessionsResp> resp) {
                SCOPED_TIMER(&execTime_);
                if (!resp.ok()) {
                    return Status::Error("Show sessions failed: %s.",
                                          resp.status().toString().c_str());
                }
                auto sessions = resp.value().get_sessions();
                DataSet result({"SessionId",
                                "UserName",
                                "SpaceName",
                                "CreateTime",
                                "UpdateTime",
                                "GraphAddr",
                                "Timezone",
                                "ClientIp"});
                for (auto &session : sessions) {
                    Row row;
                    row.emplace_back(session.session_id);
                    row.emplace_back(session.user_name);
                    row.emplace_back(session.space_name);
                    // TODO(laura) format time to local time
                    row.emplace_back(session.create_time);
                    row.emplace_back(session.update_time);
                    row.emplace_back(network::NetworkUtils::toHostsStr({session.graph_addr}));
                    row.emplace_back(session.timezone);
                    row.emplace_back(session.client_ip);
                    result.emplace_back(std::move(row));
                }
                return finish(ResultBuilder().value(Value(std::move(result))).finish());
            });
}

folly::Future<Status> GetSessionExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto *getNode = asNode<GetSession>(node());
    return qctx()->getMetaClient()->getSession(getNode->getSessionId())
            .via(runner())
            .then([this, getNode](StatusOr<meta::cpp2::GetSessionResp> resp) {
                SCOPED_TIMER(&execTime_);
                if (!resp.ok()) {
                    return Status::Error("Get session `%ld' failed: %s.",
                                          getNode->getSessionId(),
                                          resp.status().toString().c_str());
                }
                auto session = resp.value().get_session();
                DataSet result({"VariableName", "Value"});
                result.emplace_back(Row({"SessionID", session.session_id}));
                result.emplace_back(Row({"UserName", session.user_name}));
                result.emplace_back(Row({"SpaceName", session.space_name}));
                result.emplace_back(Row({"CreateTime", session.create_time}));
                result.emplace_back(Row({"UpdateTime", session.update_time}));
                result.emplace_back(Row({"GraphAddr",
                                         network::NetworkUtils::toHostsStr({session.graph_addr})}));
                result.emplace_back(Row({"Timezone", session.timezone}));
                result.emplace_back(Row({"ClientIp", session.client_ip}));
                return finish(ResultBuilder().value(Value(std::move(result))).finish());
            });
}

folly::Future<Status> UpdateSessionExecutor::execute() {
    VLOG(1) << "Update sessions to metad";
    SCOPED_TIMER(&execTime_);
    auto *updateNode = asNode<UpdateSession>(node());
    std::vector<meta::cpp2::Session> sessions;
    sessions.emplace_back(updateNode->getSession());
    return qctx()->getMetaClient()->updateSessions(sessions)
            .via(runner())
            .then([this, updateNode](auto&& resp) {
                SCOPED_TIMER(&execTime_);
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}

}   // namespace graph
}   // namespace nebula
