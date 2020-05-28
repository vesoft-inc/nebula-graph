/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/admin/ListUsersExecutor.h"
#include "planner/Admin.h"
#include "service/ExecutionContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> ListUsersExecutor::execute() {
    return listUsers().ensure([this]() { UNUSED(this); });
}

folly::Future<Status> ListUsersExecutor::listUsers() {
    dumpLog();

    return ectx()->getMetaClient()->listUsers()
        .via(runner())
        .then([](StatusOr<std::unordered_map<std::string, std::string>> &&resp) {
            if (!resp.ok()) {
                return std::move(resp).status();
            }
            nebula::DataSet v({"Account"});
            auto items = std::move(resp).value();
            for (const auto &item : items) {
                v.emplace_back(nebula::Row(
                    {
                        std::move(item).first,
                    }));
            }
            return Status::OK();
        });
}

}   // namespace graph
}   // namespace nebula
