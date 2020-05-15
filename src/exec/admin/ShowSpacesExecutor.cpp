/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "exec/admin/ShowSpacesExecutor.h"
#include "planner/Admin.h"
#include "service/ExecutionContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> ShowSpacesExecutor::execute() {
    dumpLog();

    return ectx()->getMetaClient()->listSpaces()
            .via(runner())
            .then([this](StatusOr<std::vector<meta::SpaceIdName>> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                auto spaceItems = std::move(resp).value();

                DataSet dataSet;
                dataSet.colNames = {"Name"};
                std::vector<Row> rows;
                std::set<std::string> orderSpaceNames;
                for (auto &space : spaceItems) {
                    orderSpaceNames.emplace(space.second);
                }
                for (auto &name : orderSpaceNames) {
                    std::vector<Value> columns;
                    columns.emplace_back(name);
                    Row row;
                    row.columns = std::move(columns);
                    rows.emplace_back(row);
                }
                dataSet.rows = std::move(rows);
                finish(dataSet);
                return Status::OK();
            });
}

}   // namespace graph
}   // namespace nebula
