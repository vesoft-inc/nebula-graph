/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "common/conf/Configuration.h"
#include "executor/admin/ConfigExecutor.h"
#include "planner/Admin.h"
#include "util/SchemaUtil.h"
#include "context/QueryContext.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

std::vector<Value> ConfigBaseExecutor::generateColumns(const meta::cpp2::ConfigItem& item) {
    std::vector<Value> columns;
    columns.resize(5);
    auto value = item.get_value();
    columns[0].setStr(meta::cpp2::_ConfigModule_VALUES_TO_NAMES.at(item.get_module()));
    columns[1].setStr(item.get_name());
    columns[2].setStr(value.typeName());
    columns[3].setStr(meta::cpp2::_ConfigMode_VALUES_TO_NAMES.at(item.get_mode()));
    columns[4] = std::move(value);
    return columns;
}

DataSet ConfigBaseExecutor::generateResult(const std::vector<meta::cpp2::ConfigItem> &items) {
    DataSet result;
    result.colNames = {"module", "name", "type", "mode", "value"};
    for (const auto &item : items) {
        auto columns = generateColumns(item);
        result.rows.emplace_back(std::move(columns));
    }
    return result;
}

folly::Future<GraphStatus> ShowConfigsExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *scNode = asNode<ShowConfigs>(node());
    return qctx()->getMetaClient()->listConfigs(scNode->getModule())
            .via(runner())
            .then([this, scNode](StatusOr<meta::cpp2::ListConfigsResp> resp) {
                auto gStatus = checkMetaResp(resp);
                if (!gStatus.ok()) {
                    return gStatus;
                }

                auto result = generateResult(resp.value().get_items());
                return finish(ResultBuilder().value(Value(std::move(result))).finish());
            });
}

folly::Future<GraphStatus> SetConfigExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *scNode = asNode<SetConfig>(node());
    return qctx()->getMetaClient()->setConfig(scNode->getModule(),
                                              scNode->getName(),
                                              scNode->getValue())
            .via(runner())
            .then([this, scNode](StatusOr<meta::cpp2::ExecResp> resp) {
                auto gStatus = checkMetaResp(resp);
                if (!gStatus.ok()) {
                    return gStatus;
                }
                return GraphStatus::OK();
            });
}

folly::Future<GraphStatus> GetConfigExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *gcNode = asNode<GetConfig>(node());
    return qctx()->getMetaClient()->getConfig(gcNode->getModule(),
                                              gcNode->getName())
            .via(runner())
            .then([this, gcNode](StatusOr<meta::cpp2::GetConfigResp> resp) {
                auto gStatus = checkMetaResp(resp);
                if (!gStatus.ok()) {
                    return gStatus;
                }
                auto result = generateResult(resp.value().get_items());
                return finish(ResultBuilder().value(Value(std::move(result))).finish());
            });
}

}   // namespace graph
}   // namespace nebula
