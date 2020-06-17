/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "common/conf/Configuration.h"
#include "exec/admin/ConfigExecutor.h"
#include "planner/Admin.h"
#include "util/SchemaUtil.h"
#include "context/QueryContext.h"

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

folly::Future<Status> ShowConfigsExecutor::execute() {
    dumpLog();

    auto *scNode = asNode<ShowConfigs>(node());
    return qctx()->getMetaClient()->listConfigs(scNode->getModule())
            .via(runner())
            .then([this](StatusOr<std::vector<meta::cpp2::ConfigItem>> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }

                auto result = generateResult(resp.value());
                finish(ResultBuilder().value(Value(std::move(result))).finish());
                return Status::OK();
            });
}

folly::Future<Status> SetConfigExecutor::execute() {
    dumpLog();

    auto *scNode = asNode<SetConfig>(node());
    return qctx()->getMetaClient()->setConfig(scNode->getModule(),
                                              scNode->getName(),
                                              scNode->getValue())
            .via(runner())
            .then([](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}

folly::Future<Status> GetConfigExecutor::execute() {
    dumpLog();

    auto *gcNode = asNode<GetConfig>(node());
    return qctx()->getMetaClient()->getConfig(gcNode->getModule(),
                                              gcNode->getName())
            .via(runner())
            .then([this](StatusOr<std::vector<meta::cpp2::ConfigItem>> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                auto result = generateResult(resp.value());
                finish(ResultBuilder().value(Value(std::move(result))).finish());
                return Status::OK();
            });
}

}   // namespace graph
}   // namespace nebula
