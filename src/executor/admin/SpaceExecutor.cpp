/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "executor/admin/SpaceExecutor.h"
#include "planner/Admin.h"
#include "context/QueryContext.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {
folly::Future<GraphStatus> CreateSpaceExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *csNode = asNode<CreateSpace>(node());
    return qctx()->getMetaClient()->createSpace(csNode->getSpaceDesc(), csNode->getIfNotExists())
            .via(runner())
            .then([this](auto&& resp) {
                auto gStatus = checkMetaResp(resp);
                if (!gStatus.ok()) {
                    return gStatus;
                }
                return GraphStatus::OK();
            });
}


folly::Future<GraphStatus> DescSpaceExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *dsNode = asNode<DescSpace>(node());
    return qctx()->getMetaClient()->getSpace(dsNode->getSpaceName())
            .via(runner())
            .then([this](auto&& resp) {
                auto gStatus = checkMetaResp(resp);
                if (!gStatus.ok()) {
                    return gStatus;
                }
                DataSet dataSet;
                dataSet.colNames = {"ID",
                                    "Name",
                                    "Partition Number",
                                    "Replica Factor",
                                    "Vid Size",
                                    "Charset",
                                    "Collate"};
                auto &spaceItem = resp.value().get_item();
                auto &properties = spaceItem.get_properties();
                Row row;
                row.values.emplace_back(spaceItem.get_space_id());
                row.values.emplace_back(properties.get_space_name());
                row.values.emplace_back(properties.get_partition_num());
                row.values.emplace_back(properties.get_replica_factor());
                row.values.emplace_back(properties.get_vid_size());
                row.values.emplace_back(properties.get_charset_name());
                row.values.emplace_back(properties.get_collate_name());
                dataSet.rows.emplace_back(std::move(row));
                return finish(ResultBuilder()
                                  .value(Value(std::move(dataSet)))
                                  .iter(Iterator::Kind::kDefault)
                                  .finish());
            });
}

folly::Future<GraphStatus> DropSpaceExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *dsNode = asNode<DropSpace>(node());
    return qctx()->getMetaClient()->dropSpace(dsNode->getSpaceName(), dsNode->getIfExists())
            .via(runner())
            .then([this, dsNode](auto&& resp) {
                auto gStatus = checkMetaResp(resp);
                if (!gStatus.ok()) {
                    return gStatus;
                }
                if (dsNode->getSpaceName() == qctx()->rctx()->session()->spaceName()) {
                    qctx()->rctx()->session()->setSpace("", -1);
                }
                return GraphStatus::OK();
            });
}


folly::Future<GraphStatus> ShowSpacesExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    return qctx()->getMetaClient()->listSpaces().via(runner()).then(
        [this](auto&& resp) {
            auto gStatus = checkMetaResp(resp);
            if (!gStatus.ok()) {
                return gStatus;
            }
            auto spaceItems = resp.value().get_spaces();

            DataSet dataSet({"Name"});
            std::set<std::string> orderSpaceNames;
            for (auto &space : spaceItems) {
                orderSpaceNames.emplace(space.name);
            }
            for (auto &name : orderSpaceNames) {
                Row row;
                row.values.emplace_back(name);
                dataSet.rows.emplace_back(std::move(row));
            }
            return finish(ResultBuilder()
                              .value(Value(std::move(dataSet)))
                              .iter(Iterator::Kind::kDefault)
                              .finish());
        });
}

folly::Future<GraphStatus> ShowCreateSpaceExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *scsNode = asNode<ShowCreateSpace>(node());
    return qctx()->getMetaClient()->getSpace(scsNode->getSpaceName())
            .via(runner())
            .then([this, scsNode](auto&& resp) {
                auto gStatus = checkMetaResp(resp);
                if (!gStatus.ok()) {
                    return gStatus;
                }
                auto properties = resp.value().get_item().get_properties();
                DataSet dataSet({"Space", "Create Space"});
                Row row;
                row.values.emplace_back(properties.get_space_name());
                auto fmt = "CREATE SPACE `%s` (partition_num = %d, replica_factor = %d, "
                           "vid_size = %d, charset = %s, collate = %s)";
                row.values.emplace_back(
                        folly::stringPrintf(fmt,
                                            properties.get_space_name().c_str(),
                                            properties.get_partition_num(),
                                            properties.get_replica_factor(),
                                            properties.get_vid_size(),
                                            properties.get_charset_name().c_str(),
                                            properties.get_collate_name().c_str()));
                dataSet.rows.emplace_back(std::move(row));
                return finish(ResultBuilder()
                                  .value(Value(std::move(dataSet)))
                                  .iter(Iterator::Kind::kDefault)
                                  .finish());
            });
}
}   // namespace graph
}   // namespace nebula
