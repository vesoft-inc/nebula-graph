/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "executor/admin/SpaceExecutor.h"
#include "context/QueryContext.h"
#include "service/PermissionManager.h"
#include "planner/Admin.h"
#include "util/SchemaUtil.h"
#include "util/ScopedTimer.h"
#include "common/plugin/fulltext/elasticsearch/ESGraphAdapter.h"

DECLARE_uint32(ft_request_retry_times);

namespace nebula {
namespace graph {
folly::Future<Status> CreateSpaceExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *csNode = asNode<CreateSpace>(node());
    meta::MetaClient *client = qctx()->getMetaClient();
    return client->createSpace(csNode->getSpaceDesc(), csNode->getIfNotExists())
            .via(runner())
            .then([this, csNode, client](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                if (csNode->getSpaceDesc().text_search) {
                    auto ret = client->getFTClientsFromCache();
                    if (!ret.ok() || ret.value().empty()) {
                        return Status::Error("text search clients not found");
                    }
                    auto spaceName = csNode->getSpaceDesc().space_name;
                    auto edgeTSIndex = nebula::plugin::IndexTraits::indexName(spaceName, true);
                    if (createTSIndex(ret.value(), edgeTSIndex)) {
                        return Status::Error("external index create failed : %s",
                                             edgeTSIndex.c_str());
                    }

                    auto tagTSIndex = nebula::plugin::IndexTraits::indexName(spaceName, false);
                    if (createTSIndex(ret.value(), tagTSIndex)) {
                        return Status::Error("external index create failed : %s",
                                             tagTSIndex.c_str());
                    }
                }
                return Status::OK();
            });
}

bool CreateSpaceExecutor::createTSIndex(const std::vector<meta::cpp2::FTClient>& clients,
                                        const std::string& index) {
    auto retryCnt = FLAGS_ft_request_retry_times;
    while (--retryCnt > 0) {
        auto i = folly::Random::rand32(clients.size() - 1);
        nebula::plugin::HttpClient ftc;
        ftc.host = clients[i].host;
        if (clients[i].__isset.user) {
            ftc.user = clients[i].user;
        }
        if (clients[i].__isset.pwd) {
            ftc.password = clients[i].pwd;
        }
        auto ret = nebula::plugin::ESGraphAdapter::kAdapter->createIndex(std::move(ftc), index);
        if (ret.ok()) {
            return true;
        }
    }
    return false;
}

folly::Future<Status> DescSpaceExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *dsNode = asNode<DescSpace>(node());
    return qctx()->getMetaClient()->getSpace(dsNode->getSpaceName())
            .via(runner())
            .then([this](StatusOr<meta::cpp2::SpaceItem> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                auto &spaceItem = resp.value();
                auto &properties = spaceItem.get_properties();
                auto spaceId = spaceItem.get_space_id();

                // check permission
                auto *session = qctx_->rctx()->session();
                NG_RETURN_IF_ERROR(PermissionManager::canReadSpace(session, spaceId));

                DataSet dataSet;
                dataSet.colNames = {"ID",
                                    "Name",
                                    "Partition Number",
                                    "Replica Factor",
                                    "Charset",
                                    "Collate",
                                    "Vid Type"};
                Row row;
                row.values.emplace_back(spaceId);
                row.values.emplace_back(properties.get_space_name());
                row.values.emplace_back(properties.get_partition_num());
                row.values.emplace_back(properties.get_replica_factor());
                row.values.emplace_back(properties.get_charset_name());
                row.values.emplace_back(properties.get_collate_name());
                row.values.emplace_back(SchemaUtil::typeToString(properties.get_vid_type()));
                dataSet.rows.emplace_back(std::move(row));
                return finish(ResultBuilder()
                                  .value(Value(std::move(dataSet)))
                                  .iter(Iterator::Kind::kDefault)
                                  .finish());
            });
}

folly::Future<Status> DropSpaceExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *dsNode = asNode<DropSpace>(node());
    return qctx()->getMetaClient()->dropSpace(dsNode->getSpaceName(), dsNode->getIfExists())
            .via(runner())
            .then([this, dsNode](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "Drop space `" << dsNode->getSpaceName()
                               << "' failed: " << resp.status();
                    return resp.status();
                }
                if (dsNode->getSpaceName() == qctx()->rctx()->session()->space().name) {
                    SpaceInfo spaceInfo;
                    spaceInfo.name = "";
                    spaceInfo.id = -1;
                    qctx()->rctx()->session()->setSpace(std::move(spaceInfo));
                }
                return Status::OK();
            });
}


folly::Future<Status> ShowSpacesExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    return qctx()->getMetaClient()->listSpaces().via(runner()).then(
        [this](StatusOr<std::vector<meta::SpaceIdName>> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << "Show spaces failed: " << resp.status();
                return resp.status();
            }
            auto spaceItems = std::move(resp).value();

            DataSet dataSet({"Name"});
            std::set<std::string> orderSpaceNames;
            for (auto &space : spaceItems) {
                if (!PermissionManager::canReadSpace(qctx_->rctx()->session(),
                                                     space.first).ok()) {
                    continue;
                }
                orderSpaceNames.emplace(space.second);
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

folly::Future<Status> ShowCreateSpaceExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *scsNode = asNode<ShowCreateSpace>(node());
    return qctx()->getMetaClient()->getSpace(scsNode->getSpaceName())
            .via(runner())
            .then([this, scsNode](StatusOr<meta::cpp2::SpaceItem> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "Show create space `" << scsNode->getSpaceName()
                               << "' failed: " << resp.status();
                    return resp.status();
                }
                auto properties = resp.value().get_properties();
                DataSet dataSet({"Space", "Create Space"});
                Row row;
                row.values.emplace_back(properties.get_space_name());
                auto fmt = "CREATE SPACE `%s` (partition_num = %d, replica_factor = %d, "
                           "charset = %s, collate = %s, vid_type = %s)";
                row.values.emplace_back(folly::stringPrintf(
                    fmt,
                    properties.get_space_name().c_str(),
                    properties.get_partition_num(),
                    properties.get_replica_factor(),
                    properties.get_charset_name().c_str(),
                    properties.get_collate_name().c_str(),
                    SchemaUtil::typeToString(properties.get_vid_type()).c_str()));
                dataSet.rows.emplace_back(std::move(row));
                return finish(ResultBuilder()
                                  .value(Value(std::move(dataSet)))
                                  .iter(Iterator::Kind::kDefault)
                                  .finish());
            });
}
}   // namespace graph
}   // namespace nebula
