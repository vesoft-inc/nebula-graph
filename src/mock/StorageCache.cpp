/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "mock/StorageCache.h"
#include <gtest/gtest-printers.h>
#include "common/expression/Expression.h"
#include "common/expression/SymbolPropertyExpression.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace graph {

StorageCache::StorageCache(uint16_t metaPort) {
    FLAGS_heartbeat_interval_secs = 1;
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    auto hostStatus = network::NetworkUtils::resolveHost("127.0.0.1", metaPort);
    meta::MetaClientOptions options;
    options.serviceName_ = "StorageCache";
    metaClient_ = std::make_unique<meta::MetaClient>(threadPool,
            std::move(hostStatus).value(), options);
    metaClient_->waitForMetadReady();

    mgr_ = meta::SchemaManager::create(metaClient_.get());
}

Status StorageCache::addVertices(const storage::cpp2::AddVerticesRequest& req) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    auto spaceId = req.get_space_id();
    auto spaceFind = cache_.find(spaceId);
    SpaceDataInfo *spaceDataInfo = nullptr;
    if (spaceFind != cache_.end()) {
        spaceDataInfo = &spaceFind->second;
    } else {
        cache_[spaceId] = SpaceDataInfo();
        spaceDataInfo = &cache_[spaceId];
    }

    std::unordered_map<TagID, std::vector<std::string>> propNames = req.get_prop_names();
    auto &parts = req.get_parts();
    for (auto &part : parts) {
        for (auto &vertex : part.second) {
            auto vId = vertex.get_id();
            auto findV = spaceDataInfo->vertices.find(vId);
            std::unordered_map<TagID, PropertyInfo> *vertexInfo = nullptr;
            if (findV != spaceDataInfo->vertices.end()) {
                vertexInfo = &findV->second;
            } else {
                spaceDataInfo->vertices[vId] = {};
                vertexInfo = &spaceDataInfo->vertices[vId];
            }

            for (auto &tag : vertex.get_tags()) {
                auto tagId = tag.get_tag_id();
                auto propValues = tag.get_props();
                if (propValues.size() != propNames[tagId].size()) {
                    return Status::Error("Wrong size");
                }

                std::unordered_map<std::string, int32_t> propIndexes;
                for (auto i = 0u; i < propValues.size(); i++) {
                    propIndexes[propNames[tagId][i]] = i;
                }

                (*vertexInfo)[tagId].propNames = propNames[tagId];
                (*vertexInfo)[tagId].propValues = std::move(propValues);
                (*vertexInfo)[tagId].propIndexes = std::move(propIndexes);
            }
        }
    }
    return Status::OK();
}

Status StorageCache::addEdges(const storage::cpp2::AddEdgesRequest& req) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    auto spaceId = req.get_space_id();
    auto spaceFind = cache_.find(spaceId);
    SpaceDataInfo *spaceDataInfo = nullptr;
    if (spaceFind != cache_.end()) {
        spaceDataInfo = &spaceFind->second;
    } else {
        cache_[spaceId] = SpaceDataInfo();
        spaceDataInfo = &cache_[spaceId];
    }

    std::vector<std::string> propNames = req.get_prop_names();
    auto &parts = req.get_parts();
    for (auto &part : parts) {
        for (auto &edge : part.second) {
            auto key = edge.get_key();
            auto edgeKey = getEdgeKey(key.get_src(),
                    key.get_edge_type(), key.get_ranking(), key.get_dst());
            PropertyInfo propertyInfo;
            propertyInfo.propNames = propNames;
            for (auto i = 0u; i < propertyInfo.propNames.size(); i++) {
                propertyInfo.propIndexes[propertyInfo.propNames[i]] = i;
            }
            propertyInfo.propValues = edge.get_props();
            if (propertyInfo.propValues.size() != propertyInfo.propNames.size()) {
                LOG(ERROR) << "Wrong size, propValues.size : " << propertyInfo.propValues.size()
                           << ", propNames.size : " << propertyInfo.propNames.size();
                return Status::Error("Wrong size");
            }
            spaceDataInfo->edges[edgeKey] = std::move(propertyInfo);
        }
    }
    return Status::OK();
}

}  // namespace graph
}  // namespace nebula
