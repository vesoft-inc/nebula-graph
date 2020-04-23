/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "StorageCache.h"

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
                std::vector<std::string> propNames;
                if (!tag.__isset.names) {
                    LOG(INFO) << "Get prop name from cache";
                    propNames = getTagPropNamesFromCache(spaceId, tagId);
                } else {
                    LOG(INFO) << "Get prop name from req";
                    propNames = *tag.get_names();
                }

                std::unordered_map<std::string, int32_t> propIndexes;
                for (auto i = 0u; i < propNames.size(); i++) {
                    propIndexes[propNames[i]] = i;
                }

                auto propValues = tag.get_props().get_props();

                if (propValues.size() != propNames.size()) {
                    LOG(ERROR) << "Wrong size, propValues.size : " << propValues.size()
                               << ", propNames.size : " << propNames.size();
                    return Status::Error("Wrong size");
                }
                (*vertexInfo)[tagId].propNames = std::move(propNames);
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

    auto &parts = req.get_parts();
    for (auto &part : parts) {
        for (auto &edge : part.second) {
            auto key = edge.get_key();
            auto edgeKey = getEdgeKey(key.get_src(),
                    key.get_edge_type(), key.get_ranking(), key.get_dst());
            PropertyInfo propertyInfo;
            if (!edge.__isset.names) {
                propertyInfo.propNames = getEdgePropNamesFromCache(spaceId, key.get_edge_type());
            } else {
                propertyInfo.propNames = *edge.get_names();
            }
            for (auto i = 0u; i < propertyInfo.propNames.size(); i++) {
                propertyInfo.propIndexes[propertyInfo.propNames[i]] = i;
            }
            propertyInfo.propValues = edge.get_props().get_props();
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

StatusOr<std::vector<storage::cpp2::VertexPropData>>
StorageCache::getVertexProps(const storage::cpp2::VertexPropRequest&) {
    return {};
#if 0
    folly::RWSpinLock::ReadHolder holder(lock_);
    std::vector<storage::cpp2::VertexPropData> data;
    auto spaceId = req.get_space_id();
    auto findIt = cache_.find(spaceId);
    if (findIt == cache_.end()) {
        return Status::Error("SpaceID `%d' not found", spaceId);
    }
    auto vertices = findIt->second.vertices;
    auto parts = req.get_parts();
    std::vector<storage::cpp2::VertexProp> props;
    if (req.__isset.vertex_props) {
        props = req.get_vertex_props();
    }

    for (auto &part : parts) {
        for (auto &vId : part.second) {
            auto vFindIt = vertices.find(vId);
            if (vFindIt == vertices.end()) {
                return Status::Error("VertexId `%s' not found", vId.c_str());
            }
            storage::cpp2::VertexPropData vertex;
            vertex.set_id(vId);
            vertex.set_props(vFindIt.second);
            vertex.set_names();
        }
    }
#endif
}

std::vector<std::string> StorageCache::getTagPropNamesFromCache(const GraphSpaceID spaceId,
                                                                const TagID tagId) {
    auto verRet = metaClient_->getLatestTagVersionFromCache(spaceId, tagId);
    if (!verRet.ok()) {
        LOG(ERROR) << "Get version failed: spaceId: " << spaceId << ", tagId: " << tagId;
        return {};
    }
    auto version = verRet.value();
    std::vector<std::string> names;
    auto status = metaClient_->getTagSchemaFromCache(spaceId, tagId, version);
    if (!status.ok()) {
        LOG(ERROR) << status.status();
        return {};
    }
    auto schema = status.value();
    if (schema == nullptr) {
        return {};
    }
    for (auto i = 0u; i < schema->getNumFields(); i++) {
        names.emplace_back(schema->getFieldName(i));
        LOG(INFO) << "Prop name: " << schema->getFieldName(i);
    }
    return names;
}

std::vector<std::string> StorageCache::getEdgePropNamesFromCache(const GraphSpaceID spaceId,
                                                                 const EdgeType edgeType) {
    auto verRet = metaClient_->getLatestEdgeVersionFromCache(spaceId, edgeType);
    if (!verRet.ok()) {
        LOG(ERROR) << "Get version failed: spaceId: " << spaceId << ", edgeType: " << edgeType;
        return {};
    }
    auto version = verRet.value();
    std::vector<std::string> names;
    auto status = metaClient_->getEdgeSchemaFromCache(spaceId, edgeType, version);
    if (!status.ok()) {
        LOG(ERROR) << status.status();
        return {};
    }
    auto schema = status.value();
    if (schema == nullptr) {
        return {};
    }
    for (auto i = 0u; i < schema->getNumFields(); i++) {
        names.emplace_back(schema->getFieldName(i));
    }
    return names;
}
}  // namespace graph
}  // namespace nebula
