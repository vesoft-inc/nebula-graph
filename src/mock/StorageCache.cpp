/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "mock/StorageCache.h"

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
    mgr_ = std::make_unique<meta::ServerBasedSchemaManager>();
    mgr_->init(metaClient_.get());
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

    std::unordered_map<TagID, std::vector<std::string>> propNames;
    for (auto &prop : req.get_prop_names()) {
        propNames[prop.first] = prop.second;
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
<<<<<<< HEAD
            storage::cpp2::EdgeKey edgeKey;
            edgeKey.set_src(edge.key.get_src());
            auto edgeType = edge.key.get_edge_type();
            edgeKey.set_edge_type(edgeType);
            edgeKey.set_ranking(edge.key.get_ranking());
            edgeKey.set_dst(edge.key.get_dst());
=======
            auto key = edge.get_key();
<<<<<<< HEAD
            storage::cpp2::EdgeKey edgeKey;
            edgeKey.set_src(key.get_src());
            edgeKey.set_edge_type(key.get_edge_type());
            edgeKey.set_ranking(key.get_ranking());
            edgeKey.set_dst(key.get_dst());
>>>>>>> add update executor and test
            auto propValues = edge.get_props();
            if (propValues.size() != propNames.size()) {
                LOG(ERROR) << "Wrong size, propValues.size : " << propValues.size()
                           << ", propNames.size : " << propNames.size();
                return Status::Error("Wrong size");
            }
<<<<<<< HEAD
            auto ret = getEdgeWholeValue(spaceId, edgeType, propValues, propNames);
=======
            auto ret = getEdgeWholeValue(spaceId, key.get_edge_type(), propValues, propNames);
>>>>>>> add update executor and test
            if (!ret.ok()) {
                LOG(ERROR) << ret.status();
                return ret.status();
=======
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
>>>>>>> rebase upstream
            }
            spaceDataInfo->edges[edgeKey] = std::move(propertyInfo);
        }
    }
    return Status::OK();
}

<<<<<<< HEAD
StatusOr<std::unordered_map<std::string, Value>>
StorageCache::getTagWholeValue(const GraphSpaceID spaceId,
                               const TagID tagId,
                               const std::vector<Value>& props,
                               const std::vector<std::string> &names) {
    if (props.size() != names.size()) {
        return Status::Error("Wrong size between props and names");
<<<<<<< HEAD
    }
    auto schema = mgr_->getTagSchema(spaceId, tagId);
    if (schema == nullptr) {
        return Status::Error("TagId `%d' not exist", tagId);
=======
    }
    auto schema = mgr_->getTagSchema(spaceId, tagId);
    if (schema == nullptr) {
        return Status::Error("TagID `%d' not exist", tagId);
    }
    return getPropertyInfo(schema, props, names);
}

StatusOr<std::unordered_map<std::string, Value>>
StorageCache::getEdgeWholeValue(const GraphSpaceID spaceId,
                                const EdgeType edgeType,
                                const std::vector<Value>& props,
                                const std::vector<std::string> &names) {
    if (props.size() != names.size()) {
        return Status::Error("Wrong size between props and names");
    }
    auto schema = mgr_->getEdgeSchema(spaceId, std::abs(edgeType));
    if (schema == nullptr) {
        return Status::Error("edgeType `%d' not exist", edgeType);
    }
    return getPropertyInfo(schema, props, names);
}

StatusOr<std::unordered_map<std::string, Value>>
StorageCache::getPropertyInfo(std::shared_ptr<const meta::NebulaSchemaProvider> schema,
                              const std::vector<Value>& props,
                              const std::vector<std::string> &names) {
    auto number = schema->getNumFields();
    if (number == 0 && props.size() == 0) {
        return {};
    }
    if (number == 0 && props.size() != 0) {
        return Status::Error("Wrong value about empty schema");
    }

    std::bitset<32> indexBitSet;
    std::unordered_map<std::string, Value> propertyInfo;
    auto index = 0u;
    for (auto &item : names) {
        auto filed = schema->field(item);
        if (filed != nullptr) {
            indexBitSet.set(index);
            if (!filed->nullable() && props[index].isNull()) {
                return Status::Error("Wrong null type `%s'", item.c_str());
            }
            propertyInfo.emplace(item, props[index]);
        } else {
            return Status::Error("Wrong prop name `%s'", item.c_str());
        }
        index++;
    }

    if (schema->getNumFields() != indexBitSet.count()) {
        for (auto i = 0u; i < schema->getNumFields(); i++) {
            if (indexBitSet[i] != 1) {
                auto field = schema->field(i);
                if (field != nullptr && !field->hasDefault()) {
                    return Status::Error("Prop name `%s' without default value",
                                          field->name());
                }
                VLOG(1) << "Add default value, filed name: " << field->name();
                propertyInfo.emplace(field->name(), field->defaultValue());
            }
        }
    }

    return propertyInfo;
}

StatusOr<DataSet> StorageCache::updateVertex(const storage::cpp2::UpdateVertexRequest &req) {
=======
StatusOr<std::vector<DataSet>>
StorageCache::getProps(const storage::cpp2::GetPropRequest&) {
    return {};
#if 0
    folly::RWSpinLock::ReadHolder holder(lock_);
    std::vector<storage::cpp2::VertexPropData> data;
>>>>>>> rebase upstream
    auto spaceId = req.get_space_id();
    auto findIt = cache_.find(spaceId);
    if (findIt == cache_.end()) {
        return Status::Error("SpaceID `%d' not found", spaceId);
    }
<<<<<<< HEAD
    auto &verticesInfo = findIt->second;
    auto findVertex = verticesInfo.vertices.find(vertex);

    if (findVertex == verticesInfo.vertices.end() && !insertable) {
        return Status::Error("Vertex `%s' not found", vertex.c_str());
>>>>>>> add update executor and test
=======
    auto vertices = findIt->second.vertices;
    auto parts = req.get_parts();
    std::vector<storage::cpp2::VertexProp> props;
    if (req.__isset.vertex_props) {
        props = req.get_vertex_props();
>>>>>>> rebase upstream
    }
    return getPropertyInfo(schema, props, names);
}

<<<<<<< HEAD
<<<<<<< HEAD
StatusOr<std::unordered_map<std::string, Value>>
StorageCache::getEdgeWholeValue(const GraphSpaceID spaceId,
                                const EdgeType edgeType,
                                const std::vector<Value>& props,
                                const std::vector<std::string> &names) {
    if (props.size() != names.size()) {
        return Status::Error("Wrong size between props and names");
    }
    auto schema = mgr_->getEdgeSchema(spaceId, std::abs(edgeType));
    if (schema == nullptr) {
        return Status::Error("EdgeType `%d' not exist", edgeType);
    }
    return getPropertyInfo(schema, props, names);
}

StatusOr<std::unordered_map<std::string, Value>>
StorageCache::getPropertyInfo(std::shared_ptr<const meta::NebulaSchemaProvider> schema,
                              const std::vector<Value>& props,
                              const std::vector<std::string> &names) {
    auto number = schema->getNumFields();
    if (number == 0 && props.size() == 0) {
        return {};
    }
    if (number == 0 && props.size() != 0) {
        return Status::Error("Wrong value about empty schema");
    }

    std::bitset<32> indexBitSet;
    std::unordered_map<std::string, Value> propertyInfo;
    auto index = 0u;
    for (auto &item : names) {
        auto filed = schema->field(item);
        if (filed != nullptr) {
            indexBitSet.set(index);
            if (!filed->nullable() && props[index].isNull()) {
                return Status::Error("Wrong null type `%s'", item.c_str());
            }
            propertyInfo.emplace(item, props[index]);
        } else {
            return Status::Error("Wrong prop name `%s'", item.c_str());
        }
        index++;
    }

    if (schema->getNumFields() != indexBitSet.count()) {
        for (auto i = 0u; i < schema->getNumFields(); i++) {
            if (indexBitSet[i] != 1) {
                auto field = schema->field(i);
                if (field != nullptr && !field->hasDefault()) {
                    return Status::Error("Prop name `%s' without default value",
                                         field->name());
                }
                VLOG(1) << "Add default value, filed name: " << field->name();
                propertyInfo.emplace(field->name(), field->defaultValue());
            }
        }
    }

    return propertyInfo;
=======
    DataSet dataSet;
    // update
    if (findVertex != verticesInfo.vertices.end()) {
        // filter
        if (!condition.empty()) {
            return dataSet;
        }
        auto vertexInfo = findVertex->second;
        auto tagFind = vertexInfo.find(tagId);
        if (tagFind == vertexInfo.end()) {
            return Status::Error("Tag `%d' not found", tagId);
        }
        auto props = tagFind->second;
        for (auto &updatedProp : updatedProps) {
            auto prop = updatedProp.get_name();
            auto value = updatedProp.get_value();
            auto propFind = props.find(prop);
            if (propFind == props.end()) {
                return Status::Error("Prop `%s' not found", prop.c_str());
            }

            // TODO(Laura): get value expr value
            // propFind.second = value;
        }
    }

    // insert
    if (insertable) {
        struct TagPropValue {
            std::vector<std::string> names;
            std::vector<Value> values;
        };
        TagPropValue tagInfo;
        for (auto &updatedProp : updatedProps) {
            auto name = updatedProp.get_name();
            // TODO(Laura): get value by expr
            auto value = updatedProp.get_value();
            auto findTag = tagsInfo.find(tagId);
            if (findTag != tagsInfo.end()) {
                findTag->second.names.emplace_back(name);
                findTag->second.values.emplace_back(value);
            } else {
                tagInfo.names.emplace_back(updatedProp.get_name());
                tagInfo.values.emplace_back(updatedProp.get_value());
            }
        }

        auto vertexInfo = &verticesInfo.vertices[vertex];

        auto ret = getTagWholeValue(spaceId, tagId, tagInfo.values, tagInfo.names);
        if (!ret.ok()) {
            LOG(ERROR) << ret.status();
            return ret.status();
        }
        (*vertexInfo)[tagId] = std::move(ret).value();
    }
    return dataSet;
}

StatusOr<DataSet> StorageCache::updateEdge(const storage::cpp2::UpdateEdgeRequest &req) {
    auto spaceId = req.get_space_id();
    auto &edgeKey = req.get_edge_key();
    auto updatedProps = req.get_updated_props();
    bool insertable = false;
    if (req.__isset.insertable) {
        insertable = *req.get_insertable();
    }
    std::vector<std::string> returnProps;
    if (req.__isset.return_props) {
        returnProps = *req.get_return_props();
    }
    std::string condition;
    if (req.__isset.condition) {
        condition = *req.get_condition();
    }
    auto findIt = cache_.find(spaceId);
    if (findIt == cache_.end()) {
        return Status::Error("SpaceID `%d' not found", spaceId);
    }
    auto &spaceInfo = findIt->second;
    auto findEdge = spaceInfo.edges.find(edgeKey);

    if (findEdge == spaceInfo.edges.end() && !insertable) {
        return Status::Error("Edge `%s:%d:%ld:%s' not found",
                              edgeKey.get_src().c_str(),
                              edgeKey.get_edge_type(),
                              edgeKey.get_ranking(),
                              edgeKey.get_dst().c_str());
    }

    DataSet dataSet;
    // update
    if (findEdge != spaceInfo.edges.end()) {
        // filter
        if (!condition.empty()) {
            return dataSet;
        }
        auto &edgeInfo = findEdge->second;
        for (auto &updatedProp : updatedProps) {
            auto prop = updatedProp.get_name();
            auto value = updatedProp.get_value();
            auto propFind = edgeInfo.find(prop);
            if (propFind == edgeInfo.end()) {
                return Status::Error("Prop `%s' not found", prop.c_str());
=======
    for (auto &part : parts) {
        for (auto &vId : part.second) {
            auto vFindIt = vertices.find(vId);
            if (vFindIt == vertices.end()) {
                return Status::Error("VertexId `%s' not found", vId.c_str());
>>>>>>> rebase upstream
            }
            storage::cpp2::VertexPropData vertex;
            vertex.set_id(vId);
            vertex.set_props(vFindIt.second);
            vertex.set_names();
        }
    }
<<<<<<< HEAD
    return dataSet;
>>>>>>> add update executor and test
=======
#endif
>>>>>>> rebase upstream
}
}  // namespace graph
}  // namespace nebula

