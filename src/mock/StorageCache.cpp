/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "mock/StorageCache.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace graph {
constexpr char SRC[] = "_src";
constexpr char TYPE[] = "_type";
constexpr char RANK[] = "_rank";
constexpr char DST[] = "_dst";

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
            std::unordered_map<TagID,
                               std::unordered_map<std::string, Value>> *vertexInfo = nullptr;
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
                auto ret = getTagWholeValue(spaceId, tagId, propValues, propNames[tagId]);
                if (!ret.ok()) {
                    LOG(ERROR) << ret.status();
                    return ret.status();
                }
                (*vertexInfo)[tagId] = std::move(ret).value();
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
            storage::cpp2::EdgeKey edgeKey;
            edgeKey.set_src(key.get_src());
            edgeKey.set_edge_type(key.get_edge_type());
            edgeKey.set_ranking(key.get_ranking());
            edgeKey.set_dst(key.get_dst());
>>>>>>> add storage mock interface
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
>>>>>>> add storage mock interface
            if (!ret.ok()) {
                LOG(ERROR) << ret.status();
                return ret.status();
            }
            spaceDataInfo->edges[edgeKey] = std::move(ret).value();
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
    }
    auto schema = mgr_->getTagSchema(spaceId, tagId);
    if (schema == nullptr) {
        return Status::Error("TagId `%d' not exist", tagId);
=======
StatusOr<DataSet> StorageCache::getNeighbors(const storage::cpp2::GetNeighborsRequest &req) {
    folly::RWSpinLock::ReadHolder holder(lock_);
    DataSet dataSet;
    auto spaceId = req.get_space_id();
    auto findIt = cache_.find(spaceId);
    if (findIt == cache_.end()) {
        return Status::Error("SpaceID `%d' not found", spaceId);
    }
    auto &verticesInfo = findIt->second.vertices;
    auto &edgesInfo = findIt->second.edges;
    auto parts = req.get_parts();
    std::vector<std::string> vertexIds;

    std::unordered_map<std::string, std::vector<std::pair<std::string, std::string>>> tagPropNames;
    std::unordered_map<std::string, std::vector<std::pair<std::string, std::string>>> edgePropNames;
    for (auto &part : parts) {
        for (auto &row : part.second) {
            if (row.columns.size() != 1) {
                return Status::Error("Wrong row size: %ld", row.columns.size());
            }
            if (row.columns[0].type() != Value::Type::STRING) {
                return Status::Error("Wrong value type: %d",
                                     static_cast<int32_t>(row.columns[0].type()));
            }
            auto vId = row.columns[0].getStr();
            auto vFindIt = verticesInfo.find(vId);
            if (vFindIt == verticesInfo.end()) {
                return Status::Error("VertexId `%s' not found", vId.c_str());
            }
            vertexIds.emplace_back(vId);
            for (auto &tag : vFindIt->second) {
                auto ret = mgr_->toTagName(spaceId, tag.first);
                if (!ret.ok()) {
                    return ret.status();
                }
                auto tagName = std::move(ret).value();
                for (auto &prop : tag.second) {
                    tagPropNames[tagName].emplace_back(prop.first, prop.first);
                }
            }
        }
    }

    // Get edge
    auto edgeTypes = req.get_edge_types();
    auto edgeDirection = req.get_edge_direction();
    std::unordered_map<VertexID, std::vector<storage::cpp2::EdgeKey>> edgeKeys;
    if (edgeTypes.empty()) {
        edgeKeys = getAllEdgeKeys(vertexIds, edgesInfo, edgeDirection);
    } else {
        edgeKeys = getMultiEdgeKeys(vertexIds, req.get_edge_types(), edgesInfo);
    }

    if (edgeKeys.empty()) {
        LOG(INFO) << "getNeighbors:Empty";
        return DataSet();
    }
    // Get prop name
    if (req.__isset.vertex_props) {
        if (!req.get_vertex_props()->empty()) {
            auto ret = getTagPropNames(spaceId, *req.get_vertex_props());
            if (!ret.ok()) {
                return ret.status();
            }
            tagPropNames = std::move(ret).value();
        }
    } else {
        tagPropNames.clear();
    }
    if (edgeTypes.empty()) {
        // get all edgeType from edgeKeys which get from vertices
        for (auto &edgeKey : edgeKeys) {
            for (auto &edge : edgeKey.second) {
                auto edgeType = edge.get_edge_type();
                auto schema = mgr_->getEdgeSchema(spaceId, std::abs(edgeType));
                if (schema == nullptr) {
                    return Status::Error("EdgeType `%d' not exist", edgeType);
                }
                auto ret = mgr_->toEdgeName(spaceId, std::abs(edgeType));
                if (!ret.ok()) {
                    return Status::Error("EdgeName by `%d' not found", edgeType);
                }
                auto edgeName = ret.value();
                for (auto i = 0u; i < schema->getNumFields(); i++) {
                    edgePropNames[edgeName].emplace_back(schema->field(i)->name(),
                                                         schema->field(i)->name());
                }
            }
        }
    } else if (req.__isset.edge_props) {
        if (!req.get_edge_props()->empty()) {
            auto ret = getEdgePropNames(spaceId, *req.get_edge_props());
            if (!ret.ok()) {
                return ret.status();
            }
            edgePropNames = std::move(ret).value();
        } else {
            // get all edge prop about given edgeTypes
            for (auto edgeType : edgeTypes) {
                auto schema = mgr_->getEdgeSchema(spaceId, edgeType);
                if (schema == nullptr) {
                    return Status::Error("EdgeType `%d' not exist", edgeType);
                }
                auto ret = mgr_->toEdgeName(spaceId, edgeType);
                if (!ret.ok()) {
                    return Status::Error("EdgeName by `%d' not found", edgeType);
                }
                auto edgeName = ret.value();
                for (auto i = 0u; i < schema->getNumFields(); i++) {
                    edgePropNames[edgeName].emplace_back(schema->field(i)->name(),
                                                         schema->field(i)->name());
                }
            }
        }
>>>>>>> add storage mock interface
    }
    return getPropertyInfo(schema, props, names);
}

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
=======
    // Init column name
    std::vector<std::string> columnNames;
    columnNames.emplace_back("_vid");
    columnNames.emplace_back("_stat:");
    for (auto &tag : tagPropNames) {
        std::string tagProp = "_tag:" + tag.first;
        for (auto &prop : tag.second) {
            tagProp += folly::stringPrintf(":%s_%s", prop.first.c_str(), prop.second.c_str());
        }
        columnNames.emplace_back(tagProp);
    }
    for (auto &edge : edgePropNames) {
        std::string edgeProp = "_edge:" + edge.first;
        for (auto &prop : edge.second) {
            edgeProp += folly::stringPrintf(":%s_%s", prop.first.c_str(), prop.second.c_str());
        }
        columnNames.emplace_back(edgeProp);
    }

    // Generate results
    std::vector<Row> rows;
    for (auto &vertex : vertexIds) {
        Row row;
        std::vector<Value> columns;
        columns.emplace_back(Value(vertex));
        columns.emplace_back(Value());
        List vList;
        std::vector<Value> vValues;
        // get vertices props
        for (auto &tag : tagPropNames) {
            auto tagId = mgr_->toTagID(spaceId, tag.first);
            auto findTag = verticesInfo[vertex].find(tagId.value());
            for (auto &prop : tag.second) {
                if (findTag != verticesInfo[vertex].end()) {
                    auto findProp = findTag->second.find(prop.first);
                    if (findProp == findTag->second.end()) {
                        return Status::Error("Wrong tag prop name: %s", prop.first.c_str());
                    }
                    vValues.emplace_back(findProp->second);
                } else {
                    vValues.emplace_back(Value());
                }
            }
        }
        vList.values = std::move(vValues);
        columns.emplace_back(Value(vList));

        // get edge props
        List eList;

        auto findEdges = edgeKeys.find(vertex);
        if (findEdges == edgeKeys.end()) {
            columns.emplace_back(Value(eList));
        } else {
            std::vector <Value> eValues;
            for (auto &edgeKey : findEdges->second) {
                std::vector <Value> values;
                for (auto &edge : edgePropNames) {
                    for (auto &prop : edge.second) {
                        auto edgeType = mgr_->toEdgeType(spaceId, edge.first);
                        if (edgeKey.get_edge_type() == edgeType.value()) {
                            // get edge key prop
                            if (prop.first == SRC) {
                                values.emplace_back(edgeKey.get_src());
                                continue;
                            }
                            if (prop.first == TYPE) {
                                values.emplace_back(edgeKey.get_edge_type());
                                continue;
                            }
                            if (prop.first == RANK) {
                                values.emplace_back(edgeKey.get_ranking());
                                continue;
                            }
                            if (prop.first == DST) {
                                values.emplace_back(edgeKey.get_dst());
                                continue;
                            }

                            auto findProp = edgesInfo[edgeKey].find(prop.first);
                            if (findProp == edgesInfo[edgeKey].end()) {
                                return Status::Error("Wrong edge prop name: %s",
                                                     prop.first.c_str());
                            }
                            values.emplace_back(findProp->second);
                        } else {
                            values.emplace_back(Value());
                        }
                    }
                }
                List list;
                list.values = std::move(values);
                eValues.emplace_back(Value(list));
                eList.values.emplace_back(list);
            }
        }
        columns.emplace_back(Value(eList));
        row.columns = std::move(columns);
        rows.emplace_back(row);
    }
    dataSet.colNames = std::move(columnNames);
    dataSet.rows = std::move(rows);
    return dataSet;
}

StatusOr<DataSet>
StorageCache::getProps(const storage::cpp2::GetPropRequest &req) {
    folly::RWSpinLock::ReadHolder holder(lock_);
    DataSet dataSet;
    auto spaceId = req.get_space_id();
    auto findIt = cache_.find(spaceId);
    if (findIt == cache_.end()) {
        return Status::Error("SpaceID `%d' not found", spaceId);
    }
    auto columnNames = req.get_column_names();
    auto isVertex = false;
    if (columnNames.size() == 1 && columnNames[0] == "_vid") {
        isVertex = true;
    } else if (columnNames.size() == 4
               && columnNames[0] == SRC
               && columnNames[1] == TYPE
               && columnNames[2] == RANK
               && columnNames[3] == DST) {
        isVertex = false;
    } else {
        return Status::Error("Wrong format");
    }
    if (isVertex) {
        auto vertices = findIt->second.vertices;
        return getVertices(spaceId, vertices, req);
    }
    auto edges = findIt->second.edges;
    return getEdges(spaceId, edges, req);
}

StatusOr<DataSet> StorageCache::getVertices(GraphSpaceID spaceId,
                                            const VerticesInfo &vertices,
                                            const storage::cpp2::GetPropRequest &req) {
    std::unordered_map<std::string,
                       std::vector<std::pair<std::string, std::string>>> tagPropNames;
    auto parts = req.get_parts();
    std::vector<std::string> vertexIds;
    for (auto &part : parts) {
        for (auto &row : part.second) {
            if (row.columns.size() != 1) {
                return Status::Error("Wrong row size: %ld", row.columns.size());
            }
            if (row.columns[0].type() != Value::Type::STRING) {
                return Status::Error("Wrong value type: %d",
                                      static_cast<int32_t>(row.columns[0].type()));
            }
            auto vId = row.columns[0].getStr();
            auto vFindIt = vertices.find(vId);
            if (vFindIt == vertices.end()) {
                return Status::Error("VertexId `%s' not found", vId.c_str());
            }
            vertexIds.emplace_back(vId);
            for (auto &tag : vFindIt->second) {
                auto ret = mgr_->toTagName(spaceId, tag.first);
                if (!ret.ok()) {
                    return ret.status();
                }
                auto tagName = std::move(ret).value();
                for (auto &prop : tag.second) {
                    tagPropNames[tagName].emplace_back(prop.first, prop.first);
                }
            }
        }
    }

    auto props = req.get_props();
    if (props.size() != 0) {
        tagPropNames.clear();
        auto ret = getTagPropNames(spaceId, props);
        if (!ret.ok()) {
            return ret.status();
        }
        tagPropNames = std::move(ret).value();
    }
    // Init column name
    std::vector<std::string> columnNames;
    columnNames.emplace_back("_vid");
    for (auto &tag : tagPropNames) {
        for (auto &prop : tag.second) {
            columnNames.emplace_back(folly::stringPrintf("%s:%s_%s",
                    tag.first.c_str(), prop.first.c_str(), prop.second.c_str()));
        }
    }

    std::vector<Row> rows;
    for (auto &vId : vertexIds) {
        Row row;
        auto vFindIt = vertices.find(vId);
        row.columns.emplace_back(Value(vId));
        for (auto &tag : tagPropNames) {
            auto ret = mgr_->toTagID(spaceId, tag.first);
            if (!ret.ok()) {
                return ret.status();
            }
            auto tagId = ret.value();
            auto tagProps = vFindIt->second;
            auto tagFind = tagProps.find(tagId);
            if (tagFind == tagProps.end()) {
                for (auto i = 0u ; i < tag.second.size(); i++) {
                    row.columns.emplace_back(Value());
                }
            } else {
                for (auto &propName : tag.second) {
                    row.columns.emplace_back(tagFind->second[propName.first]);
                }
            }
        }
        rows.emplace_back(row);
    }
    DataSet result;
    result.colNames = std::move(columnNames);
    result.rows = std::move(rows);
    return result;
}

StatusOr<DataSet> StorageCache::getEdges(GraphSpaceID spaceId,
                                         const EdgesInfo &edges,
                                         const storage::cpp2::GetPropRequest &req) {
    UNUSED(spaceId);
    UNUSED(edges);
    UNUSED(req);
    return DataSet();
}

StatusOr<std::unordered_map<std::string, Value>>
StorageCache::getTagWholeValue(const GraphSpaceID spaceId,
                               const TagID tagId,
                               const std::vector<Value>& props,
                               const std::vector<std::string> &names) {
    if (props.size() != names.size()) {
        return Status::Error("Wrong size between props and names");
    }
    auto schema = mgr_->getTagSchema(spaceId, tagId);
    if (schema == nullptr) {
        return Status::Error("TagID `%d' not exist", tagId);
    }
    LOG(INFO) << "tagId: " << tagId << ", schema->getNumFields(): " << schema->getNumFields();
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

std::unordered_map<VertexID, std::vector<storage::cpp2::EdgeKey>>
StorageCache::getAllEdgeKeys(const std::vector<VertexID> &vertices,
                             const EdgesInfo &edgesInfo,
                             const storage::cpp2::EdgeDirection edgeDirection) {
    std::unordered_map<VertexID, std::vector<storage::cpp2::EdgeKey>> edgeKeys;
    for (auto &vertex : vertices) {
        for (auto &edge : edgesInfo) {
            if (edge.first.get_src() == vertex) {
                if (edgeDirection == storage::cpp2::EdgeDirection::IN_EDGE &&
                    edge.first.get_edge_type() > 0) {
                    continue;
                }
                if (edgeDirection == storage::cpp2::EdgeDirection::OUT_EDGE &&
                    edge.first.get_edge_type() < 0) {
                    continue;
                }
                edgeKeys[vertex].emplace_back(edge.first);
            }
        }
    }
    return edgeKeys;
}

std::unordered_map<VertexID, std::vector<storage::cpp2::EdgeKey>>
StorageCache::getMultiEdgeKeys(const std::vector<VertexID> &vertices,
                               const std::vector<EdgeType> &edgeTypes,
                               const EdgesInfo &edgesInfo) {
    std::unordered_map<VertexID, std::vector<storage::cpp2::EdgeKey>> edgeKeys;
    for (auto &vertex : vertices) {
        for (auto &edge : edgesInfo) {
            for (auto &edgeType : edgeTypes) {
                if (edge.first.get_src() == vertex && edge.first.get_edge_type() == edgeType) {
                    edgeKeys[vertex].emplace_back(edge.first);
                }
            }
        }
    }
    return edgeKeys;
}

StatusOr<std::unordered_map<std::string, std::vector<std::pair<std::string, std::string>>>>
StorageCache::getTagPropNames(const GraphSpaceID spaceId,
                              const std::vector<storage::cpp2::PropExp> &props) {
    std::unordered_map<std::string, std::vector<std::pair<std::string, std::string>>> tagPropNames;
    for (auto &prop : props) {
        std::vector<folly::StringPiece> names;
        folly::split(".", prop.prop, names);
        if (names.size() != 2) {
            return Status::Error("Wrong prop name: %s", prop.prop.c_str());
        }
        tagPropNames[names[0].str()] = {};
        if (names[1] == "*") {
            auto ret = mgr_->toTagID(spaceId, names[0]);
            if (!ret.ok()) {
                return ret.status();
            }
            auto tagId = ret.value();
            auto schema = mgr_->getTagSchema(spaceId, tagId);
            if (schema == nullptr) {
                return Status::Error("TagName `%s' not exist", names[0].str().c_str());
            }
            for (auto i = 0u; i < schema->getNumFields(); i++) {
                tagPropNames[names[0].str()].emplace_back(schema->field(i)->name(),
                                                          schema->field(i)->name());
            }
        } else {
            tagPropNames[names[0].str()].emplace_back(names[1].str(), prop.alias);
        }
    }
    return tagPropNames;
}

StatusOr<std::unordered_map<std::string, std::vector<std::pair<std::string, std::string>>>>
StorageCache::getEdgePropNames(const GraphSpaceID spaceId,
                               const std::vector<storage::cpp2::PropExp> &props) {
    std::unordered_map<std::string,
                       std::vector<std::pair<std::string, std::string>>> edgePropNames;
    for (auto &prop : props) {
        std::vector<folly::StringPiece> names;
        folly::split(".", prop.prop, names);
        if (names.size() != 2) {
            return Status::Error("Wrong prop name: %s", prop.prop.c_str());
        }

        if (names[1] == "*") {
            auto ret = mgr_->toEdgeType(spaceId, names[0]);
            if (!ret.ok()) {
                return ret.status();
            }
            auto edgeType = ret.value();
            auto schema = mgr_->getEdgeSchema(spaceId, edgeType);
            if (schema == nullptr) {
                return Status::Error("EdgeName `%s' not exist", names[0].str().c_str());
            }
            for (auto i = 0u; i < schema->getNumFields(); i++) {
                edgePropNames[names[0].str()].emplace_back(schema->field(i)->name(),
                                                           schema->field(i)->name());
            }
        } else {
            edgePropNames[names[0].str()].emplace_back(names[1].str(), prop.alias);
>>>>>>> add storage mock interface
        }
        index++;
    }
<<<<<<< HEAD

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
    return edgePropNames;
>>>>>>> add storage mock interface
}
}  // namespace graph
}  // namespace nebula
