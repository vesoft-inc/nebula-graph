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

StatusOr<DataSet> StorageCache::getProps(const storage::cpp2::GetPropRequest& req) {
    auto spaceId = req.get_space_id();
    const auto &props = req.get_props();
    std::vector<TagID>       tagsId;
    std::vector<std::string> symsName;
    std::vector<std::string> propsName;
    symsName.reserve(props.size());
    propsName.reserve(props.size());
    for (const auto &prop : props) {
        auto propExpr =  Expression::decode(prop.get_prop());
        CHECK(
            propExpr->kind() == Expression::Kind::kEdgeProperty ||
            propExpr->kind() == Expression::Kind::kDstProperty ||
            propExpr->kind() == Expression::Kind::kSrcProperty ||
            propExpr->kind() == Expression::Kind::kEdgeSrc ||
            propExpr->kind() == Expression::Kind::kEdgeType ||
            propExpr->kind() == Expression::Kind::kEdgeRank ||
            propExpr->kind() == Expression::Kind::kEdgeDst);
        auto *symbolExpr = static_cast<SymbolPropertyExpression*>(propExpr.get());
        symsName.emplace_back(*symbolExpr->sym());
        propsName.emplace_back(*symbolExpr->prop());
        if (req.get_column_names().front() == _VID) {
            auto tagIdResult = metaClient_->getTagIDByNameFromCache(spaceId, *symbolExpr->sym());
            if (!tagIdResult.ok()) {
                LOG(ERROR) << tagIdResult.status().toString();
                return std::move(tagIdResult).status();
            }
            tagsId.emplace_back(tagIdResult.value());
        } else {
            auto edgeResult = metaClient_->getEdgeTypeByNameFromCache(spaceId, *symbolExpr->sym());
            if (!edgeResult.ok()) {
                LOG(ERROR) << edgeResult.status().toString();
                return std::move(edgeResult).status();
            }
        }
    }
    DataSet v;
    std::unordered_map<std::string, std::vector<std::string>> symProps;
    for (std::size_t i = 0; i < symsName.size(); ++i) {
        v.colNames.emplace_back(symsName[i] + "." + propsName[i]);
        auto symProp = symProps.find(symsName[i]);
        if (symProp == symProps.end()) {
            symProps.emplace(symsName[i], std::vector<std::string>{propsName[i]});
        } else {
            symProp->second.emplace_back(propsName[i]);
        }
    }

    folly::RWSpinLock::ReadHolder rh(lock_);
    const auto spaceData = cache_.find(spaceId);
    if (spaceData == cache_.end()) {
        return Status::Error("Not space %d", spaceId);
    }
    if (req.get_column_names().front() == _VID) {
        // get vertices
        const auto &vertices = spaceData->second.vertices;
        for (const auto &part : req.get_parts()) {
            for (const auto &row : part.second) {
                const auto &vid = row.columns.front();
                const auto &vertex = vertices.find(vid.getStr());
                if (vertex != vertices.end()) {
                    Row record;
                    for (std::size_t i = 0; i < tagsId.size(); ++i) {
                        const auto tagProps = vertex->second.find(tagsId[i]);
                        if (tagProps != vertex->second.end()) {
                            const auto propIndex = tagProps->second.propIndexes.find(propsName[i]);
                            if (propIndex != tagProps->second.propIndexes.end()) {
                                record.emplace_back(tagProps->second.propValues[propIndex->second]);
                            } else {
                                record.emplace_back(Value());
                            }
                        }
                    }
                    v.emplace_back(std::move(record));
                }
            }
        }
    } else {
        // get edges
        if (propsName.empty()) {
            LOG(FATAL) << "Not supported";
        }

        if (propsName.front() == "*") {
            v.clear();
            v.colNames.emplace_back(symsName.front() + "." + _SRC);
            v.colNames.emplace_back(symsName.front() + "." + _DST);
            v.colNames.emplace_back(symsName.front() + "." + _RANK);
            const auto edgeTypeResult = mgr_->toEdgeType(spaceId, symsName.front());
            if (!edgeTypeResult.ok()) {
                LOG(ERROR) << "Edge " << symsName.front() << " not find.";
                return Status::EdgeNotFound("Edge %s not find.", symsName.front().c_str());
            }
            auto edgeType = edgeTypeResult.value();
            const auto edgeSchema = mgr_->getEdgeSchema(spaceId, edgeType);
            if (edgeSchema != nullptr) {
                LOG(ERROR) << "Edge " << symsName.front() << " schema not find.";
                return Status::EdgeNotFound("Edge type %d schema not find.", edgeType);
            }
            for (std::size_t i = 0; i < edgeSchema->getNumFields(); ++i) {
                v.colNames.emplace_back(symsName.front() + "." + edgeSchema->getFieldName(i));
            }
        }

        const auto &edges = spaceData->second.edges;
        for (const auto &part : req.get_parts()) {
            for (const auto &row : part.second) {
                const auto &src = row.columns[0];
                const auto type = row.columns[1];
                const auto ranking = row.columns[2];
                const auto &dst = row.columns[3];
                const auto key =
                    getEdgeKey(src.getStr(), type.getInt(), ranking.getInt(), dst.getStr());
                const auto edge = edges.find(key);
                if (edge != edges.end()) {
                    Row record;
                    if (propsName.front() == "*") {
                        // all properties of this edge
                        record.emplace_back(src);
                        record.emplace_back(dst);
                        record.emplace_back(ranking);
                        for (const auto &value : edge->second.propValues) {
                            record.emplace_back(value);
                        }
                    } else {
                        for (std::size_t i = 0; i < propsName.size(); ++i) {
                            const auto propIndex = edge->second.propIndexes.find(propsName[i]);
                            if (propIndex != edge->second.propIndexes.end()) {
                                record.emplace_back(edge->second.propValues[propIndex->second]);
                            } else {
                                record.emplace_back(Value());
                            }
                        }
                    }
                    v.emplace_back(std::move(record));
                }
            }
        }
    }
    return std::move(v);
}
}  // namespace graph
}  // namespace nebula
