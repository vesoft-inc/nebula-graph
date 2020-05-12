/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_STORAGECACHE_H_
#define EXEC_STORAGECACHE_H_

#include "base/Base.h"
#include "base/StatusOr.h"
#include "interface/gen-cpp2/storage_types.h"
#include "clients/meta/MetaClient.h"
#include "meta/ServerBasedSchemaManager.h"
#include "meta/NebulaSchemaProvider.h"
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>

namespace nebula {
namespace graph {

struct EdgeHasher {
    std::size_t operator()(const storage::cpp2::EdgeKey& k) const {
        std::size_t hash_val = 0;
        hash_val ^= ((std::hash<std::string>()(k.get_src())) << 1);
        hash_val ^= ((std::hash<int32_t>()(k.get_edge_type())) << 1);
        hash_val ^= ((std::hash<int64_t>()(k.get_ranking())) << 1);
        hash_val ^= ((std::hash<std::string>()(k.get_dst())) << 1);
        return hash_val;
    }
};

using VerticesInfo = std::unordered_map<VertexID,
      std::unordered_map<TagID, std::unordered_map<std::string, Value>>>;
using EdgesInfo = std::unordered_map<storage::cpp2::EdgeKey,
      std::unordered_map<std::string, Value>, EdgeHasher>;

class StorageCache final {
public:
    explicit StorageCache(uint16_t metaPort);

    ~StorageCache() = default;

    Status addVertices(const storage::cpp2::AddVerticesRequest& req);

    Status addEdges(const storage::cpp2::AddEdgesRequest& req);

    StatusOr<DataSet> getNeighbors(const storage::cpp2::GetNeighborsRequest &req);

    StatusOr<DataSet> getProps(const storage::cpp2::GetPropRequest &req);

private:
    std::string getEdgeKeyStr(const VertexID &srcId,
                              EdgeType type,
                              EdgeRanking rank,
                              const VertexID &dstId) {
        storage::cpp2::EdgeKey edgeKey;
        edgeKey.set_src(srcId);
        edgeKey.set_edge_type(type);
        edgeKey.set_ranking(rank);
        edgeKey.set_dst(dstId);
        std::string key;
        apache::thrift::CompactSerializer::serialize(edgeKey, &key);
        return key;
    }

    storage::cpp2::EdgeKey getEdgeKey(const std::string &key) {
        storage::cpp2::EdgeKey edgeKey;
        apache::thrift::CompactSerializer::deserialize(key, edgeKey);
        return edgeKey;
    }

    std::string srcEdgePrefix(VertexID srcId, EdgeType type) {
        storage::cpp2::EdgeKey edgeKey;
        edgeKey.set_src(srcId);
        edgeKey.set_edge_type(type);
        std::string key;
        apache::thrift::CompactSerializer::serialize(edgeKey, &key);
        return key;
    }

    StatusOr<std::unordered_map<std::string, Value>>
    getTagWholeValue(const GraphSpaceID spaceId,
                     const TagID tagId,
                     const std::vector<Value>& props,
                     const std::vector<std::string> &names);

    StatusOr<std::unordered_map<std::string, Value>>
    getEdgeWholeValue(const GraphSpaceID spaceId,
                      const EdgeType edgeType,
                      const std::vector<Value>& props,
                      const std::vector<std::string> &names);

    StatusOr<std::unordered_map<std::string, Value>>
    getPropertyInfo(std::shared_ptr<const meta::NebulaSchemaProvider> schema,
                    const std::vector<Value>& props,
                    const std::vector<std::string> &names);

    StatusOr<DataSet> getVertices(GraphSpaceID spaceId,
                                  const VerticesInfo &vertices,
                                  const storage::cpp2::GetPropRequest &req);

    StatusOr<DataSet> getEdges(GraphSpaceID spaceId,
                               const EdgesInfo &edges,
                               const storage::cpp2::GetPropRequest &req);

    // Use src_id get to all edgeType
    std::unordered_map<VertexID, std::vector<storage::cpp2::EdgeKey>>
    getAllEdgeKeys(const std::vector<VertexID> &vertices,
                   const EdgesInfo &edgesInfo,
                   const storage::cpp2::EdgeDirection edgeDirection);

    // Use src_id and edge type to get all edgeType
    std::unordered_map<VertexID, std::vector<storage::cpp2::EdgeKey>>
    getMultiEdgeKeys(const std::vector<VertexID> &vertices,
                     const std::vector<EdgeType> &edgeTypes,
                     const EdgesInfo &edgesInfo);

    StatusOr<std::unordered_map<std::string, std::vector<std::pair<std::string, std::string>>>>
    getTagPropNames(const GraphSpaceID spaceId, const std::vector<storage::cpp2::PropExp> &props);

    StatusOr<std::unordered_map<std::string, std::vector<std::pair<std::string, std::string>>>>
    getEdgePropNames(const GraphSpaceID spaceId, const std::vector<storage::cpp2::PropExp> &props);

    StatusOr<DataSet> getNeighborsResult(GraphSpaceID spaceId);


private:
    struct SpaceDataInfo {
        VerticesInfo     vertices;
        EdgesInfo        edges;
    };

    std::unordered_map<GraphSpaceID, SpaceDataInfo>   cache_;
    mutable folly::RWSpinLock                         lock_;
    std::unique_ptr<meta::MetaClient>                 metaClient_;
    std::unique_ptr<meta::ServerBasedSchemaManager>   mgr_;
};

}  // namespace graph
}  // namespace nebula
#endif  // EXEC_STORAGECACHE_H_
