/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_STORAGECACHE_H_
#define EXEC_STORAGECACHE_H_

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "common/clients/meta/MetaClient.h"
#include "common/meta/ServerBasedSchemaManager.h"
#include "common/meta/NebulaSchemaProvider.h"
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
<<<<<<< HEAD
                     std::unordered_map<TagID, std::unordered_map<std::string, Value>>>;
using EdgesInfo = std::unordered_map<storage::cpp2::EdgeKey,
                                     std::unordered_map<std::string, Value>, EdgeHasher>;
=======
      std::unordered_map<TagID, std::unordered_map<std::string, Value>>>;
using EdgesInfo = std::unordered_map<storage::cpp2::EdgeKey,
      std::unordered_map<std::string, Value>, EdgeHasher>;
>>>>>>> add update executor and test

class StorageCache final {
public:
    explicit StorageCache(uint16_t metaPort);

    ~StorageCache() = default;

    Status addVertices(const storage::cpp2::AddVerticesRequest& req);

    Status addEdges(const storage::cpp2::AddEdgesRequest& req);

<<<<<<< HEAD
private:
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

private:
    struct SpaceDataInfo {
        SpaceDataInfo() = default;
        ~SpaceDataInfo() = default;
=======
    StatusOr<DataSet> getProps(const storage::cpp2::GetPropRequest &req);

    StatusOr<DataSet> updateVertex(const storage::cpp2::UpdateVertexRequest &req);

    StatusOr<DataSet> updateEdge(const storage::cpp2::UpdateEdgeRequest &req);

private:
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

private:
    struct SpaceDataInfo {
>>>>>>> add update executor and test
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
