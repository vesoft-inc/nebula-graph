/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/GetNeighborsExecutor.h"

// common
#include "clients/storage/GraphStorageClient.h"
#include "datatypes/List.h"
#include "datatypes/Vertex.h"
// graph
#include "planner/Query.h"
#include "service/ExecutionContext.h"

using nebula::cpp2::TagID;
using nebula::storage::GraphStorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;

namespace nebula {
namespace graph {

folly::Future<Status> GetNeighborsExecutor::execute() {
    return SingleInputExecutor::execute().then(cb([this](Status s) {
        if (!s.ok()) return error(std::move(s));

        return getNeighbors().ensure([this]() {
            // TODO(yee): some cleanup or stats actions
            UNUSED(this);
        });
    }));
}

folly::Future<Status> GetNeighborsExecutor::getNeighbors() {
    dumpLog();

    const GetNeighbors* gn = asNode<GetNeighbors>(node());
    Expression* srcExpr = gn->src();
    // TODO(yee): get vertex id list by evaluating above expression
    UNUSED(srcExpr);
    // srcExpr->eval(Getters &getters);

    std::vector<std::string> colNames;

    GraphStorageClient* storageClient = ectx()->getStorageClient();
    return storageClient
        ->getNeighbors(gn->space(),
                       std::move(colNames),
                       gn->vertices(),
                       gn->edgeTypes(),
                       gn->edgeDirection(),
                       &gn->statProps(),
                       &gn->vertexProps(),
                       &gn->edgeProps(),
                       gn->dedup(),
                       gn->orderBy(),
                       gn->limit(),
                       gn->filter())
        .via(runner())
        .then([this](StorageRpcResponse<GetNeighborsResponse>&& resp) {
            auto completeness = resp.completeness();
            if (completeness != 0) {
                return error(Status::Error("Get neighbors failed"));
            }
            if (completeness != 100) {
                // TODO(dutor) We ought to let the user know that the execution was partially
                // performed, even in the case that this happened in the intermediate process.
                // Or, make this case configurable at runtime.
                // For now, we just do some logging and keep going.
                LOG(INFO) << "Get neighbors partially failed: " << completeness << "%";
                for (auto& error : resp.failedParts()) {
                    LOG(ERROR) << "part: " << error.first
                               << "error code: " << static_cast<int>(error.second);
                }
            }

            auto status = handleResponse(resp.responses());
            return status.ok() ? start() : error(std::move(status));
        });
}

Status GetNeighborsExecutor::handleResponse(const std::vector<GetNeighborsResponse>& responses) {
    // nebula::List l;
    // for (auto& resp : responses) {
    //     auto vertices = resp.get_vertices();
    //     if (vertices == nullptr) {
    //         LOG(INFO) << "Empty vertices in response";
    //         continue;
    //     }

    //     for (auto& vertex : *vertices) {
    //         auto vertData = vertex.get_vertex_data();
    //         if (vertData == nullptr) {
    //             LOG(INFO) << "Empty properties in vertex " << vertex.get_id();
    //             continue;
    //         }

    //         auto gn = asNode<GetNeighbors>(node());
    //         std::vector<Tag> tags;
    //         auto status = collectVertexTags(gn->vertexProps(), vertData->get_props(), &tags);
    //         if (!status.ok()) return status;

    //         nebula::Vertex vert;
    //         vert.vid = vertex.get_id();
    //         vert.tags = std::move(tags);

    //         l.values.emplace_back(std::move(vert));
    //     }
    // }

    // // Store response results to ExecutionContext
    // return finish({std::move(l)});
    UNUSED(responses);
    return Status::OK();
}

Status GetNeighborsExecutor::collectVertexTags(const std::vector<std::string>& schema,
                                               const std::vector<Value>& resp,
                                               std::vector<Tag>* tags) const {
    // if (schema.size() != resp.size()) {
    //     return Status::Error("Invalid storage response data of vertices");
    // }

    // std::unordered_map<TagID, Tag> tagMap;
    // // auto schemaMgr = ectx()->schemaManager();

    // for (size_t i = 0, e = schema.size(); i < e; i++) {
    //     auto tagId = schema[i].get_tag();
    //     auto iter = tagMap.find(tagId);
    //     if (iter == tagMap.end()) {
    //         // TODO(yee): wait for nebula-common updating
    //         // auto tagName = schemaMgr->toTagName(gn->space(), tagId);
    //         nebula::Tag tag;
    //         std::string tagName = "";
    //         tag.name = tagName;
    //         auto inserted = tagMap.insert({tagId, std::move(tag)});
    //         if (!inserted.second) {
    //             return Status::Error("Runtime error for caching tag %d", tagId);
    //         }
    //         iter = inserted.first;
    //     }
    //     iter->second.props.insert({schema[i].get_name(), std::move(resp[i])});
    // }

    // for (auto& t : tagMap) {
    //     tags->emplace_back(std::move(t.second));
    // }
    // return Status::OK();

    UNUSED(schema);
    UNUSED(resp);
    UNUSED(tags);
    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
