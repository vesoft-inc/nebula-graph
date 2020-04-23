/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "MockStorageServiceHandler.h"


namespace nebula {
namespace graph {

folly::Future<storage::cpp2::GetNeighborsResponse>
MockStorageServiceHandler::future_getNeighbors(const storage::cpp2::GetNeighborsRequest& req) {
    UNUSED(req);
    folly::Promise<storage::cpp2::GetNeighborsResponse> promise;
    auto future = promise.getFuture();
    return future;
}

folly::Future<storage::cpp2::VertexPropResponse>
MockStorageServiceHandler::future_getVertexProps(const storage::cpp2::VertexPropRequest& req) {
    UNUSED(req);
    folly::Promise<storage::cpp2::VertexPropResponse> promise;
    auto future = promise.getFuture();
    return future;
}

folly::Future<storage::cpp2::EdgePropResponse>
MockStorageServiceHandler::future_getEdgeProps(const storage::cpp2::EdgePropRequest& req) {
    UNUSED(req);
    folly::Promise<storage::cpp2::EdgePropResponse> promise;
    auto future = promise.getFuture();
    return future;
}

folly::Future<storage::cpp2::ExecResponse>
MockStorageServiceHandler::future_addVertices(const storage::cpp2::AddVerticesRequest& req) {
    LOG(INFO) << "=== future_addVertices ===";
    folly::Promise<storage::cpp2::ExecResponse> promise;
    auto future = promise.getFuture();
    storage::cpp2::ExecResponse resp;
    storageCache_->addVertices(req);
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<storage::cpp2::ExecResponse>
MockStorageServiceHandler::future_addEdges(const storage::cpp2::AddEdgesRequest& req) {
    UNUSED(req);
    folly::Promise<storage::cpp2::ExecResponse> promise;
    auto future = promise.getFuture();
    return future;
}

folly::Future<storage::cpp2::ExecResponse>
MockStorageServiceHandler::future_deleteEdges(const storage::cpp2::DeleteEdgesRequest& req) {
    UNUSED(req);
    folly::Promise<storage::cpp2::ExecResponse> promise;
    auto future = promise.getFuture();
    return future;
}

folly::Future<storage::cpp2::ExecResponse>
MockStorageServiceHandler::future_deleteVertices(const storage::cpp2::DeleteVerticesRequest& req) {
    UNUSED(req);
    folly::Promise<storage::cpp2::ExecResponse> promise;
    auto future = promise.getFuture();
    return future;
}

folly::Future<storage::cpp2::UpdateResponse>
MockStorageServiceHandler::future_updateVertex(const storage::cpp2::UpdateVertexRequest& req) {
    UNUSED(req);
    folly::Promise<storage::cpp2::UpdateResponse> promise;
    auto future = promise.getFuture();
    return future;
}

folly::Future<storage::cpp2::UpdateResponse>
MockStorageServiceHandler::future_updateEdge(const storage::cpp2::UpdateEdgeRequest& req) {
    UNUSED(req);
    folly::Promise<storage::cpp2::UpdateResponse> promise;
    auto future = promise.getFuture();
    return future;
}

folly::Future<storage::cpp2::LookUpVertexIndexResp>
MockStorageServiceHandler::future_lookUpVertexIndex(const storage::cpp2::LookUpIndexRequest& req) {
    UNUSED(req);
    folly::Promise<storage::cpp2::LookUpVertexIndexResp> promise;
    auto future = promise.getFuture();
    return future;
}

folly::Future<storage::cpp2::LookUpEdgeIndexResp>
MockStorageServiceHandler::future_lookUpEdgeIndex(const storage::cpp2::LookUpIndexRequest& req) {
    UNUSED(req);
    folly::Promise<storage::cpp2::LookUpEdgeIndexResp> promise;
    auto future = promise.getFuture();
    return future;
}
}  // namespace graph
}  // namespace nebula

