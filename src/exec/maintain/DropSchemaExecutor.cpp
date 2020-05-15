/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "exec/maintain/DropSchemaExecutor.h"
#include "planner/Maintain.h"
#include "service/ExecutionContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> DropTagExecutor::execute() {
    dumpLog();

    auto *dtNode = asNode<DropTag>(node());
    return ectx()->getMetaClient()->dropTagSchema(dtNode->getSpaceId(),
                                                  dtNode->getName(),
                                                  dtNode->getIfExists())
            .via(runner())
            .then([this](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}

folly::Future<Status> DropEdgeExecutor::execute() {
    dumpLog();

    auto *deNode = asNode<DropEdge>(node());
    return ectx()->getMetaClient()->dropEdgeSchema(deNode->getSpaceId(),
                                                   deNode->getName(),
                                                   deNode->getIfExists())
            .via(runner())
            .then([this](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}

}   // namespace graph
}   // namespace nebula
