/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/SwitchSpaceExecutor.h"
#include "planner/Query.h"
#include "service/ExecutionContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> SwitchSpaceExecutor::execute() {
    folly::Promise<Status> promise;
    auto future = promise.getFuture();
    auto *spaceToNode = asNode<SwitchSpace>(node());
    ectx()->rctx()->session()->setSpace(spaceToNode->getSpaceName(), spaceToNode->getSpaceId());
    LOG(INFO) << "Graph space switched to `" << spaceToNode->getSpaceName()
              << "', space id: " << spaceToNode->getSpaceId();
    promise.setValue(Status::OK());
    return future;
}

}   // namespace graph
}   // namespace nebula
