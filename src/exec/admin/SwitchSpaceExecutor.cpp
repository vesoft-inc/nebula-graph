/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/admin/SwitchSpaceExecutor.h"

#include "planner/Query.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> SwitchSpaceExecutor::execute() {
    auto *spaceToNode = asNode<SwitchSpace>(node());
    auto &spaceName = spaceToNode->getSpaceName();
    auto ret = qctx_->schemaMng()->toGraphSpaceID(spaceName);
    if (!ret.ok()) {
        LOG(ERROR) << "Unknown space: " << spaceName;
        return ret.status();
    }
    auto spaceId = ret.value();
    qctx_->rctx()->session()->setSpace(spaceName, spaceId);
    LOG(INFO) << "Graph space switched to `" << spaceName
              << "', space id: " << spaceId;
    return start();
}

}   // namespace graph
}   // namespace nebula
