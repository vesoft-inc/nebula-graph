/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "exec/admin/DropSpaceExecutor.h"
#include "planner/Admin.h"
#include "service/ExecutionContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> DropSpaceExecutor::execute() {
    dumpLog();

    auto *dsNode = asNode<DropSpace>(node());
    return ectx()->getMetaClient()->dropSpace(dsNode->getSpaceName(), dsNode->getIfExists())
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
