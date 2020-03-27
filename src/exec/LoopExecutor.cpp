/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/LoopExecutor.h"

#include "planner/Query.h"

namespace nebula {
namespace graph {

folly::Future<Status> LoopExecutor::exec() {
    auto *loopNode = static_cast<const Loop *>(node());
    Getters getters;
    auto res = loopNode->condition()->eval(getters);
    if (!res.ok()) {
        return folly::makeFuture(res.status());
    }
    // auto value = res.value();
    // TODO(yee): eval expression result
    bool result = false;
    if (!result) {
        return folly::makeFuture(Status::OK());
    }

    return body_->execute().then(cb([this](Status s) {
        if (!s.ok()) {
            return folly::makeFuture(s);
        }
        return this->exec();
    }));
}

}   // namespace graph
}   // namespace nebula
