/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/ProduceSemiShortestPathExecutor.h"

namespace nebula {
namespace graph {
folly::Future<Status> ProduceSemiShortestPathExecutor::execute() {
    return Status::Error("Unimplemented executor: %s", name_.c_str());
}

void ProduceSemiShortestPathExecutor::singleSource() {
}
}  // namespace graph
}  // namespace nebula
