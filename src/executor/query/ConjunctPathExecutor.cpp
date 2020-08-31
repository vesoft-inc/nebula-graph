/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/ConjunctPathExecutor.h"

namespace nebula {
namespace graph {
folly::Future<Status> ConjunctPathExecutor::execute() {
    return Status::Error("Unimplemented executor: %s", name_.c_str());
}
}  // namespace graph
}  // namespace nebula
