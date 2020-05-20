/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/ProjectExecutor.h"

#include "parser/Clauses.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

folly::Future<Status> ProjectExecutor::execute() {
    return SingleInputExecutor::execute().then(cb([this](Status s) {
        if (!s.ok()) return error(std::move(s));
        dumpLog();
        auto *project = asNode<ProjectNode>(node());
        auto columns = project->columns();
        UNUSED(columns);
        return start();
    }));
}

}   // namespace graph
}   // namespace nebula
