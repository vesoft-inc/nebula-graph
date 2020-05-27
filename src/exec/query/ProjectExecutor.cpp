/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/ProjectExecutor.h"

#include "parser/Clauses.h"
#include "planner/Query.h"
#include "context/ExpressionContextImpl.h"

namespace nebula {
namespace graph {

folly::Future<Status> ProjectExecutor::execute() {
    dumpLog();
    auto *project = asNode<Project>(node());
    auto columns = project->columns()->columns();
    auto* iter = ectx_->getResult(project->inputVar()).iter();
    ExpressionContextImpl expCtx(ectx_, iter);
    // TODO: build colNames in validator.
    std::vector<std::string> colNames;
    for (auto& col : columns) {
        if (col->alias() == nullptr) {
            colNames.emplace_back(col->expr()->toString());
        } else {
            colNames.emplace_back(*col->alias());
        }
        col->expr()->setEctx(&expCtx);
    }

    DataSet ds;
    ds.colNames = std::move(colNames);
    for (; iter->valid(); iter->next()) {
        Row row;
        for (auto& col : columns) {
            auto val = Expression::eval(col->expr());
            ds.rows.emplace_back(std::move(row));
        }
    }
    auto status = finish(Result::buildDefault(Value(std::move(ds))));
    if (!status.ok()) {
        return error(std::move(status));
    }
    return start();
}

}   // namespace graph
}   // namespace nebula
