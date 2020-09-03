/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/ConjunctPathExecutor.h"

#include "planner/Query.h"

namespace nebula {
namespace graph {
folly::Future<Status> ConjunctPathExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto* bfs = asNode<ConjunctPath>(node());
    auto lIter = ectx_->getResult(bfs->leftInputVar()).iter();
    const auto& rHist = ectx_->getHistory(bfs->rightInputVar());
    DCHECK(!!lIter);

    DataSet ds;
    ds.colNames = {"path"};

    std::multimap<Value, const Path*> table;
    for (; lIter->valid(); lIter->next()) {
        auto& path = lIter->getColumn("path");
        auto dst = path.getPath().steps.back().dst.vid;
        table.emplace(dst, &path.getPath());
    }

    if (rHist.size() >= 2) {
        auto previous = rHist[rHist.size() - 2].iter();
        if (findPath(previous.get(), table, ds)) {
            return finish(ResultBuilder().value(Value(std::move(ds))).finish());
        }
    }

    auto latest = rHist.back().iter();
    findPath(latest.get(), table, ds);
    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}

bool ConjunctPathExecutor::findPath(Iterator* iter,
                                    std::multimap<Value, const Path*>& table,
                                    DataSet& ds) {
    bool found = false;
    for (; iter->valid(); iter->next()) {
        auto& path = iter->getColumn("path");
        auto dst = path.getPath().steps.back().dst.vid;
        auto paths = table.equal_range(dst);
        if (paths.first != paths.second) {
            for (auto i = paths.first; i != paths.second; ++i) {
                Row row;
                Path forward = path.getPath();
                auto backward = *i->second;
                backward.reverse();
                forward.append(std::move(backward));
                row.values.emplace_back(std::move(forward));
                ds.rows.emplace_back(std::move(row));
            }
            found = true;
        }
    }
    return found;
}
}  // namespace graph
}  // namespace nebula
