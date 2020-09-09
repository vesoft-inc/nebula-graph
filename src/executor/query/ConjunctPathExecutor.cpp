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
    auto* conjunct = asNode<ConjunctPath>(node());
    auto lIter = ectx_->getResult(conjunct->leftInputVar()).iter();
    const auto& rHist = ectx_->getHistory(conjunct->rightInputVar());
    VLOG(1) << "current: " << node()->varName();
    VLOG(1) << "left input: " << conjunct->leftInputVar()
            << " right input: " << conjunct->rightInputVar();
    DCHECK(!!lIter);

    DataSet ds;
    ds.colNames = conjunct->colNames();

    std::multimap<Value, const Path*> table;
    for (; lIter->valid(); lIter->next()) {
        auto& dst = lIter->getColumn("_vid");
        auto& path = lIter->getColumn("path");
        if (path.isPath() && !path.getPath().steps.empty()) {
            VLOG(1) << "Forward dst: " << dst;
            table.emplace(dst, &path.getPath());
        }
    }

    if (rHist.size() >= 2) {
        auto previous = rHist[rHist.size() - 2].iter();
        if (findPath(previous.get(), table, ds)) {
            VLOG(1) << "Meet odd length path.";
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
        auto& dst = iter->getColumn("_vid");
        VLOG(1) << "Backward dst: " << dst;
        auto& path = iter->getColumn("path");
        if (path.isPath()) {
            auto paths = table.equal_range(dst);
            if (paths.first != paths.second) {
                for (auto i = paths.first; i != paths.second; ++i) {
                    Row row;
                    auto forward = *i->second;
                    Path backward = path.getPath();
                    VLOG(1) << "Forward path:" << forward;
                    VLOG(1) << "Backward path:" << backward;
                    backward.reverse();
                    VLOG(1) << "Backward reverse path:" << backward;
                    forward.append(std::move(backward));
                    VLOG(1) << "Found path: " << forward;
                    row.values.emplace_back(std::move(forward));
                    ds.rows.emplace_back(std::move(row));
                }
                found = true;
            }
        }
    }
    return found;
}
}  // namespace graph
}  // namespace nebula
