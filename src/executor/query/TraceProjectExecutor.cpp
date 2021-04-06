/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/TraceProjectExecutor.h"

#include "planner/Query.h"

namespace nebula {
namespace graph {
folly::Future<Status> TraceProjectExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto trace = asNode<TraceProject>(node());
    auto inputVar = trace->inputVar();
    auto& hist = ectx_->getHistory(inputVar);
    // build the trace map
    std::unordered_map<Value, std::unordered_set<Value>> traceMap;
    auto iter = hist.front().iter();
    for (; iter->valid(); iter->next()) {
        auto& src = iter->getColumn(kVid);
        auto& dst = iter->getEdgeProp("*", kDst);
        traceMap[dst].emplace(src);
    }
    for (size_t i = 0; i < hist.size() - 1; ++i) {
        iter = hist[i].iter();
        for (; iter->valid(); iter->next()) {
            auto& src = iter->getColumn(kVid);
            auto& dst = iter->getEdgeProp("*", kDst);
            auto& startVids = traceMap[src];
            traceMap[dst].insert(startVids.begin(), startVids.end());
        }
    }
    // get the original src vid for last src vid
    auto columns = trace->columns()->columns();
    iter = hist.back().iter();
    DataSet ds;
    ds.colNames = trace->colNames();
    QueryExpressionContext ctx(ectx_);
    for (; iter->valid(); iter->next()) {
        auto& src = iter->getColumn(kVid);
        auto& startVids = traceMap[src];
        ds.rows.reserve(ds.rows.size() + startVids.size());
        std::vector<Value> vals;
        for (auto& col : columns) {
            Value val = col->expr()->eval(ctx(iter.get()));
            vals.emplace_back(std::move(val));
        }
        for (auto& startVid : startVids) {
            Row row;
            row.values.reserve(vals.size() + 1);
            row.values.insert(row.values.end(), vals.begin(), vals.end());
            row.values.emplace_back(startVid);
            ds.rows.emplace_back(std::move(row));
        }
    }

    VLOG(1) << node()->outputVar() << ":" << ds;
    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}
}  // namespace graph
}  // namespace nebula
