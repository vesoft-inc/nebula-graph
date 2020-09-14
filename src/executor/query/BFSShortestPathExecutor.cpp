/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/BFSShortestPathExecutor.h"

#include "planner/Query.h"

namespace nebula {
namespace graph {
folly::Future<Status> BFSShortestPathExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto* bfs = asNode<BFSShortestPath>(node());
    auto iter = ectx_->getResult(bfs->inputVar()).iter();
    VLOG(1) << "current: " << node()->varName();
    VLOG(1) << "input: " << bfs->inputVar();
    DCHECK(!!iter);

    DataSet ds;
    ds.colNames = node()->colNames();
    std::unordered_map<Value, Value> interim;

    for (; iter->valid(); iter->next()) {
        auto& dst = iter->getEdgeProp("*", kDst);
        auto visited = visited_.find(dst) != visited_.end();
        if (visited) {
            continue;
        }

        auto& src = iter->getColumn(kVid);
        auto& type = iter->getEdgeProp("*", kType);
        auto& rank = iter->getEdgeProp("*", kRank);
        // save the starts.
        visited_.emplace(src);
        Edge e = Edge(src.getStr(), dst.getStr(), type.getInt(), "", rank.getInt(), {});
        VLOG(1) << "dst: " << dst << " edge: " << e;
        interim.emplace(dst, std::move(e));
    }
    for (auto& kv : interim) {
        auto dst = std::move(kv.first);
        auto edge = std::move(kv.second);
        Row row;
        row.values.emplace_back(dst);
        row.values.emplace_back(std::move(edge));
        ds.rows.emplace_back(std::move(row));
        visited_.emplace(dst);
    }
    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}
}  // namespace graph
}  // namespace nebula
