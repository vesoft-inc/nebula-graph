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
    std::unordered_map<Value, Value> interimPath;

    for (; iter->valid(); iter->next()) {
        auto dst = iter->getEdgeProp("*", kDst);
        auto visited = !visited_.emplace(dst).second;
        if (visited) {
            continue;
        }

        auto src = iter->getColumn(kVid);
        auto type = iter->getEdgeProp("*", kType);
        auto rank = iter->getEdgeProp("*", kRank);
        auto pathRange = path_.equal_range(src);
        VLOG(1) << src << "->" << dst << "@" << rank;
        if (pathRange.first == pathRange.second) {
            Path path;
            path.src = Vertex(src.getStr(), {});
            path.steps.emplace_back(
                Step(Vertex(dst.getStr(), {}), type.getInt(), "", rank.getInt(), {}));
            VLOG(1) << "dst: " << dst << " path: " << path;
            interimPath.emplace(std::move(dst), std::move(path));
            interimPath.emplace(std::move(src), Path());
        } else {
            for (auto i = pathRange.first; i != pathRange.second; ++i) {
                if (i->second == nullptr) {
                    continue;
                }
                Path path = *i->second;
                path.steps.emplace_back(
                    Step(Vertex(dst.getStr(), {}), type.getInt(), "", rank.getInt(), {}));
                interimPath.emplace(std::move(dst), std::move(path));
                VLOG(1) << "dst: " << dst << " path: " << path;
            }
        }
    }
    for (auto& kv : interimPath) {
        auto dst = std::move(kv.first);
        auto path = std::move(kv.second);
        Row row;
        row.values.emplace_back(dst);
        row.values.emplace_back(std::move(path));
        ds.rows.emplace_back(std::move(row));
        if (ds.rows.back().values.back().isPath()) {
            path_.emplace(dst, &ds.rows.back().values.back().getPath());
        } else {
            path_.emplace(dst, nullptr);
        }
    }
    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}
}  // namespace graph
}  // namespace nebula
