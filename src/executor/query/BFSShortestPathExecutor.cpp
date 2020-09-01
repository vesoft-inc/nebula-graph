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
    DCHECK(!!iter);

    DataSet ds;
    ds.colNames = {"dst", "path"};
    for (; iter->valid(); iter->next()) {
        auto dst = iter->getEdgeProp("*", kDst);
        auto notVisited = visited_.emplace(dst);
        if (!notVisited.second) {
            continue;
        }

        auto src = iter->getEdgeProp("*", kSrc);
        auto type = iter->getEdgeProp("*", kType);
        auto rank = iter->getEdgeProp("*", kRank);
        auto pathRange = path_.equal_range(src);
        if (pathRange.first == pathRange.second) {
            Path path;
            path.src = Vertex(src.getStr(), {});
            path.steps.emplace_back(
                Step(Vertex(dst.getStr(), {}), type.getInt(), "", rank.getInt(), {}));
            Row row;
            row.values.emplace_back(dst);
            row.values.emplace_back(std::move(path));
            path_.emplace(dst, &ds.rows.back().values.back().getPath());
        } else {
            for (auto i = pathRange.first; i != pathRange.second; ++i) {
                Path path = *i->second;
                path.steps.emplace_back(
                    Step(Vertex(dst.getStr(), {}), type.getInt(), "", rank.getInt(), {}));
                Row row;
                row.values.emplace_back(dst);
                row.values.emplace_back(std::move(path));
                ds.rows.emplace_back(std::move(row));
                path_.emplace(dst, &ds.rows.back().values.back().getPath());
            }
        }
    }
    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}
}  // namespace graph
}  // namespace nebula
