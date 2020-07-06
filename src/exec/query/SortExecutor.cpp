/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/SortExecutor.h"
#include "context/ExpressionContextImpl.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

folly::Future<Status> SortExecutor::execute() {
    dumpLog();
    auto* sort = asNode<Sort>(node());
    auto iter = ectx_->getResult(sort->inputVar()).iter();
    if (iter == nullptr) {
        return Status::Error("Internal error: nullptr iterator in sort executor");
    }
    if (iter->isGetNeighborsIter() || iter->isUnionIter()) {
        std::string errMsg = "Internal error: "
                             "Sort executor does not supported GetNeighborsIter and UnionIter";
        LOG(ERROR) << errMsg;
        return Status::Error(errMsg);
    }
    if (iter->isSequentialIter()) {
        auto seqIter = static_cast<SequentialIter*>(iter.get());
        auto &factors = sort->factors();
        auto &colIndexes = seqIter->getColIndexes();
        std::vector<std::pair<size_t, OrderFactor::OrderType>> indexes;
        for (auto &factor : factors) {
            auto indexFind = colIndexes.find(factor.first);
            if (indexFind == colIndexes.end()) {
                LOG(ERROR) << "Column name `" << factor.first << "' is not exist.";
                return Status::Error("Column name `%s' is not exist.", factor.first.c_str());
            }
            indexes.emplace_back(std::make_pair(indexFind->second, factor.second));
        }
        auto comparator = [&indexes] (RowRef &lhs, RowRef &rhs) {
            const auto &lhsColumns = lhs.row->values;
            const auto &rhsColumns = rhs.row->values;
            for (auto &item : indexes) {
                auto index = item.first;
                auto orderType = item.second;
                if (lhsColumns[index] == rhsColumns[index]) {
                    continue;
                }

                if (orderType == OrderFactor::OrderType::ASCEND) {
                    return lhsColumns[index] < rhsColumns[index];
                } else if (orderType == OrderFactor::OrderType::DESCEND) {
                    return lhsColumns[index] > rhsColumns[index];
                }
            }
            return false;
        };
        std::sort(seqIter->begin(), seqIter->end(), comparator);
    }
    auto result = ExecResult::buildDefault(iter->valuePtr());
    ExpressionContextImpl ctx(ectx_, iter.get());
    result.setIter(std::move(iter));
    return finish(std::move(result));
}

}   // namespace graph
}   // namespace nebula
