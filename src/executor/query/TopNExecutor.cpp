/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/TopNExecutor.h"
#include "planner/Query.h"
#include "util/ScopedTimer.h"


namespace nebula {
namespace graph {

folly::Future<Status> TopNExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    std::cout << "TopNExecutor executing..." << std::endl;
    auto* topn = asNode<TopN>(node());
    auto iter = ectx_->getResult(topn->inputVar()).iter();
    if (UNLIKELY(iter == nullptr)) {
        return Status::Error("Internal error: nullptr iterator in topn executor");
    }
    if (UNLIKELY(iter->isGetNeighborsIter())) {
        std::string errMsg = "Internal error: TopN executor does not supported GetNeighborsIter";
        LOG(ERROR) << errMsg;
        return Status::Error(errMsg);
    }
    if (iter->isSequentialIter()) {
        std::cout << "isSequentialIter" << std::endl;
        auto seqIter = static_cast<SequentialIter*>(iter.get());
        auto &factors = topn->factors();
        auto &colIndices = seqIter->getColIndices();
        std::vector<std::pair<size_t, OrderFactor::OrderType>> indexes;
        for (auto &factor : factors) {
            auto indexFind = colIndices.find(factor.first);
            if (indexFind == colIndices.end()) {
                LOG(ERROR) << "Column name `" << factor.first
                           << "' does not exist.";
                return Status::Error("Column name `%s' does not exist.",
                                     factor.first.c_str());
            }
            indexes.emplace_back(std::make_pair(indexFind->second, factor.second));
        }
        auto comparator = [&indexes] (const LogicalRow &lhs, const LogicalRow &rhs) {
            for (auto &item : indexes) {
                auto index = item.first;
                auto orderType = item.second;
                std::cout << "lhs[index]: ";
                std::cout << lhs[index];
                std::cout << " rhs[index]: ";
                std::cout << rhs[index] << std::endl;
                if (lhs[index] == rhs[index]) {
                    std::cout << "==" << std::endl;
                    continue;
                }

                if (orderType == OrderFactor::OrderType::ASCEND) {
                    std::cout << (lhs[index] < rhs[index] ? "asc <" : "asc >") << std::endl;
                    return lhs[index] < rhs[index];
                } else if (orderType == OrderFactor::OrderType::DESCEND) {
                    std::cout << (lhs[index] > rhs[index] ? "dsc >" : "dsc <") << std::endl;
                    return lhs[index] > rhs[index];
                }
            }
            return false;
        };

        auto offset = topn->offset();
        auto count = topn->count();
        auto size = seqIter->size();
        int64_t maxCount = count;
        int64_t heapSize = 0;
        if (size <= static_cast<size_t>(offset)) {
            maxCount = 0;
        } else if (size > static_cast<size_t>(offset + count)) {
            heapSize = offset + count;
        } else {
            maxCount = size - offset;
            heapSize = size;
        }
        if (heapSize == 0) {
            seqIter->clear();
            return finish(ResultBuilder().value(iter->valuePtr()).iter(std::move(iter)).finish());
        }
        std::cout << "原始数据：" << std::endl;
        for (auto i = seqIter->begin(); i != seqIter->end(); ++i) {
            std::cout << *i << std::endl;
        }
        std::vector<SequentialIter::SeqLogicalRow> heap(seqIter->begin(),
                                                        seqIter->begin() + heapSize);
        std::make_heap(heap.begin(), heap.end(), comparator);
        auto it = seqIter->begin() + heapSize;
        while (it != seqIter->end()) {
            if (comparator(*it, heap[0])) {
                std::pop_heap(heap.begin(), heap.end(), comparator);
                heap.pop_back();
                heap.push_back(*it);
                std::push_heap(heap.begin(), heap.end(), comparator);
            }
            ++it;
        }
        std::cout << "堆有序数据:" << std::endl;
        for (auto a : heap) {
            std::cout << a << std::endl;
        }
        std::sort_heap(heap.begin(), heap.end(), comparator);
        std::cout << "sort_heap数据： " << std::endl;
        for (auto a : heap) {
            std::cout << a << std::endl;
        }
        std::cout << "=======" << std::endl;
        iter->eraseRange(maxCount, size);
        auto beg = seqIter->begin();
        for (int i = 0; i < maxCount; ++i) {
            beg[i] = heap[offset+i];
        }
    }
    return finish(ResultBuilder().value(iter->valuePtr()).iter(std::move(iter)).finish());
}

}   // namespace graph
}   // namespace nebula
