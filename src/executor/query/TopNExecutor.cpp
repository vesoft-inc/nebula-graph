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
    auto* topn = asNode<TopN>(node());
    auto iter = ectx_->getResult(topn->inputVar()).iter();
    if (UNLIKELY(iter == nullptr)) {
        return Status::Error("Internal error: nullptr iterator in topn executor");
    }
    if (UNLIKELY(iter->isDefaultIter())) {
        std::string errMsg = "Internal error: Sort executor does not supported DefaultIter";
        LOG(ERROR) << errMsg;
        return Status::Error(errMsg);
    }
    if (UNLIKELY(iter->isGetNeighborsIter())) {
        std::string errMsg = "Internal error: TopN executor does not supported GetNeighborsIter";
        LOG(ERROR) << errMsg;
        return Status::Error(errMsg);
    }

    auto &factors = topn->factors();
    auto comparator = [&factors] (const LogicalRow &lhs, const LogicalRow &rhs) {
        for (auto &item : factors) {
            auto index = item.first;
            auto orderType = item.second;
            if (lhs[index] == rhs[index]) {
                continue;
            }

            if (orderType == OrderFactor::OrderType::ASCEND) {
                return lhs[index] < rhs[index];
            } else if (orderType == OrderFactor::OrderType::DESCEND) {
                return lhs[index] > rhs[index];
            }
        }
        return false;
    };

    auto offset = topn->offset();
    auto count = topn->count();
    auto size = iter->size();
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
        iter->clear();
        return finish(ResultBuilder().value(iter->valuePtr()).iter(std::move(iter)).finish());
    }

    if (iter->isSequentialIter()) {
        TopNHeap<SequentialIter::SeqLogicalRow> topnHeap;
        topnHeap.offset = offset;
        topnHeap.maxCount = maxCount;
        topnHeap.heapSize = heapSize;
        topnHeap.comparator = comparator;
        executeTopN<SequentialIter::SeqLogicalRow, SequentialIter>(iter.get(), topnHeap);
    } else if (iter->isJoinIter()) {
        TopNHeap<JoinIter::JoinLogicalRow> topnHeap;
        topnHeap.offset = offset;
        topnHeap.maxCount = maxCount;
        topnHeap.heapSize = heapSize;
        topnHeap.comparator = comparator;
        executeTopN<JoinIter::JoinLogicalRow, JoinIter>(iter.get(), topnHeap);
    } else if (iter->isPropIter()) {
        TopNHeap<PropIter::PropLogicalRow> topnHeap;
        topnHeap.offset = offset;
        topnHeap.maxCount = maxCount;
        topnHeap.heapSize = heapSize;
        topnHeap.comparator = comparator;
        executeTopN<PropIter::PropLogicalRow, PropIter>(iter.get(), topnHeap);
    }
    iter->eraseRange(maxCount, size);
    return finish(ResultBuilder().value(iter->valuePtr()).iter(std::move(iter)).finish());
}

template<typename T, typename U>
void TopNExecutor::executeTopN(Iterator *pIter, TopNHeap<T> &topnHeap) {
    auto iter = static_cast<U*>(pIter);
    topnHeap.heap = std::vector<T>(iter->begin(),
                        iter->begin() + topnHeap.heapSize);
    std::make_heap(topnHeap.heap.begin(), topnHeap.heap.end(), topnHeap.comparator);
    auto it = iter->begin() + topnHeap.heapSize;
    while (it != iter->end()) {
        if (topnHeap.comparator(*it, topnHeap.heap[0])) {
            std::pop_heap(topnHeap.heap.begin(), topnHeap.heap.end(), topnHeap.comparator);
            topnHeap.heap.pop_back();
            topnHeap.heap.push_back(*it);
            std::push_heap(topnHeap.heap.begin(), topnHeap.heap.end(), topnHeap.comparator);
        }
        ++it;
    }
    std::sort_heap(topnHeap.heap.begin(), topnHeap.heap.end(), topnHeap.comparator);

    auto beg = iter->begin();
    for (int i = 0; i < topnHeap.maxCount; ++i) {
        beg[i] = topnHeap.heap[topnHeap.offset+i];
    }
}

}   // namespace graph
}   // namespace nebula
