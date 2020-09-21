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
        executeTopN<SequentialIter, SequentialIter::SeqLogicalRow>(iter.get(),
        offset, maxCount, heapSize, size, comparator);
    } else if (iter->isJoinIter()) {
        executeTopN<JoinIter, JoinIter::JoinLogicalRow>(iter.get(),
        offset, maxCount, heapSize, size, comparator);
    } else if (iter->isPropIter()) {
        executeTopN<PropIter, PropIter::PropLogicalRow>(iter.get(),
        offset, maxCount, heapSize, size, comparator);
    }
    return finish(ResultBuilder().value(iter->valuePtr()).iter(std::move(iter)).finish());
}

template<typename T, typename U>
void TopNExecutor::executeTopN(Iterator *pIter, int64_t offset, int64_t maxCount,
        int64_t heapSize, int64_t size,
        std::function<bool(const LogicalRow&, const LogicalRow&)> comparator) {
    auto iter = static_cast<T*>(pIter);
    std::vector<U> heap(iter->begin(),
                        iter->begin() + heapSize);
    std::make_heap(heap.begin(), heap.end(), comparator);
    auto it = iter->begin() + heapSize;
    while (it != iter->end()) {
        if (comparator(*it, heap[0])) {
            std::pop_heap(heap.begin(), heap.end(), comparator);
            heap.pop_back();
            heap.push_back(*it);
            std::push_heap(heap.begin(), heap.end(), comparator);
        }
        ++it;
    }
    std::sort_heap(heap.begin(), heap.end(), comparator);

    auto beg = iter->begin();
    for (int i = 0; i < maxCount; ++i) {
        beg[i] = heap[offset+i];
    }
    iter->eraseRange(maxCount, size);
}

}   // namespace graph
}   // namespace nebula
