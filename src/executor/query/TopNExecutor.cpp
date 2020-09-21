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
        auto seqIter = static_cast<SequentialIter*>(iter.get());

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
        std::sort_heap(heap.begin(), heap.end(), comparator);

        auto beg = seqIter->begin();
        for (int i = 0; i < maxCount; ++i) {
            beg[i] = heap[offset+i];
        }
        iter->eraseRange(maxCount, size);
    } else if (iter->isJoinIter()) {
        auto joinIter = static_cast<JoinIter*>(iter.get());

        std::vector<JoinIter::JoinLogicalRow> heap(joinIter->begin(),
                                                   joinIter->begin() + heapSize);
        std::make_heap(heap.begin(), heap.end(), comparator);
        auto it = joinIter->begin() + heapSize;
        while (it != joinIter->end()) {
            if (comparator(*it, heap[0])) {
                std::pop_heap(heap.begin(), heap.end(), comparator);
                heap.pop_back();
                heap.push_back(*it);
                std::push_heap(heap.begin(), heap.end(), comparator);
            }
            ++it;
        }
        std::sort_heap(heap.begin(), heap.end(), comparator);

        auto beg = joinIter->begin();
        for (int i = 0; i < maxCount; ++i) {
            beg[i] = heap[offset+i];
        }
        iter->eraseRange(maxCount, size);
    } else if (iter->isPropIter()) {
        auto propIter = static_cast<PropIter*>(iter.get());

        std::vector<PropIter::PropLogicalRow> heap(propIter->begin(),
                                                   propIter->begin() + heapSize);
        std::make_heap(heap.begin(), heap.end(), comparator);
        auto it = propIter->begin() + heapSize;
        while (it != propIter->end()) {
            if (comparator(*it, heap[0])) {
                std::pop_heap(heap.begin(), heap.end(), comparator);
                heap.pop_back();
                heap.push_back(*it);
                std::push_heap(heap.begin(), heap.end(), comparator);
            }
            ++it;
        }
        std::sort_heap(heap.begin(), heap.end(), comparator);

        auto beg = propIter->begin();
        for (int i = 0; i < maxCount; ++i) {
            beg[i] = heap[offset+i];
        }
        iter->eraseRange(maxCount, size);
    }
    return finish(ResultBuilder().value(iter->valuePtr()).iter(std::move(iter)).finish());
}

}   // namespace graph
}   // namespace nebula
