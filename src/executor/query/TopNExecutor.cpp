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
    comparator_ = [&factors] (const LogicalRow &lhs, const LogicalRow &rhs) {
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

    offset_ = topn->offset();
    auto count = topn->count();
    auto size = iter->size();
    maxCount_ = count;
    heapSize_ = 0;
    if (size <= static_cast<size_t>(offset_)) {
        maxCount_ = 0;
    } else if (size > static_cast<size_t>(offset_ + count)) {
        heapSize_ = offset_ + count;
    } else {
        maxCount_ = size - offset_;
        heapSize_ = size;
    }
    if (heapSize_ == 0) {
        iter->clear();
        return finish(ResultBuilder().value(iter->valuePtr()).iter(std::move(iter)).finish());
    }

    if (iter->isSequentialIter()) {
        executeTopN<SequentialIter::SeqLogicalRow, SequentialIter>(iter.get());
    } else if (iter->isJoinIter()) {
        executeTopN<JoinIter::JoinLogicalRow, JoinIter>(iter.get());
    } else if (iter->isPropIter()) {
        executeTopN<PropIter::PropLogicalRow, PropIter>(iter.get());
    }
    iter->eraseRange(maxCount_, size);
    return finish(ResultBuilder().value(iter->valuePtr()).iter(std::move(iter)).finish());
}

template<typename T, typename U>
void TopNExecutor::executeTopN(Iterator *pIter) {
    auto iter = static_cast<U*>(pIter);
    std::vector<T> heap(iter->begin(), iter->begin()+heapSize_);
    std::make_heap(heap.begin(), heap.end(), comparator_);
    auto it = iter->begin() + heapSize_;
    while (it != iter->end()) {
        if (comparator_(*it, heap[0])) {
            std::pop_heap(heap.begin(), heap.end(), comparator_);
            heap.pop_back();
            heap.push_back(*it);
            std::push_heap(heap.begin(), heap.end(), comparator_);
        }
        ++it;
    }
    std::sort_heap(heap.begin(), heap.end(), comparator_);

    auto beg = iter->begin();
    for (int i = 0; i < maxCount_; ++i) {
        beg[i] = heap[offset_+i];
    }
}

}   // namespace graph
}   // namespace nebula
