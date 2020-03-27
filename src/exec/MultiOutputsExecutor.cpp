/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/MultiOutputsExecutor.h"

namespace nebula {
namespace graph {

folly::Future<Status> MultiOutputsExecutor::execute() {
    folly::SpinLockGuard g(lock_);

    if (isInLoopBody_) {
        cpp2::Value val = ectx()->getValue(iterVarName_);
        if (val.get_iVal() > iterCount_) {
            hasBeenRun_ = false;
            iterCount_++;
        }
    }

    if (!hasBeenRun_) {
        hasBeenRun_ = true;
        promiseMap_.insert(iterCount_, std::make_shared<folly::SharedPromise<Status>>());
        return input_->execute().then(cb([this, count = iterCount_](Status s) {
            Status copy = s;
            auto iter = promiseMap_.find(count);
            DCHECK(iter != promiseMap_.end());
            iter->second->setValue(std::move(copy));
            return s;
        }));
    }

    auto iter = promiseMap_.find(iterCount_);
    DCHECK(iter != promiseMap_.end());
    return iter->second->getFuture();
}

}   // namespace graph
}   // namespace nebula
