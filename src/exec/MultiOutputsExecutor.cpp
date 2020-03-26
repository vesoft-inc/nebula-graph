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
        if (val.get_iVal() > iterCount) {
            hasBeenRun_ = false;
            iterCount++;
        }
    }

    if (!hasBeenRun_) {
        hasBeenRun_ = true;
        return input_->execute().then(cb([this](Status s) {
            Status copy = s;
            sharedPromise_.setValue(std::move(copy));
            return s;
        }));
    }

    return sharedPromise_.getFuture();
}

}   // namespace graph
}   // namespace nebula
