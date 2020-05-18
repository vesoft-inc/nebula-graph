/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/logic/MultiOutputsExecutor.h"

#include "planner/Query.h"
#include "service/ExecutionContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> MultiOutputsExecutor::execute() {
    folly::SpinLockGuard g(lock_);

    if (currentOut_ == 0) {
        currentOut_ = successors_.size();
        sharedPromise_ = std::make_shared<folly::SharedPromise<Status>>();
    }

    if (--currentOut_ > 0) {
        return sharedPromise_->getFuture();
    }

    // return input_->execute().then([this](Status s) {
    //     dumpLog();

    //     sharedPromise_->setValue(s);
    //     return s;
    // });
    return folly::makeFuture(Status::OK());
}

}   // namespace graph
}   // namespace nebula
