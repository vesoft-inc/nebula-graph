/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTIL_ASYNCDROPPER_H_
#define UTIL_ASYNCDROPPER_H_

#include <type_traits>

#include <folly/Executor.h>

namespace nebula {
namespace graph {

// drop object async
class AsyncDropper {
public:
    explicit AsyncDropper(folly::Executor *executor) : executor_(executor) {}

    template <typename T>
    void drop(T &&v) {
        static_assert(!std::is_const_v<T>);
        static_assert(!std::is_reference_v<T>);
        executor_->add([_ = std::move(v)](){});
    }

private:
    folly::Executor *executor_;
};

}   // namespace graph
}   // namespace nebula

#endif   // UTIL_ASYNCDROPPER_H_
