/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTIL_OBJECTPOOL_H_
#define UTIL_OBJECTPOOL_H_

#include <functional>
#include <list>

#include <folly/SpinLock.h>

#include "cpp/helpers.h"

namespace nebula {

class ObjectPool final : private cpp::NonCopyable, private cpp::NonMovable {
public:
    ObjectPool() {}

    ~ObjectPool() = default;

    void clear() {
        folly::SpinLockGuard g(lock_);
        objects_.clear();
    }

    // Take ownership
    template <typename T>
    T *add(T *obj) {
        folly::SpinLockGuard g(lock_);
        objects_.emplace_back(OwnershipHolder(obj));
        return obj;
    }

private:
    // Hold the ownership of any object only
    class OwnershipHolder : private cpp::NonCopyable, private cpp::NonMovable {
    public:
        OwnershipHolder() = delete;
        OwnershipHolder(void*, std::function<void(void*)>) = delete;
        // !Note the parameter obj is the object created by new
        template<typename T>
        explicit OwnershipHolder(T *obj) : obj_(obj),
            deleteFn_([](void *p) { delete reinterpret_cast<T*>(p); }) {}

        ~OwnershipHolder() {
            CHECK_NOTNULL(deleteFn_)(CHECK_NOTNULL(obj_));
        }

    private:
        void *obj_;
        std::function<void(void *)> deleteFn_;
    };

    std::list<OwnershipHolder> objects_;
    folly::SpinLock lock_;
};

}   // namespace nebula

#endif   // UTIL_OBJECTPOOL_H_
