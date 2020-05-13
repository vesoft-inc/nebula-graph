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

// Require emplace_back, reserve and clear for the object pool container
// And only accept one explicit template argument
template <template <typename, typename...> class Container = std::list>
class ObjectPool final : private cpp::NonCopyable, private cpp::NonMovable {
public:
    ObjectPool() {}

    ~ObjectPool() = default;

    void reserve(std::size_t to) {
        objects_.reserve(to);
    }

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
    class OwnershipHolder final : private cpp::NonCopyable {
    public:
        OwnershipHolder() = delete;
        OwnershipHolder(void *, std::function<void(void *)>) = delete;
        // !Note the parameter obj is the object created by new
        template <typename T>
        explicit OwnershipHolder(T *obj)
            : obj_(obj), deleteFn_([](void *p) { delete reinterpret_cast<T *>(p); }) {}

        // Move ownership
        OwnershipHolder(OwnershipHolder &&o) {
            obj_ = o.obj_;
            o.obj_ = nullptr;

            deleteFn_ = o.deleteFn_;
            o.deleteFn_ = nullptr;
        }

        ~OwnershipHolder() {
            if (deleteFn_ && obj_) {
                deleteFn_(obj_);
            }
        }

    private:
        void *obj_;
        std::function<void(void *)> deleteFn_;
    };

    Container<OwnershipHolder> objects_;
    folly::SpinLock lock_;
};

}   // namespace nebula

#endif   // UTIL_OBJECTPOOL_H_
