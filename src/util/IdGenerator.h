/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTIL_IDGENERATOR_H_
#define UTIL_IDGENERATOR_H_

#include <atomic>
#include <cstdint>

#include <folly/Singleton.h>

namespace nebula {

class IdGenerator {
public:
    enum class Type {
        SEQUENCE,
        // RANDOM,
    };

    static IdGenerator* get(Type type);

    virtual ~IdGenerator() = default;

    virtual int64_t id() = 0;
    virtual int64_t invalidId() const = 0;
};

class SequenceIdGenerator final : public IdGenerator {
public:
    static SequenceIdGenerator *instance();

    int64_t id() override {
        return ++count_;
    }

    int64_t invalidId() const override {
        return kInvalidSequenceId;
    }

private:
    SequenceIdGenerator() : count_(kInvalidSequenceId) {}

    static constexpr int64_t kInvalidSequenceId = -1;

    std::atomic<int64_t> count_;
};

}   // namespace nebula

#endif   // UTIL_IDGENERATOR_H_
