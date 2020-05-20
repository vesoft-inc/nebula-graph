/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CONTEXT_EXECUTIONCONTEXT_H_
#define CONTEXT_EXECUTIONCONTEXT_H_

#include "base/Base.h"
#include "datatypes/Value.h"

namespace nebula {
namespace graph {
/***************************************************************************
 *
 * The context for each query request
 *
 * The context is NOT thread-safe. The execution plan has to guarantee
 * all accesses to context are safe
 *
 * The life span of the context is same as the request. That means a new
 * context object will be created as soon as the query engine receives the
 * query request. The context object will be visible to the parser, the
 * planner, the optimizer, and the executor.
 *
 **************************************************************************/

class Iter {
public:
    explicit Iter(const Value& value) : value_(value) {}

    virtual bool hasNext() const = 0;

    virtual Iter& next() = 0;

    Iter& operator++() {
        return next();
    }

    virtual const Value& operator*() = 0;

    // The derived class should rewrite get prop if the Value is kind of dataset.
    virtual const Value& getProp(const std::string& prop) {
        return kEmpty;
    }

protected:
    const Value& value_;
};

class DefaultIter final : public Iter {
public:
    explicit DefaultIter(const Value& value) : Iter(value) {}

    bool hasNext() const override {
        return !(counter_ > 0);
    }

    Iter& next() {
        counter_++;
        return *this;
    }

    const Value& operator*() {
        return value_;
    }

private:
    int64_t counter_{0};
};

class State final {
public:
    enum class Stat : uint8_t {
        UNEXECUTED,
        PARTIAL_SUCCESS,
        SUCCESS
    }

    State(Stat stat, std::string msg) {
        stat_ = stat;
        msg_ = std::move(msg);
    }

private:
    Stat            stat_;
    std::string     msg_;
};

// An executor will produce a result.
class Result final {
public:
    explicit Result(Value&& val) {
        value_ = std::move(val);
        stat_ = State(State::Stat::SUCCESS, "");
        iter_ = std::make_unique<DefaultIter>(value_);
    }

    Result(Value&& val, State stat, std::unique_ptr<Iter> iter) {
        value_ = std::move(val);
        stat_ = stat;
        iter = std::move(iter);
    }

    const Value& value() const {
        return value_;
    }

    Iter& iter() const {
        return *iter_;
    }

private:
    Value                    value_;
    State                    state_;
    std::unique_ptr<Iter>    iter_;
};

class ExecutionContext {
public:
    ExecutionContext() = default;

    virtual ~ExecutionContext() = default;

    // Get the latest version of the value
    const Result& getValue(const std::string& name) const;

    size_t numVersions(const std::string& name) const;

    // Return all existing history of the value. The front is the latest value
    // and the back is the oldest value
    const std::vector<Result>& getHistory(const std::string& name) const;

    void setValue(const std::string& name, Result&& val);

    void deleteValue(const std::string& name);

    // Only keep the last several versoins of the Value
    void truncHistory(const std::string& name, size_t numVersionsToKeep);

private:
    // name -> Value with multiple versions
    std::unordered_map<std::string, std::vector<Result>>     valueMap_;
};
}  // namespace graph
}  // namespace nebula
#endif  // CONTEXT_EXECUTIONCONTEXT_H_


