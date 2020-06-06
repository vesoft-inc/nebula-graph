/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_AGGREGATEFUNCTION_H
#define PLANNER_AGGREGATEFUNCTION_H

#include "common/base/Base.h"
#include "common/datatypes/List.h"

namespace nebula {
namespace graph {

constexpr char kCount[] = "COUNT";
constexpr char kCountDist[] = "COUNT_DISTINCT";
constexpr char kSum[] = "SUM";
constexpr char kAvg[] = "AVG";
constexpr char kMax[] = "MAX";
constexpr char kMin[] = "MIN";
constexpr char kStd[] = "STD";
constexpr char kBitAnd[] = "BIT_AND";
constexpr char kBitOr[] = "BIT_OR";
constexpr char kBitXor[] = "BIT_XOR";
constexpr char kCollect[] = "COLLECT";

class AggFun {
public:
    AggFun() {}
    virtual ~AggFun() {}

public:
    virtual void apply(Value &val) = 0;
    virtual Value getResult() = 0;
};


class Group final : public AggFun {
public:
    void apply(Value &val) override {
        col_ = val;
    }

    Value getResult() override {
        return col_;
    }

private:
    Value   col_;
};


class Count final : public AggFun {
public:
    void apply(Value &val) override {
        UNUSED(val);
        count_ = count_ + 1;
    }

    Value getResult() override {
        return count_;
    }

private:
    Value   count_{0};
};


class Sum final : public AggFun {
public:
    void apply(Value &val) override {
        if (sum_.type() == Value::Type::__EMPTY__) {
            sum_ = val;
        } else {
            // TODO: Support += for value.
            sum_ = sum_ + val;
        }
    }

    Value getResult() override {
        return sum_;
    }

private:
    Value   sum_;
};


class Avg final : public AggFun {
public:
    void apply(Value &val) override {
        if (sum_.type() == Value::Type::__EMPTY__) {
            sum_ = val;
        } else {
            sum_ = sum_ + val;
        }
        count_ = count_ + 1;
    }

    Value getResult() override {
        return sum_ / count_;
    }

private:
    Value               sum_;
    int64_t            count_{0};
};


class CountDistinct final : public AggFun {
public:
    void apply(Value &val) override {
        valueSet_.emplace(val);
    }

    Value getResult() override {
        int64_t count = static_cast<int64_t>(valueSet_.size());
        return Value(count);
    }

private:
    std::unordered_set<Value> valueSet_;
};


class Max final : public AggFun {
public:
    void apply(Value &val) override {
        if (max_.type() == Value::Type::__EMPTY__ || val > max_) {
            max_ = val;
        }
    }

    Value getResult() override {
        return max_;
    }

private:
    Value     max_;
};


class Min final : public AggFun {
public:
    void apply(Value &val) override {
        if (min_.type() == Value::Type::__EMPTY__ || val < min_) {
            min_ = val;
        }
    }

    Value getResult() override {
        return min_;
    }

private:
    Value       min_;
};


class Stdev final : public AggFun {
public:
    void apply(Value &val) override {
        UNUSED(val);
        // TODO
    }

    Value getResult() override {
        // TODO
        return kEmpty;
    }
};


class BitAnd final : public AggFun {
public:
    void apply(Value &val) override {
        UNUSED(val);
        // TODO
    }

    Value getResult() override {
        // TODO
        return kEmpty;
    }
};


class BitOr final : public AggFun {
public:
    void apply(Value &val) override {
        UNUSED(val);
        // TODO
    }

    Value getResult() override {
        // TODO
        return kEmpty;
    }
};


class BitXor final : public AggFun {
public:
    void apply(Value &val) override {
        UNUSED(val);
        // TODO
    }

    Value getResult() override {
        // TODO
        return kEmpty;
    }
};

class Collect final : public AggFun {
public:
    void apply(Value &val) override {
        list_.values.emplace_back(val);
    }

    Value getResult() override {
        return Value(std::move(list_));
    }

private:
    List    list_;
};

static std::unordered_map<std::string, std::function<std::shared_ptr<AggFun>()>> funVec = {
    { "", []() -> auto { return std::make_shared<Group>();} },
    { kCount, []() -> auto { return std::make_shared<Count>();} },
    { kCountDist, []() -> auto { return std::make_shared<CountDistinct>();} },
    { kSum, []() -> auto { return std::make_shared<Sum>();} },
    { kAvg, []() -> auto { return std::make_shared<Avg>();} },
    { kMax, []() -> auto { return std::make_shared<Max>();} },
    { kMin, []() -> auto { return std::make_shared<Min>();} },
    { kStd, []() -> auto { return std::make_shared<Stdev>();} },
    { kBitAnd, []() -> auto { return std::make_shared<BitAnd>();} },
    { kBitOr, []() -> auto { return std::make_shared<BitOr>();} },
    { kBitXor, []() -> auto { return std::make_shared<BitXor>();} },
    { kCollect, []() -> auto { return std::make_shared<Collect>();} },
};


}  // namespace graph
}  // namespace nebula


#endif  // GRAPH_AGGREGATEFUNCTION_H
