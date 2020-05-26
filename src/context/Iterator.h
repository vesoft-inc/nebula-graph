/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CONTEXT_ITERATOR_H_
#define CONTEXT_ITERATOR_H_

#include "datatypes/Value.h"
#include "datatypes/List.h"
#include "datatypes/DataSet.h"

namespace nebula {
namespace graph {
class Iterator {
public:
    explicit Iterator(const Value& value) : value_(value) {}

    virtual ~Iterator() = default;

    virtual bool hasNext() const = 0;

    virtual Iterator& next() = 0;

    Iterator& operator++() {
        return next();
    }

    virtual const Value& operator*() = 0;

    virtual size_t size() const {
        return 1;
    }

    // The derived class should rewrite get prop if the Value is kind of dataset.
    virtual const Value& getColumn(const std::string& col) const {
        UNUSED(col);
        return kEmpty;
    }

    virtual const Value& getTagProp(const std::string& tag,
                                    const std::string& prop) const {
        UNUSED(tag);
        UNUSED(prop);
        return kEmpty;
    }

    virtual const Value& getEdgeProp(const std::string& edge,
                                     const std::string& prop) const {
        UNUSED(edge);
        UNUSED(prop);
        return kEmpty;
    }

protected:
    const Value& value_;
};

class DefaultIter final : public Iterator {
public:
    explicit DefaultIter(const Value& value) : Iterator(value) {}

    bool hasNext() const override {
        return !(counter_ > 0);
    }

    Iterator& next() override {
        counter_++;
        return *this;
    }

    const Value& operator*() override {
        return value_;
    }

private:
    int64_t counter_{0};
};

class GetNeighborsIter final : public Iterator {
public:
    explicit GetNeighborsIter(const Value& value);

    bool hasNext() const override {
        return iter_ < edges_.end() - 1;
    }

    Iterator& next() override {
        ++iter_;
        return *this;
    }

    const Value& operator*() override {
        return value_;
    }

    const Value& getColumn(const std::string& col) const override {
        auto& current = *iter_;
        auto segment = std::get<0>(current);
        auto& index = colIndex_[segment];
        auto found = index.find(col);
        if (found == index.end()) {
            return kNullValue;
        }
        auto row = std::get<1>(current);
        return row->columns[found->second];
    }

    const Value& getTagProp(const std::string& tag,
                            const std::string& prop) const override {
        auto& current = *iter_;
        auto segment = std::get<0>(current);
        auto index = tagPropIndex_[segment].find(tag);
        if (index == tagPropIndex_[segment].end()) {
            return kNullValue;
        }
        auto propIndex = index->second.find(prop);
        if (propIndex == index->second.end()) {
            return kNullValue;
        }
        auto& list = std::get<3>(current);
        return list->values[propIndex->second];
    }

    const Value& getEdgeProp(const std::string& edge,
                             const std::string& prop) const override {
        auto& current = *iter_;
        auto segment = std::get<0>(current);
        auto index = edgePropIndex_[segment].find(edge);
        if (index == edgePropIndex_[segment].end()) {
            return kNullValue;
        }
        auto propIndex = index->second.find(prop);
        if (propIndex == index->second.end()) {
            return kNullValue;
        }
        auto& list = std::get<3>(current);
        return list->values[propIndex->second];
    }


private:
    int64_t buildIndex(const std::vector<std::string>& colNames);

    std::unordered_map<std::string, int64_t> buildPropIndex(const std::string& props);

private:
    std::vector<std::unordered_map<std::string, int64_t>> colIndex_;
    using PropIndex = std::vector</* segment */
                            std::unordered_map<
                                         std::string, /* tag or edge */
                                         std::unordered_map<std::string, /* prop */
                                                            int64_t      /* id */
                                                           >
                                       >
                                 >;
    using Edge = std::tuple<int64_t, /* segment id */
                           const Row*,
                           int64_t, /* column id */
                           const List* /* edge props */
                          >;
    PropIndex                      tagPropIndex_;
    PropIndex                      edgePropIndex_;
    std::vector<const DataSet*>    segments_;
    std::vector<Edge>              edges_;
    std::vector<Edge>::iterator    iter_;
};
}  // namespace graph
}  // namespace nebula
#endif  // CONTEXT_ITERATOR_H_


