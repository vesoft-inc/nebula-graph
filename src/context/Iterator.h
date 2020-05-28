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

    virtual bool valid() const = 0;

    virtual void next() = 0;

    void operator++() {
        next();
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

    bool valid() const override {
        return !(counter_ > 0);
    }

    void next() override {
        counter_++;
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

    bool valid() const override {
        return iter_ < edges_.end();
    }

    void next() override {
        ++iter_;
    }

    const Value& operator*() override {
        return value_;
    }

    const Value& getColumn(const std::string& col) const override;

    const Value& getTagProp(const std::string& tag,
                            const std::string& prop) const override;

    const Value& getEdgeProp(const std::string& edge,
                             const std::string& prop) const override;

private:
    int64_t buildIndex(const std::vector<std::string>& colNames);

    std::pair<std::string, std::unordered_map<std::string, int64_t>>
    buildPropIndex(const std::string& props);

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

class SequentialIter final : public Iterator {
public:
    explicit SequentialIter(const Value& value) : Iterator(value) {
        DCHECK(value.type() == Value::Type::DATASET);
        auto& ds = value.getDataSet();
        rows_ = &ds.rows;
        iter_ = rows_->begin();
        for (size_t i = 0; i < ds.colNames.size(); ++i) {
            colIndex_.emplace(ds.colNames[i], i);
        }
    }

    bool valid() const override {
        return iter_ < rows_->end();
    }

    void next() override {
        ++iter_;
    }

    const Value& operator*() override {
        return value_;
    }

    const Value& getColumn(const std::string& col) const override {
        auto& row = *iter_;
        auto index = colIndex_.find(col);
        if (index == colIndex_.end()) {
            return kNullValue;
        } else {
            DCHECK_LT(index->second, row.columns.size());
            return row.columns[index->second];
        }
    }

private:
    const std::vector<Row>*                     rows_;
    std::vector<Row>::const_iterator            iter_;
    std::unordered_map<std::string, int64_t>    colIndex_;
};
}  // namespace graph
}  // namespace nebula
#endif  // CONTEXT_ITERATOR_H_


