/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CONTEXT_ITERATOR_H_
#define CONTEXT_ITERATOR_H_

#include <memory>

#include <gtest/gtest_prod.h>

#include <boost/dynamic_bitset.hpp>

#include "common/datatypes/Value.h"
#include "common/datatypes/List.h"
#include "common/datatypes/DataSet.h"
#include "parser/TraverseSentences.h"

namespace nebula {
namespace graph {

class LogicalRow {
public:
    enum class Kind : uint8_t {
        kGetNeighbors,
        kSequential,
        kJoin,
        kProp,
    };

    LogicalRow() = default;
    explicit LogicalRow(std::vector<const Row*>&& segments) : segments_(std::move(segments)) {}
    virtual ~LogicalRow() {}
    virtual const Value& operator[](size_t) const = 0;
    virtual size_t size() const = 0;
    virtual Kind kind() const = 0;
    virtual const std::vector<const Row*>& segments() const = 0;

protected:
    std::vector<const Row*> segments_;
};

class Iterator {
public:
    template <typename T>
    using RowsType = std::vector<T>;
    template <typename T>
    using RowsIter = typename RowsType<T>::iterator;

    // Warning this will break the origin order of elements!
    template <typename T>
    static RowsIter<T> eraseBySwap(RowsType<T> &rows, RowsIter<T> i) {
        DCHECK(!rows.empty());
        std::swap(rows.back(), *i);
        rows.pop_back();
        return i;
    }

    enum class Kind : uint8_t {
        kDefault,
        kGetNeighbors,
        kSequential,
        kJoin,
        kProp,
    };

    explicit Iterator(std::shared_ptr<Value> value, Kind kind)
        : value_(value), kind_(kind) {}

    virtual ~Iterator() = default;

    Kind kind() const {
        return kind_;
    }

    virtual std::unique_ptr<Iterator> copy() const = 0;

    virtual bool valid() const = 0;

    virtual void next() = 0;

    // erase current iter
    virtual void erase() = 0;

    // Warning this will break the origin order of elements!
    virtual void unstableErase() = 0;

    virtual const LogicalRow* row() const = 0;

    // erase range, no include last position, if last > size(), erase to the end position
    virtual void eraseRange(size_t first, size_t last) = 0;

    // Reset iterator position to `pos' from begin. Must be sure that the `pos' position
    // is lower than `size()' before resetting
    void reset(size_t pos = 0) {
        doReset(pos);
    }

    virtual void clear() = 0;

    void operator++() {
        next();
    }

    virtual std::shared_ptr<Value> valuePtr() const {
        return value_;
    }

    virtual size_t size() const = 0;

    bool empty() const {
        return size() == 0;
    }

    bool isDefaultIter() const {
        return kind_ == Kind::kDefault;
    }

    bool isGetNeighborsIter() const {
        return kind_ == Kind::kGetNeighbors;
    }

    bool isSequentialIter() const {
        return kind_ == Kind::kSequential;
    }

    bool isJoinIter() const {
        return kind_ == Kind::kJoin;
    }

    bool isPropIter() const {
        return kind_ == Kind::kProp;
    }

    // The derived class should rewrite get prop if the Value is kind of dataset.
    virtual const Value& getColumn(const std::string& col) const = 0;

    virtual const Value& getColumn(int32_t index) const = 0;

    template <typename Iter>
    const Value& getColumnByIndex(int32_t index, Iter iter) const {
        auto size = iter->size();
        if (static_cast<size_t>(std::abs(index)) >= size) {
            return Value::kNullBadType;
        }
        return iter->operator[]((size + index) % size);
    }

    virtual const Value& getTagProp(const std::string&,
                                    const std::string&) const {
        DLOG(FATAL) << "Shouldn't call the unimplemented method";
        return Value::kEmpty;
    }

    virtual const Value& getEdgeProp(const std::string&,
                                     const std::string&) const {
        DLOG(FATAL) << "Shouldn't call the unimplemented method";
        return Value::kEmpty;
    }

    virtual Value getVertex() const {
        return Value();
    }

    virtual Value getEdge() const {
        return Value();
    }

protected:
    virtual void doReset(size_t pos) = 0;

    std::shared_ptr<Value> value_;
    Kind                   kind_;
};

class DefaultIter final : public Iterator {
public:
    explicit DefaultIter(std::shared_ptr<Value> value)
        : Iterator(value, Kind::kDefault) {}

    std::unique_ptr<Iterator> copy() const override {
        return std::make_unique<DefaultIter>(*this);
    }

    bool valid() const override {
        return !(counter_ > 0);
    }

    void next() override {
        counter_++;
    }

    void erase() override {
        counter_--;
    }

    void unstableErase() override {
        DLOG(ERROR) << "Unimplemented default iterator.";
        counter_--;
    }

    void eraseRange(size_t, size_t) override {
        return;
    }

    void clear() override {
        reset();
    }

    size_t size() const override {
        return 1;
    }

    const Value& getColumn(const std::string& /* col */) const override {
        DLOG(FATAL) << "This method should not be invoked";
        return Value::kEmpty;
    }

    const Value& getColumn(int32_t) const override {
        DLOG(FATAL) << "This method should not be invoked";
        return Value::kEmpty;
    }

    const LogicalRow* row() const override {
        DLOG(FATAL) << "This method should not be invoked";
        return nullptr;
    }

private:
    void doReset(size_t pos) override {
        DCHECK((pos == 0 && size() == 0) || (pos < size()));
        counter_ = pos;
    }

    int64_t counter_{0};
};

class GetNeighborsIter final : public Iterator {
public:
    explicit GetNeighborsIter(std::shared_ptr<Value> value);

    std::unique_ptr<Iterator> copy() const override {
        auto copy = std::make_unique<GetNeighborsIter>(*this);
        copy->reset();
        return copy;
    }

    bool valid() const override;

    void next() override;

    void clear() override {
        valid_ = false;
        dsIndices_.clear();
    }

    void erase() override;

    void unstableErase() override {
        erase();
    }

    void eraseRange(size_t first, size_t last) override {
        UNUSED(first);
        UNUSED(last);
        DCHECK(false);
    }

    size_t size() const override {
        return 0;
    }

    const Value& getColumn(const std::string& col) const override;

    const Value& getColumn(int32_t index) const override;

    const Value& getTagProp(const std::string& tag,
                            const std::string& prop) const override;

    const Value& getEdgeProp(const std::string& edge,
                             const std::string& prop) const override;

    Value getVertex() const override;

    Value getEdge() const override;

    // getVertices and getEdges arg batch interface use for subgraph
    // Its unique based on the plan
    List getVertices();

    // Its unique based on the GN interface dedup
    List getEdges();

    const LogicalRow* row() const override {
        DCHECK(false);
        return nullptr;
    }

private:
    void doReset(size_t pos) override {
        UNUSED(pos);
        valid_ = false;
        bitIdx_ = -1;
        goToFirstEdge();
    }

    inline const std::string& currentEdgeName() const {
        DCHECK(currentDs_->tagEdgeNameIndices.find(colIdx_)
                != currentDs_->tagEdgeNameIndices.end());
        return currentDs_->tagEdgeNameIndices.find(colIdx_)->second;
    }

    struct PropIndex {
        size_t colIdx;
        std::vector<std::string> propList;
        std::unordered_map<std::string, size_t> propIndices;
    };

    struct DataSetIndex {
        const DataSet* ds;
        // | _vid | _stats | _tag:t1:p1:p2 | _edge:e1:p1:p2 |
        // -> {_vid : 0, _stats : 1, _tag:t1:p1:p2 : 2, _edge:d1:p1:p2 : 3}
        std::unordered_map<std::string, size_t> colIndices;
        // | _vid | _stats | _tag:t1:p1:p2 | _edge:e1:p1:p2 |
        // -> {2 : t1, 3 : e1}
        std::unordered_map<size_t, std::string> tagEdgeNameIndices;
        // _tag:t1:p1:p2  ->  {t1 : [column_idx, [p1, p2], {p1 : 0, p2 : 1}]}
        std::unordered_map<std::string, PropIndex> tagPropsMap;
        // _edge:e1:p1:p2  ->  {e1 : [column_idx, [p1, p2], {p1 : 0, p2 : 1}]}
        std::unordered_map<std::string, PropIndex> edgePropsMap;

        int64_t colLowerBound{-1};
        int64_t colUpperBound{-1};
    };

    Status processList(std::shared_ptr<Value> value);

    void goToFirstEdge();

    StatusOr<int64_t> buildIndex(DataSetIndex* dsIndex);

    Status buildPropIndex(const std::string& props,
                          size_t columnId,
                          bool isEdge,
                          DataSetIndex* dsIndex);

    StatusOr<DataSetIndex> makeDataSetIndex(const DataSet& ds);

    FRIEND_TEST(IteratorTest, TestHead);

    bool                                 valid_{false};
    std::vector<DataSetIndex>            dsIndices_;

    std::vector<DataSetIndex>::iterator  currentDs_;

    std::vector<Row>::const_iterator     currentRow_;
    std::vector<Row>::const_iterator     rowsUpperBound_;

    int64_t                              colIdx_{-1};
    const List*                          currentCol_{nullptr};

    int64_t                              edgeIdx_{-1};
    int64_t                              edgeIdxUpperBound_{-1};
    const List*                          currentEdge_{nullptr};

    boost::dynamic_bitset<>              bitset_;
    int64_t                              bitIdx_{-1};
};

class SequentialIter final : public Iterator {
public:
    class SeqLogicalRow final : public LogicalRow {
    public:
        explicit SeqLogicalRow(const Row* row) : LogicalRow({row}) {}

        SeqLogicalRow(const SeqLogicalRow &r) = default;
        SeqLogicalRow& operator=(const SeqLogicalRow &r) = default;

        SeqLogicalRow(SeqLogicalRow &&r) noexcept {
            *this = std::move(r);
        }
        SeqLogicalRow& operator=(SeqLogicalRow &&r) noexcept {
            segments_ = std::move(r.segments_);
            return *this;
        }

        const Value& operator[](size_t idx) const override {
            DCHECK_EQ(segments_.size(), 1);
            auto* row = segments_[0];
            if (idx < row->size()) {
                return row->values[idx];
            }
            return Value::kEmpty;
        }

        size_t size() const override {
            DCHECK_EQ(segments_.size(), 1);
            auto* row = segments_[0];
            return row->size();
        }

        LogicalRow::Kind kind() const override {
            return Kind::kSequential;
        }

        const std::vector<const Row*>& segments() const override {
            return segments_;
        }

    private:
        friend class SequentialIter;
    };

    explicit SequentialIter(std::shared_ptr<Value> value);

    // Union multiple sequential iterators
    explicit SequentialIter(std::vector<std::unique_ptr<Iterator>> inputList);

    // Union two sequential iterators.
    SequentialIter(std::unique_ptr<Iterator> left, std::unique_ptr<Iterator> right);

    std::unique_ptr<Iterator> copy() const override {
        auto copy = std::make_unique<SequentialIter>(*this);
        copy->reset();
        return copy;
    }

    bool valid() const override {
        return iter_ < rows_.end();
    }

    void next() override {
        if (valid()) {
            ++iter_;
        }
    }

    void erase() override {
        iter_ = rows_.erase(iter_);
    }

    void unstableErase() override {
        iter_ = eraseBySwap(rows_, iter_);
    }

    void eraseRange(size_t first, size_t last) override {
        if (first >= last || first >= size()) {
            return;
        }
        if (last > size()) {
            rows_.erase(rows_.begin() + first, rows_.end());
        } else {
            rows_.erase(rows_.begin() + first, rows_.begin() + last);
        }
        reset();
    }

    void clear() override {
        rows_.clear();
        reset();
    }

    RowsIter<SeqLogicalRow> begin() {
        return rows_.begin();
    }

    RowsIter<SeqLogicalRow> end() {
        return rows_.end();
    }

    const std::unordered_map<std::string, size_t>& getColIndices() const {
        return colIndices_;
    }

    size_t size() const override {
        return rows_.size();
    }

    const Value& getColumn(const std::string& col) const override {
        if (!valid()) {
            return Value::kNullValue;
        }
        auto logicalRow = *iter_;
        auto index = colIndices_.find(col);
        if (index == colIndices_.end()) {
            return Value::kNullValue;
        }

        DCHECK_EQ(logicalRow.segments_.size(), 1);
        auto* row = logicalRow.segments_[0];
        DCHECK_LT(index->second, row->values.size());
        return row->values[index->second];
    }

    const Value& getColumn(int32_t index) const override;

    // TODO: We should build new iter for get props, the seq iter will
    // not meet the requirements of match any more.
    const Value& getTagProp(const std::string& tag,
                            const std::string& prop) const override {
        return getColumn(tag+ "." + prop);
    }

    const Value& getEdgeProp(const std::string& edge,
                             const std::string& prop) const override {
        return getColumn(edge + "." + prop);
    }

protected:
    const LogicalRow* row() const override {
        if (!valid()) {
            return nullptr;
        }
        return &*iter_;
    }

    // Notice: We only use this interface when return results to client.
    friend class DataCollectExecutor;
    Row&& moveRow() {
        DCHECK_EQ(iter_->segments_.size(), 1);
        auto* row = iter_->segments_[0];
        return std::move(*const_cast<Row*>(row));
    }

private:
    void doReset(size_t pos) override {
        DCHECK((pos == 0 && size() == 0) || (pos < size()));
        iter_ = rows_.begin() + pos;
    }

    void init(std::vector<std::unique_ptr<Iterator>>&& iterators);

    RowsType<SeqLogicalRow>                      rows_;
    RowsIter<SeqLogicalRow>                      iter_;
    std::unordered_map<std::string, size_t>      colIndices_;
};

class PropIter;
class JoinIter final : public Iterator {
public:
    class JoinLogicalRow final : public LogicalRow {
    public:
        explicit JoinLogicalRow(
            std::vector<const Row*> segments,
            size_t size,
            const std::unordered_map<size_t, std::pair<size_t, size_t>>* colIdxIndices)
            : LogicalRow(std::move(segments)), size_(size), colIdxIndices_(colIdxIndices) {}

        JoinLogicalRow(const JoinLogicalRow &r) = default;
        JoinLogicalRow& operator=(const JoinLogicalRow &r) = default;

        JoinLogicalRow(JoinLogicalRow &&r) noexcept {
            *this = std::move(r);
        }

        JoinLogicalRow& operator=(JoinLogicalRow &&r) noexcept {
            segments_ = std::move(r.segments_);

            size_ = r.size_;
            r.size_ = 0;

            colIdxIndices_ = r.colIdxIndices_;
            r.colIdxIndices_ = nullptr;
            return *this;
        }

        const Value& operator[](size_t idx) const override {
            if (idx < size_) {
                auto index = colIdxIndices_->find(idx);
                if (index == colIdxIndices_->end()) {
                    return Value::kNullValue;
                }
                auto keyIdx = index->second.first;
                auto valIdx = index->second.second;
                DCHECK_LT(keyIdx, segments_.size());
                DCHECK_LT(valIdx, segments_[keyIdx]->values.size());
                return segments_[keyIdx]->values[valIdx];
            }
            return Value::kEmpty;
        }

        size_t size() const override {
            return size_;
        }

        LogicalRow::Kind kind() const override {
            return Kind::kJoin;
        }

        const std::vector<const Row*>& segments() const override {
            return segments_;
        }

    private:
        friend class JoinIter;
        size_t size_;
        const std::unordered_map<size_t, std::pair<size_t, size_t>>* colIdxIndices_;
    };

    explicit JoinIter(std::vector<std::string> colNames)
        : Iterator(nullptr, Kind::kJoin), colNames_(std::move(colNames)) {}

    void joinIndex(const Iterator* lhs, const Iterator* rhs);

    void addRow(JoinLogicalRow row) {
        rows_.emplace_back(std::move(row));
        iter_ = rows_.begin();
    }

    std::unique_ptr<Iterator> copy() const override {
        auto copy = std::make_unique<JoinIter>(*this);
        copy->reset();
        return copy;
    }

    std::vector<std::string> colNames() const {
        return colNames_;
    }

    bool valid() const override {
        return iter_ < rows_.end();
    }

    void next() override {
        if (valid()) {
            ++iter_;
        }
    }

    void erase() override {
        iter_ = rows_.erase(iter_);
    }

    void unstableErase() override {
        iter_ = eraseBySwap(rows_, iter_);
    }

    void eraseRange(size_t first, size_t last) override {
        if (first >= last || first >= size()) {
            return;
        }
        if (last > size()) {
            rows_.erase(rows_.begin() + first, rows_.end());
        } else {
            rows_.erase(rows_.begin() + first, rows_.begin() + last);
        }
        reset();
    }

    void clear() override {
        rows_.clear();
        reset();
    }

    RowsIter<JoinLogicalRow> begin() {
        return rows_.begin();
    }

    RowsIter<JoinLogicalRow> end() {
        return rows_.end();
    }

    const std::unordered_map<std::string, std::pair<size_t, size_t>>&
    getColIndices() const {
        return colIndices_;
    }

    const std::unordered_map<size_t, std::pair<size_t, size_t>>&
    getColIdxIndices() const {
        return colIdxIndices_;
    }

    size_t size() const override {
        return rows_.size();
    }

    const Value& getColumn(const std::string& col) const override {
        if (!valid()) {
            return Value::kNullValue;
        }
        auto row = *iter_;
        auto index = colIndices_.find(col);
        if (index == colIndices_.end()) {
            return Value::kNullValue;
        }
        auto segIdx = index->second.first;
        auto colIdx = index->second.second;
        DCHECK_LT(segIdx, row.segments_.size());
        DCHECK_LT(colIdx, row.segments_[segIdx]->values.size());
        return row.segments_[segIdx]->values[colIdx];
    }

    const Value& getColumn(int32_t index) const override;

    const LogicalRow* row() const override {
        if (!valid()) {
            return nullptr;
        }
        return &*iter_;
    }

    void reserve(size_t n) {
        rows_.reserve(n);
    }

private:
    void doReset(size_t pos) override {
        DCHECK((pos == 0 && size() == 0) || (pos < size()));
        iter_ = rows_.begin() + pos;
    }

    size_t buildIndexFromSeqIter(const SequentialIter* iter, size_t segIdx);

    size_t buildIndexFromJoinIter(const JoinIter* iter, size_t segIdx);

    size_t buildIndexFromPropIter(const PropIter* iter, size_t segIdx);

private:
    std::vector<std::string>                                       colNames_;
    RowsType<JoinLogicalRow>                                       rows_;
    RowsIter<JoinLogicalRow>                                       iter_;
    // colName -> segIdx, currentSegColIdx
    std::unordered_map<std::string, std::pair<size_t, size_t>>     colIndices_;
    // colIdx -> segIdx, currentSegColIdx
    std::unordered_map<size_t, std::pair<size_t, size_t>>          colIdxIndices_;
};

class PropIter final : public Iterator {
public:
    class PropLogicalRow final : public LogicalRow {
    public:
        explicit PropLogicalRow(const Row* row) : LogicalRow({row}) {}

        PropLogicalRow(const PropLogicalRow &r) = default;
        PropLogicalRow& operator=(const PropLogicalRow &r) = default;

        PropLogicalRow(PropLogicalRow &&r) noexcept {
            *this = std::move(r);
        }

        PropLogicalRow& operator=(PropLogicalRow &&r) noexcept {
            segments_ = std::move(r.segments_);
            return *this;
        }

        const Value& operator[](size_t idx) const override {
            DCHECK_EQ(segments_.size(), 1);
            auto* row = segments_[0];
            if (idx < row->size()) {
                return row->values[idx];
            }
            return Value::kEmpty;
        }

        size_t size() const override {
            DCHECK_EQ(segments_.size(), 1);
            auto* row = segments_[0];
            return row->size();
        }

        LogicalRow::Kind kind() const override {
            return Kind::kProp;
        }

        const std::vector<const Row*>& segments() const override {
            return segments_;
        }

    private:
        friend class PropIter;
    };

    explicit PropIter(std::shared_ptr<Value> value);

    Status makeDataSetIndex(const DataSet& ds);

    std::unique_ptr<Iterator> copy() const override {
        auto copy = std::make_unique<PropIter>(*this);
        copy->reset();
        return copy;
    }

    bool valid() const override {
        return iter_ < rows_.end();
    }

    void next() override {
        if (valid()) {
            ++iter_;
        }
    }

    void erase() override {
        iter_ = rows_.erase(iter_);
    }

    void unstableErase() override {
        iter_ = eraseBySwap(rows_, iter_);
    }

    void eraseRange(size_t first, size_t last) override {
        if (first >= last || first >= size()) {
            return;
        }
        if (last > size()) {
            rows_.erase(rows_.begin() + first, rows_.end());
        } else {
            rows_.erase(rows_.begin() + first, rows_.begin() + last);
        }
        reset();
    }

    void clear() override {
        rows_.clear();
        reset();
    }

    RowsIter<PropLogicalRow> begin() {
        return rows_.begin();
    }

    RowsIter<PropLogicalRow> end() {
        return rows_.end();
    }

    size_t size() const override {
        return rows_.size();
    }

    const LogicalRow* row() const override {
        if (!valid()) {
            return nullptr;
        }
        return &*iter_;
    }

    const std::unordered_map<std::string, size_t>& getColIndices() const {
        return dsIndex_.colIndices;
    }

    const Value& getColumn(const std::string& col) const override;

    const Value& getColumn(int32_t index) const override;

    Value getVertex() const override;

    Value getEdge() const override;

    List getVertices();

    List getEdges();

    const Value& getProp(const std::string& name, const std::string& prop) const;

    const Value& getTagProp(const std::string& tag, const std::string& prop) const override {
        return getProp(tag, prop);
    }

    const Value& getEdgeProp(const std::string& edge, const std::string& prop) const override {
        return getProp(edge, prop);
    }

    Status buildPropIndex(const std::string& props, size_t columnIdx);

private:
    void doReset(size_t pos) override {
        DCHECK((pos == 0 && size() == 0) || (pos < size()));
        iter_ = rows_.begin() + pos;
    }

    struct DataSetIndex {
        const DataSet* ds;
        // vertex | _vid | tag1.prop1 | tag1.prop2 | tag2,prop1 | tag2,prop2 | ...
        //        |_vid : 0 | tag1.prop1 : 1 | tag1.prop2 : 2 | tag2.prop1 : 3 |...
        // edge   |_src | _type| _ranking | _dst | edge1.prop1 | edge1.prop2 |...
        //        |_src : 0 | _type : 1| _ranking : 2 | _dst : 3| edge1.prop1 : 4|...
        std::unordered_map<std::string, size_t> colIndices;
        // {tag1 : {prop1 : 1, prop2 : 2}
        // {edge1 : {prop1 : 4, prop2 : 5}
        std::unordered_map<std::string, std::unordered_map<std::string, size_t> > propsMap;
    };

private:
    RowsType<PropLogicalRow>                                       rows_;
    RowsIter<PropLogicalRow>                                       iter_;
    DataSetIndex                                                   dsIndex_;
};



std::ostream& operator<<(std::ostream& os, Iterator::Kind kind);
std::ostream& operator<<(std::ostream& os, LogicalRow::Kind kind);
std::ostream& operator<<(std::ostream& os, const LogicalRow& row);

}  // namespace graph
}  // namespace nebula

namespace std {

template <>
struct equal_to<const nebula::Row*> {
    bool operator()(const nebula::Row* lhs, const nebula::Row* rhs) const {
        return lhs == rhs
                   ? true
                   : (lhs != nullptr) && (rhs != nullptr) && (*lhs == *rhs);
    }
};

template <>
struct equal_to<const nebula::graph::LogicalRow*> {
    bool operator()(const nebula::graph::LogicalRow* lhs,
                    const nebula::graph::LogicalRow* rhs) const;
};

template <>
struct hash<const nebula::Row*> {
    size_t operator()(const nebula::Row* row) const {
        return !row ? 0 : hash<nebula::Row>()(*row);
    }
};

template <>
struct hash<nebula::graph::LogicalRow> {
    size_t operator()(const nebula::graph::LogicalRow& row) const {
        size_t seed = 0;
        for (auto& value : row.segments()) {
            seed ^= std::hash<const nebula::Row*>()(value);
        }
        return seed;
    }
};

template <>
struct hash<const nebula::graph::LogicalRow*> {
    size_t operator()(const nebula::graph::LogicalRow* row) const {
        return !row ? 0 : hash<nebula::graph::LogicalRow>()(*row);
    }
};

}  // namespace std

#endif  // CONTEXT_ITERATOR_H_
