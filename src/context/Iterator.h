/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CONTEXT_ITERATOR_H_
#define CONTEXT_ITERATOR_H_

#include <memory>

#include <gtest/gtest_prod.h>

#include "common/datatypes/DataSet.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Value.h"
#include "parser/TraverseSentences.h"

namespace nebula {
namespace graph {

class Iterator;
class LogicalRow {
public:
    using RowsType = std::vector<std::shared_ptr<LogicalRow>>;
    using RowsIter = typename RowsType::iterator;
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
    virtual const std::vector<const Row*>& segments() const {
        return segments_;
    }

public:
    // The derived class should rewrite get prop if the Value is kind of dataset.
    virtual const Value& getColumn(const std::string& col, Iterator* iter) const = 0;

    virtual const Value& getColumn(int32_t index, Iterator* iter) const = 0;

    virtual const Value& getTagProp(const std::string&, const std::string&, Iterator*) const {
        DLOG(FATAL) << "Shouldn't call the unimplemented method";
        return Value::kEmpty;
    }

    virtual const Value& getEdgeProp(const std::string&, const std::string&, Iterator*) const {
        DLOG(FATAL) << "Shouldn't call the unimplemented method";
        return Value::kEmpty;
    }

    virtual Value getVertex(Iterator*) const {
        return Value();
    }

    virtual Value getEdge(Iterator*) const {
        return Value();
    }

protected:
    const Value& getColumnByIndex(int32_t index, Iterator* iter) const {
        UNUSED(iter);
        auto size = this->size();
        if (static_cast<size_t>(std::abs(index)) >= size) {
            return Value::kNullBadType;
        }
        return this->operator[]((size + index) % size);
    }

protected:
    std::vector<const Row*> segments_;
};

class Iterator {
public:
    using RowsType = std::vector<std::shared_ptr<LogicalRow>>;
    using RowsIter = typename RowsType::iterator;

    // Warning this will break the origin order of elements!
    static RowsIter eraseBySwap(RowsType& rows, RowsIter i) {
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

    explicit Iterator(std::shared_ptr<Value> value, Kind kind) : value_(value), kind_(kind) {}

    virtual ~Iterator() = default;

    Kind kind() const {
        return kind_;
    }

    virtual std::unique_ptr<Iterator> copy() const = 0;

    virtual bool valid(RowsIter it) const = 0;

    virtual RowsIter begin() = 0;

    virtual RowsIter end() = 0;

    // erase current iter
    virtual RowsIter erase(RowsIter it) = 0;

    // Warning this will break the origin order of elements!
    virtual RowsIter unstableErase(RowsIter it) = 0;

    // erase range, no include last position, if last > size(), erase to the end position
    virtual RowsIter eraseRange(size_t first, size_t last) = 0;

    virtual void clear() = 0;

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

protected:
    std::shared_ptr<Value> value_;
    Kind kind_;
};

class DefaultIter final : public Iterator {
public:
    explicit DefaultIter(std::shared_ptr<Value> value) : Iterator(value, Kind::kDefault) {}

    std::unique_ptr<Iterator> copy() const override {
        return std::make_unique<DefaultIter>(*this);
    }

    bool valid(RowsIter) const override {
        return !(counter_ > 0);
    }

    RowsIter begin() override {
        return RowsIter();
    }

    RowsIter end() override {
        return RowsIter();
    }

    RowsIter erase(RowsIter) override {
        counter_--;
        return RowsIter();
    }

    RowsIter unstableErase(RowsIter) override {
        DLOG(ERROR) << "Unimplemented default iterator.";
        counter_--;
        return RowsIter();
    }

    RowsIter eraseRange(size_t, size_t) override {
        return RowsIter();
    }

    void clear() override {
    }

    size_t size() const override {
        return 1;
    }

private:
    int64_t counter_{0};
};

class GetNeighborsIter final : public Iterator {
public:
    explicit GetNeighborsIter(std::shared_ptr<Value> value);

    std::unique_ptr<Iterator> copy() const override {
        auto copy = std::make_unique<GetNeighborsIter>(*this);
        return copy;
    }

    bool valid(RowsIter it) const override {
        return valid_ && it < logicalRows_.end();
    }

    bool noEdgevalid(RowsIter it) const {
        return noEdgeValid_ && it < noEdgeRows_.end();
    }

    void clear() override {
        valid_ = false;
        dsIndices_.clear();
        logicalRows_.clear();
    }

    RowsIter erase(RowsIter it) override {
        if (valid(it)) {
            return logicalRows_.erase(it);
        }
        return logicalRows_.end();
    }

    RowsIter unstableErase(RowsIter it) override {
        if (valid(it)) {
            return eraseBySwap(logicalRows_, it);
        }
        return logicalRows_.end();
    }

    RowsIter eraseRange(size_t first, size_t last) override {
        if (first >= last || first >= size()) {
            return logicalRows_.end();
        }
        if (last > size()) {
            return logicalRows_.erase(logicalRows_.begin() + first, logicalRows_.end());
        } else {
            return logicalRows_.erase(logicalRows_.begin() + first, logicalRows_.begin() + last);
        }
    }

    size_t size() const override {
        return logicalRows_.size();
    }

    RowsIter begin() override {
        return logicalRows_.begin();
    }

    RowsIter end() override {
        return logicalRows_.end();
    }

    Value getNoEdgeVertex(RowsIter noEdgeIter) const;

    // getVertices and getEdges arg batch interface use for subgraph
    // Its unique based on the plan
    List getVertices();

    // Its unique based on the GN interface dedup
    List getEdges();

private:
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
    };

    class GetNbrLogicalRow final : public LogicalRow {
    public:
        GetNbrLogicalRow(size_t dsIdx, const Row* row, std::string edgeName, const List* edgeProps)
            : LogicalRow({row}),
              dsIdx_(dsIdx),
              edgeName_(std::move(edgeName)),
              edgeProps_(edgeProps) {}

        GetNbrLogicalRow(const GetNbrLogicalRow&) = default;
        GetNbrLogicalRow& operator=(const GetNbrLogicalRow&) = default;

        GetNbrLogicalRow(GetNbrLogicalRow&& r) noexcept {
            *this = std::move(r);
        }
        GetNbrLogicalRow& operator=(GetNbrLogicalRow&& r) noexcept {
            dsIdx_ = r.dsIdx_;
            r.dsIdx_ = 0;

            segments_ = std::move(r.segments_);

            edgeName_ = std::move(r.edgeName_);

            edgeProps_ = r.edgeProps_;
            r.edgeProps_ = nullptr;
            return *this;
        }

        const Value& operator[](size_t idx) const override {
            DCHECK_EQ(segments_.size(), 1);
            auto* row = segments_[0];
            if (idx < row->size()) {
                return (*row)[idx];
            }
            return Value::kNullOverflow;
        }

        size_t size() const override {
            DCHECK_EQ(segments_.size(), 1);
            auto* row = segments_[0];
            return row->size();
        }

        LogicalRow::Kind kind() const override {
            return Kind::kGetNeighbors;
        }

    public:
        const Value& getColumn(const std::string& col, Iterator* iter) const override;

        const Value& getColumn(int32_t index, Iterator* iter) const override;

        const Value& getTagProp(const std::string& tag,
                                const std::string& prop,
                                Iterator* iter) const override;

        const Value& getEdgeProp(const std::string& edge,
                                 const std::string& prop,
                                 Iterator* iter) const override;

        Value getVertex(Iterator* iter) const override;

        Value getEdge(Iterator* iter) const override;

    private:
        inline size_t currentSeg() const {
            return dsIdx_;
        }

        inline const std::string& currentEdgeName() const {
            return edgeName_;
        }

        inline const List* currentEdgeProps() const {
            return edgeProps_;
        }

    private:
        friend class GetNeighborsIter;
        size_t dsIdx_;
        std::string edgeName_;
        const List* edgeProps_;
    };

    StatusOr<int64_t> buildIndex(DataSetIndex* dsIndex);
    Status buildPropIndex(const std::string& props,
                          size_t columnId,
                          bool isEdge,
                          DataSetIndex* dsIndex);
    Status processList(std::shared_ptr<Value> value);
    StatusOr<DataSetIndex> makeDataSetIndex(const DataSet& ds, size_t idx);
    void makeLogicalRowByEdge(int64_t edgeStartIndex, size_t idx, const DataSetIndex& dsIndex);

    FRIEND_TEST(IteratorTest, TestHead);

    bool valid_{false};
    RowsType logicalRows_;
    // rows without edges
    bool noEdgeValid_{false};
    RowsType noEdgeRows_;
    std::vector<DataSetIndex> dsIndices_;
};

class SequentialIter final : public Iterator {
public:
    class SeqLogicalRow final : public LogicalRow {
    public:
        explicit SeqLogicalRow(const Row* row) : LogicalRow({row}) {}

        SeqLogicalRow(const SeqLogicalRow& r) = default;
        SeqLogicalRow& operator=(const SeqLogicalRow& r) = default;

        SeqLogicalRow(SeqLogicalRow&& r) noexcept {
            *this = std::move(r);
        }
        SeqLogicalRow& operator=(SeqLogicalRow&& r) noexcept {
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

    public:
        const Value& getColumn(const std::string& col, Iterator* iter) const override {
            DCHECK(iter->isSequentialIter());
            auto seqIter = static_cast<SequentialIter*>(iter);
            auto index = seqIter->colIndices_.find(col);
            if (index == seqIter->colIndices_.end()) {
                return Value::kNullValue;
            }

            DCHECK_EQ(this->segments_.size(), 1);
            auto* row = this->segments_[0];
            DCHECK_LT(index->second, row->values.size());
            return row->values[index->second];
        }

        const Value& getColumn(int32_t index, Iterator* iter) const override;

        // TODO: We should build new iter for get props, the seq iter will
        // not meet the requirements of match any more.
        const Value& getTagProp(const std::string& tag,
                                const std::string& prop,
                                Iterator* iter) const override {
            return getColumn(tag + "." + prop, iter);
        }

        const Value& getEdgeProp(const std::string& edge,
                                 const std::string& prop,
                                 Iterator* iter) const override {
            return getColumn(edge + "." + prop, iter);
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
        return copy;
    }

    bool valid(RowsIter it) const override {
        return it < rows_.end();
    }

    RowsIter erase(RowsIter it) override {
        return rows_.erase(it);
    }

    RowsIter unstableErase(RowsIter it) override {
        return eraseBySwap(rows_, it);
    }

    RowsIter eraseRange(size_t first, size_t last) override {
        if (first >= last || first >= size()) {
            return rows_.end();
        }
        if (last > size()) {
            return rows_.erase(rows_.begin() + first, rows_.end());
        } else {
            return rows_.erase(rows_.begin() + first, rows_.begin() + last);
        }
    }

    void clear() override {
        rows_.clear();
    }

    RowsIter begin() override {
        return rows_.begin();
    }

    RowsIter end() override {
        return rows_.end();
    }

    const std::unordered_map<std::string, size_t>& getColIndices() const {
        return colIndices_;
    }

    size_t size() const override {
        return rows_.size();
    }

protected:
    // Notice: We only use this interface when return results to client.
    friend class DataCollectExecutor;
    // Row&& moveRow() {
    //     DCHECK_EQ(iter_->segments_.size(), 1);
    //     auto* row = iter_->segments_[0];
    //     return std::move(*const_cast<Row*>(row));
    // }

private:
    void init(std::vector<std::unique_ptr<Iterator>>&& iterators);

    RowsType rows_;
    std::unordered_map<std::string, size_t> colIndices_;
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

        JoinLogicalRow(const JoinLogicalRow& r) = default;
        JoinLogicalRow& operator=(const JoinLogicalRow& r) = default;

        JoinLogicalRow(JoinLogicalRow&& r) noexcept {
            *this = std::move(r);
        }

        JoinLogicalRow& operator=(JoinLogicalRow&& r) noexcept {
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

    public:
        const Value& getColumn(const std::string& col, Iterator* iter) const override {
            DCHECK(iter->isJoinIter());
            auto joinIter = static_cast<JoinIter*>(iter);
            auto index = joinIter->colIndices_.find(col);
            if (index == joinIter->colIndices_.end()) {
                return Value::kNullValue;
            }
            auto segIdx = index->second.first;
            auto colIdx = index->second.second;
            DCHECK_LT(segIdx, this->segments_.size());
            DCHECK_LT(colIdx, this->segments_[segIdx]->values.size());
            return this->segments_[segIdx]->values[colIdx];
        }

        const Value& getColumn(int32_t index, Iterator* iter) const override;

    private:
        friend class JoinIter;
        size_t size_;
        const std::unordered_map<size_t, std::pair<size_t, size_t>>* colIdxIndices_;
    };

    explicit JoinIter(std::vector<std::string> colNames)
        : Iterator(nullptr, Kind::kJoin), colNames_(std::move(colNames)) {}

    void joinIndex(const Iterator* lhs, const Iterator* rhs);

    void addRow(JoinLogicalRow* row) {
        rows_.emplace_back(row);
    }

    std::unique_ptr<Iterator> copy() const override {
        auto copy = std::make_unique<JoinIter>(*this);
        return copy;
    }

    std::vector<std::string> colNames() const {
        return colNames_;
    }

    bool valid(RowsIter it) const override {
        return it < rows_.end();
    }

    RowsIter erase(RowsIter it) override {
        return rows_.erase(it);
    }

    RowsIter unstableErase(RowsIter it) override {
        return eraseBySwap(rows_, it);
    }

    RowsIter eraseRange(size_t first, size_t last) override {
        if (first >= last || first >= size()) {
            return rows_.end();
        }
        if (last > size()) {
            return rows_.erase(rows_.begin() + first, rows_.end());
        } else {
            return rows_.erase(rows_.begin() + first, rows_.begin() + last);
        }
    }

    void clear() override {
        rows_.clear();
    }

    RowsIter begin() override {
        return rows_.begin();
    }

    RowsIter end() override {
        return rows_.end();
    }

    const std::unordered_map<std::string, std::pair<size_t, size_t>>& getColIndices() const {
        return colIndices_;
    }

    const std::unordered_map<size_t, std::pair<size_t, size_t>>& getColIdxIndices() const {
        return colIdxIndices_;
    }

    size_t size() const override {
        return rows_.size();
    }

private:
    size_t buildIndexFromSeqIter(const SequentialIter* iter, size_t segIdx);

    size_t buildIndexFromJoinIter(const JoinIter* iter, size_t segIdx);

    size_t buildIndexFromPropIter(const PropIter* iter, size_t segIdx);

private:
    std::vector<std::string> colNames_;
    RowsType rows_;
    // colName -> segIdx, currentSegColIdx
    std::unordered_map<std::string, std::pair<size_t, size_t>> colIndices_;
    // colIdx -> segIdx, currentSegColIdx
    std::unordered_map<size_t, std::pair<size_t, size_t>> colIdxIndices_;
};

class PropIter final : public Iterator {
public:
    class PropLogicalRow final : public LogicalRow {
    public:
        explicit PropLogicalRow(const Row* row) : LogicalRow({row}) {}

        PropLogicalRow(const PropLogicalRow& r) = default;
        PropLogicalRow& operator=(const PropLogicalRow& r) = default;

        PropLogicalRow(PropLogicalRow&& r) noexcept {
            *this = std::move(r);
        }

        PropLogicalRow& operator=(PropLogicalRow&& r) noexcept {
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

        const Value& getColumn(const std::string& col, Iterator* iter) const override;

        const Value& getColumn(int32_t index, Iterator* iter) const override;

        Value getVertex(Iterator* iter) const override;

        Value getEdge(Iterator* iter) const override;

        const Value& getTagProp(const std::string& tag,
                                const std::string& prop,
                                Iterator* iter) const override {
            return getProp(tag, prop, iter);
        }

        const Value& getEdgeProp(const std::string& edge,
                                 const std::string& prop,
                                 Iterator* iter) const override {
            return getProp(edge, prop, iter);
        }

    private:
        const Value& getProp(const std::string& name,
                             const std::string& prop,
                             Iterator* iter) const;

    private:
        friend class PropIter;
    };

    explicit PropIter(std::shared_ptr<Value> value);

    Status makeDataSetIndex(const DataSet& ds);

    std::unique_ptr<Iterator> copy() const override {
        auto copy = std::make_unique<PropIter>(*this);
        return copy;
    }

    bool valid(RowsIter it) const override {
        return it < rows_.end();
    }

    RowsIter erase(RowsIter it) override {
        return rows_.erase(it);
    }

    RowsIter unstableErase(RowsIter it) override {
        return eraseBySwap(rows_, it);
    }

    RowsIter eraseRange(size_t first, size_t last) override {
        if (first >= last || first >= size()) {
            return rows_.end();
        }
        if (last > size()) {
            return rows_.erase(rows_.begin() + first, rows_.end());
        } else {
            return rows_.erase(rows_.begin() + first, rows_.begin() + last);
        }
    }

    void clear() override {
        rows_.clear();
    }

    RowsIter begin() override {
        return rows_.begin();
    }

    RowsIter end() override {
        return rows_.end();
    }

    size_t size() const override {
        return rows_.size();
    }

    const std::unordered_map<std::string, size_t>& getColIndices() const {
        return dsIndex_.colIndices;
    }

    List getVertices();

    List getEdges();

    Status buildPropIndex(const std::string& props, size_t columnIdx);

private:
    struct DataSetIndex {
        const DataSet* ds;
        // vertex | _vid | tag1.prop1 | tag1.prop2 | tag2,prop1 | tag2,prop2 | ...
        //        |_vid : 0 | tag1.prop1 : 1 | tag1.prop2 : 2 | tag2.prop1 : 3 |...
        // edge   |_src | _type| _ranking | _dst | edge1.prop1 | edge1.prop2 |...
        //        |_src : 0 | _type : 1| _ranking : 2 | _dst : 3| edge1.prop1 : 4|...
        std::unordered_map<std::string, size_t> colIndices;
        // {tag1 : {prop1 : 1, prop2 : 2}
        // {edge1 : {prop1 : 4, prop2 : 5}
        std::unordered_map<std::string, std::unordered_map<std::string, size_t>> propsMap;
    };

private:
    RowsType rows_;
    DataSetIndex dsIndex_;
};

std::ostream& operator<<(std::ostream& os, Iterator::Kind kind);
std::ostream& operator<<(std::ostream& os, LogicalRow::Kind kind);
std::ostream& operator<<(std::ostream& os, const LogicalRow& row);

}   // namespace graph
}   // namespace nebula

namespace std {

template <>
struct equal_to<const nebula::Row*> {
    bool operator()(const nebula::Row* lhs, const nebula::Row* rhs) const {
        return lhs == rhs ? true : (lhs != nullptr) && (rhs != nullptr) && (*lhs == *rhs);
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

}   // namespace std

#endif   // CONTEXT_ITERATOR_H_
