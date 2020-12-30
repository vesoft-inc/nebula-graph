/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "context/Iterator.h"

#include "common/datatypes/Vertex.h"
#include "common/datatypes/Edge.h"
#include "common/interface/gen-cpp2/common_types.h"
#include "util/SchemaUtil.h"

namespace std {

bool equal_to<const nebula::graph::LogicalRow*>::operator()(
    const nebula::graph::LogicalRow* lhs,
    const nebula::graph::LogicalRow* rhs) const {
    DCHECK_EQ(lhs->kind(), rhs->kind()) << lhs->kind() << " vs. " << rhs->kind();
    switch (lhs->kind()) {
        case nebula::graph::LogicalRow::Kind::kSequential:
        case nebula::graph::LogicalRow::Kind::kJoin: {
            auto lhsValues = lhs->segments();
            auto rhsValues = rhs->segments();
            if (lhsValues.size() != rhsValues.size()) {
                return false;
            }
            for (size_t i = lhsValues.size(); i < lhsValues.size(); ++i) {
                const auto* l = lhsValues[i];
                const auto* r = rhsValues[i];
                auto equal =
                    l == r ? true : (l != nullptr) && (r != nullptr) && (*l == *r);
                if (!equal) {
                    return false;
                }
            }
            break;
        }
        default:
            LOG(FATAL) << "Not support equal_to for " << lhs->kind();
            return false;
    }
    return true;
}
}  // namespace std

namespace nebula {
namespace graph {
GetNeighborsIter::GetNeighborsIter(std::shared_ptr<Value> value)
    : Iterator(value, Kind::kGetNeighbors) {
    auto status = processList(value);
    if (UNLIKELY(!status.ok())) {
        LOG(ERROR) << status;
        clear();
        return;
    }
    iter_ = logicalRows_.begin();
    valid_ = true;
}

Status GetNeighborsIter::processList(std::shared_ptr<Value> value) {
    if (UNLIKELY(!value->isList())) {
        std::stringstream ss;
        ss << "Value type is not list, type: " << value->type();
        return Status::Error(ss.str());
    }
    size_t idx = 0;
    for (auto& val : value->getList().values) {
        if (UNLIKELY(!val.isDataSet())) {
            return Status::Error("There is a value in list which is not a data set.");
        }
        auto status = makeDataSetIndex(val.getDataSet(), idx++);
        NG_RETURN_IF_ERROR(status);
        dsIndices_.emplace_back(std::move(status).value());
    }
    return Status::OK();
}

StatusOr<GetNeighborsIter::DataSetIndex> GetNeighborsIter::makeDataSetIndex(const DataSet& ds,
                                                                            size_t idx) {
    DataSetIndex dsIndex;
    dsIndex.ds = &ds;
    auto buildResult = buildIndex(&dsIndex);
    NG_RETURN_IF_ERROR(buildResult);
    int64_t edgeStartIndex = std::move(buildResult).value();
    if (edgeStartIndex < 0) {
        for (auto& row : dsIndex.ds->rows) {
            logicalRows_.emplace_back(GetNbrLogicalRow{idx, &row, "", nullptr});
        }
    } else {
        makeLogicalRowByEdge(edgeStartIndex, idx, dsIndex);
    }
    return dsIndex;
}

void GetNeighborsIter::makeLogicalRowByEdge(int64_t edgeStartIndex,
                                            size_t idx,
                                            const DataSetIndex& dsIndex) {
    for (auto& row : dsIndex.ds->rows) {
        auto& cols = row.values;
        for (size_t column = edgeStartIndex; column < cols.size() - 1; ++column) {
            if (!cols[column].isList()) {
                // Ignore the bad value.
                continue;
            }
            for (auto& edge : cols[column].getList().values) {
                if (!edge.isList()) {
                    // Ignore the bad value.
                    continue;
                }
                auto edgeName = dsIndex.tagEdgeNameIndices.find(column);
                DCHECK(edgeName != dsIndex.tagEdgeNameIndices.end());
                logicalRows_.emplace_back(
                    GetNbrLogicalRow{idx, &row, edgeName->second, &edge.getList()});
            }
        }
    }
}

bool checkColumnNames(const std::vector<std::string>& colNames) {
    return colNames.size() < 3 || colNames[0] != nebula::kVid || colNames[1].find("_stats") != 0 ||
           colNames.back().find("_expr") != 0;
}

StatusOr<int64_t> GetNeighborsIter::buildIndex(DataSetIndex* dsIndex) {
    auto& colNames = dsIndex->ds->colNames;
    if (UNLIKELY(checkColumnNames(colNames))) {
        return Status::Error("Bad column names.");
    }
    int64_t edgeStartIndex = -1;
    for (size_t i = 0; i < colNames.size(); ++i) {
        dsIndex->colIndices.emplace(colNames[i], i);
        auto& colName = colNames[i];
        if (colName.find("_tag") == 0) {
            NG_RETURN_IF_ERROR(buildPropIndex(colName, i, false, dsIndex));
        } else if (colName.find("_edge") == 0) {
            NG_RETURN_IF_ERROR(buildPropIndex(colName, i, true, dsIndex));
            if (edgeStartIndex < 0) {
                edgeStartIndex = i;
            }
        } else {
            // It is "_vid", "_stats", "_expr" in this situation.
        }
    }

    return edgeStartIndex;
}

Status GetNeighborsIter::buildPropIndex(const std::string& props,
                                       size_t columnId,
                                       bool isEdge,
                                       DataSetIndex* dsIndex) {
    std::vector<std::string> pieces;
    folly::split(":", props, pieces);
    if (UNLIKELY(pieces.size() < 2)) {
        return Status::Error("Bad column name format: %s", props.c_str());
    }

    PropIndex propIdx;
    // if size == 2, it is the tag defined without props.
    if (pieces.size() > 2) {
        for (size_t i = 2; i < pieces.size(); ++i) {
            propIdx.propIndices.emplace(pieces[i], i - 2);
        }
    }

    propIdx.colIdx = columnId;
    propIdx.propList.resize(pieces.size() - 2);
    std::move(pieces.begin() + 2, pieces.end(), propIdx.propList.begin());
    std::string name = pieces[1];
    if (isEdge) {
        // The first character of the edge name is +/-.
        if (UNLIKELY(name.empty() || (name[0] != '+' && name[0] != '-'))) {
            return Status::Error("Bad edge name: %s", name.c_str());
        }
        dsIndex->tagEdgeNameIndices.emplace(columnId, name);
        dsIndex->edgePropsMap.emplace(name, std::move(propIdx));
    } else {
        dsIndex->tagEdgeNameIndices.emplace(columnId, name);
        dsIndex->tagPropsMap.emplace(name, std::move(propIdx));
    }

    return Status::OK();
}

const Value& GetNeighborsIter::getColumn(const std::string& col) const {
    if (!valid()) {
        return Value::kNullValue;
    }
    auto segment = currentSeg();
    auto& index = dsIndices_[segment].colIndices;
    auto found = index.find(col);
    if (found == index.end()) {
        return Value::kEmpty;
    }
    return iter_->row_->values[found->second];
}

const Value& GetNeighborsIter::getColumn(int32_t index) const {
    return getColumnByIndex(index, iter_);
}

const Value& GetNeighborsIter::getTagProp(const std::string& tag,
                                          const std::string& prop) const {
    if (!valid()) {
        return Value::kNullValue;
    }

    auto segment = currentSeg();
    auto &tagPropIndices = dsIndices_[segment].tagPropsMap;
    auto index = tagPropIndices.find(tag);
    if (index == tagPropIndices.end()) {
        return Value::kEmpty;
    }
    auto propIndex = index->second.propIndices.find(prop);
    if (propIndex == index->second.propIndices.end()) {
        return Value::kEmpty;
    }
    auto colId = index->second.colIdx;
    auto& row = *(iter_->row_);
    DCHECK_GT(row.size(), colId);
    if (!row[colId].isList()) {
        return Value::kNullBadType;
    }
    auto& list = row[colId].getList();
    return list.values[propIndex->second];
}

const Value& GetNeighborsIter::getEdgeProp(const std::string& edge,
                                           const std::string& prop) const {
    if (!valid()) {
        return Value::kNullValue;
    }

    auto currentEdge = currentEdgeName();
    if (edge != "*" &&
            (currentEdge.compare(1, std::string::npos, edge) != 0)) {
        VLOG(1) << "Current edge: " << currentEdgeName() << " Wanted: " << edge;
        return Value::kEmpty;
    }
    auto segment = currentSeg();
    auto index = dsIndices_[segment].edgePropsMap.find(currentEdge);
    if (index == dsIndices_[segment].edgePropsMap.end()) {
        VLOG(1) << "No edge found: " << edge;
        VLOG(1) << "Current edge: " << currentEdge;
        return Value::kEmpty;
    }
    auto propIndex = index->second.propIndices.find(prop);
    if (propIndex == index->second.propIndices.end()) {
        VLOG(1) << "No edge prop found: " << prop;
        return Value::kEmpty;
    }
    auto* list = currentEdgeProps();
    return list->values[propIndex->second];
}

Value GetNeighborsIter::getVertex() const {
    if (!valid()) {
        return Value::kNullValue;
    }

    auto segment = currentSeg();
    auto vidVal = getColumn(nebula::kVid);
    if (!SchemaUtil::isValidVid(vidVal)) {
        return Value::kNullBadType;
    }
    Vertex vertex;
    vertex.vid = vidVal;
    auto& tagPropMap = dsIndices_[segment].tagPropsMap;
    for (auto& tagProp : tagPropMap) {
        auto& row = *(iter_->row_);
        auto& tagPropNameList = tagProp.second.propList;
        auto tagColId = tagProp.second.colIdx;
        if (!row[tagColId].isList()) {
            // Ignore the bad value.
            continue;
        }
        DCHECK_GE(row.size(), tagColId);
        auto& propList = row[tagColId].getList();
        DCHECK_EQ(tagPropNameList.size(), propList.values.size());
        Tag tag;
        tag.name = tagProp.first;
        for (size_t i = 0; i < propList.size(); ++i) {
            tag.props.emplace(tagPropNameList[i], propList[i]);
        }
        vertex.tags.emplace_back(std::move(tag));
    }
    return Value(std::move(vertex));
}

Value GetNeighborsIter::getEdge() const {
    if (!valid()) {
        return Value::kNullValue;
    }

    auto segment = currentSeg();
    Edge edge;
    auto edgeName = currentEdgeName().substr(1, std::string::npos);
    edge.name = edgeName;

    auto type = getEdgeProp(edgeName, kType);
    if (!type.isInt()) {
        return Value::kNullBadType;
    }
    edge.type = type.getInt();

    auto& srcVal = getColumn(kVid);
    if (!SchemaUtil::isValidVid(srcVal)) {
        return Value::kNullBadType;
    }
    edge.src = srcVal;

    auto& dstVal = getEdgeProp(edgeName, kDst);
    if (!SchemaUtil::isValidVid(dstVal)) {
        return Value::kNullBadType;
    }
    edge.dst = dstVal;

    auto& rank = getEdgeProp(edgeName, kRank);
    if (!rank.isInt()) {
        return Value::kNullBadType;
    }
    edge.ranking = rank.getInt();

    auto& edgePropMap = dsIndices_[segment].edgePropsMap;
    auto edgeProp = edgePropMap.find(currentEdgeName());
    if (edgeProp == edgePropMap.end()) {
        return Value::kNullValue;
    }
    auto& edgeNamePropList = edgeProp->second.propList;
    auto& propList = currentEdgeProps()->values;
    DCHECK_EQ(edgeNamePropList.size(), propList.size());
    for (size_t i = 0; i < propList.size(); ++i) {
        auto propName = edgeNamePropList[i];
        if (propName == kSrc || propName == kDst
                || propName == kRank || propName == kType) {
            continue;
        }
        edge.props.emplace(edgeNamePropList[i], propList[i]);
    }
    return Value(std::move(edge));
}

const Value& SequentialIter::getColumn(int32_t index) const {
    return getColumnByIndex(index, iter_);
}

void JoinIter::joinIndex(const Iterator* lhs, const Iterator* rhs) {
    size_t nextSeg = 0;
    if (lhs != nullptr) {
        switch (lhs->kind()) {
            case Iterator::Kind::kSequential: {
                nextSeg = buildIndexFromSeqIter(static_cast<const SequentialIter*>(lhs), 0);
                break;
            }
            case Iterator::Kind::kJoin: {
                nextSeg = buildIndexFromJoinIter(static_cast<const JoinIter*>(lhs), 0);
                break;
            }
            case Iterator::Kind::kProp: {
                nextSeg = buildIndexFromPropIter(static_cast<const PropIter*>(lhs), 0);
                break;
            }
            case Iterator::Kind::kDefault:
            case Iterator::Kind::kGetNeighbors: {
                LOG(FATAL) << "Join Not Support " << lhs->kind();
                break;
            }
        }
    }
    if (rhs == nullptr) {
        return;
    }
    switch (rhs->kind()) {
        case Iterator::Kind::kSequential: {
            buildIndexFromSeqIter(static_cast<const SequentialIter*>(rhs), nextSeg);
            break;
        }
        case Iterator::Kind::kJoin: {
            buildIndexFromJoinIter(static_cast<const JoinIter*>(rhs), nextSeg);
            break;
        }
        case Iterator::Kind::kProp: {
            buildIndexFromPropIter(static_cast<const PropIter*>(rhs), nextSeg);
            break;
        }
        case Iterator::Kind::kDefault:
        case Iterator::Kind::kGetNeighbors: {
            LOG(FATAL) << "Join Not Support " << lhs->kind();
            break;
        }
    }
}

size_t JoinIter::buildIndexFromPropIter(const PropIter* iter, size_t segIdx) {
    auto colIdxStart = colIdxIndices_.size();
    for (auto& col : iter->getColIndices()) {
        DCHECK_LT(col.second + colIdxStart, colNames_.size());
        auto& colName = colNames_[col.second + colIdxStart];
        colIndices_.emplace(colName, std::make_pair(segIdx, col.second));
        colIdxIndices_.emplace(col.second + colIdxStart, std::make_pair(segIdx, col.second));
    }
    return segIdx + 1;
}

size_t JoinIter::buildIndexFromSeqIter(const SequentialIter* iter, size_t segIdx) {
    auto colIdxStart = colIdxIndices_.size();
    for (auto& col : iter->getColIndices()) {
        DCHECK_LT(col.second + colIdxStart, colNames_.size());
        auto& colName = colNames_[col.second + colIdxStart];
        colIndices_.emplace(colName, std::make_pair(segIdx, col.second));
        colIdxIndices_.emplace(col.second + colIdxStart, std::make_pair(segIdx, col.second));
    }
    return segIdx + 1;
}

size_t JoinIter::buildIndexFromJoinIter(const JoinIter* iter, size_t segIdx) {
    auto colIdxStart = colIdxIndices_.size();
    size_t nextSeg = 0;
    if (iter->getColIndices().empty()) {
        return nextSeg;
    }

    for (auto& col : iter->getColIdxIndices()) {
        auto oldSeg = col.second.first;
        size_t newSeg = oldSeg + segIdx;
        if (newSeg > nextSeg) {
            nextSeg = newSeg;
        }
        DCHECK_LT(col.first + colIdxStart, colNames_.size());
        auto& colName = colNames_[col.first + colIdxStart];
        colIndices_.emplace(colName, std::make_pair(newSeg, col.second.second));
        colIdxIndices_.emplace(col.first + colIdxStart, std::make_pair(newSeg, col.second.second));
    }
    return nextSeg + 1;
}

const Value& JoinIter::getColumn(int32_t index) const {
    return getColumnByIndex(index, iter_);
}

PropIter::PropIter(std::shared_ptr<Value> value) : Iterator(value, Kind::kProp) {
    DCHECK(value->isDataSet());
    auto& ds = value->getDataSet();
    auto status = makeDataSetIndex(ds);
    if (UNLIKELY(!status.ok())) {
        LOG(ERROR) << status;
        clear();
        return;
    }
    for (auto& row : ds.rows) {
        rows_.emplace_back(&row);
    }
    iter_ = rows_.begin();
}

Status PropIter::makeDataSetIndex(const DataSet& ds) {
    dsIndex_.ds = &ds;
    auto& colNames = ds.colNames;
    for (size_t i = 0; i < colNames.size(); ++i) {
        dsIndex_.colIndices.emplace(colNames[i], i);
        auto& colName = colNames[i];
        if (colName.find(".") != std::string::npos) {
            NG_RETURN_IF_ERROR(buildPropIndex(colName, i));
        }
    }
    return Status::OK();
}

Status PropIter::buildPropIndex(const std::string& props, size_t columnId) {
    std::vector<std::string> pieces;
    folly::split(".", props, pieces);
    if (UNLIKELY(pieces.size() != 2)) {
        return Status::Error("Bad column name format: %s", props.c_str());
    }
    std::string name = pieces[0];
    auto& propsMap = dsIndex_.propsMap;
    if (propsMap.find(name) != propsMap.end()) {
        propsMap[name].emplace(pieces[1], columnId);
    } else {
        std::unordered_map<std::string, size_t> propIndices;
        propIndices.emplace(pieces[1], columnId);
        propsMap.emplace(name, std::move(propIndices));
    }
    return Status::OK();
}

const Value& PropIter::getColumn(const std::string& col) const {
    if (!valid()) {
        return Value::kNullValue;
    }

    auto& logicalRow = *iter_;
    auto index = dsIndex_.colIndices.find(col);
    if (index == dsIndex_.colIndices.end()) {
        return Value::kNullValue;
    }
    DCHECK_LT(index->second, logicalRow.row_->values.size());
    return logicalRow.row_->values[index->second];
}

const Value& PropIter::getProp(const std::string& name, const std::string& prop) const {
    if (!valid()) {
        return Value::kNullValue;
    }
    auto& row = *(iter_->row_);
    auto& propsMap = dsIndex_.propsMap;
    auto index = propsMap.find(name);
    if (index == propsMap.end()) {
        return Value::kEmpty;
    }

    auto propIndex = index->second.find(prop);
    if (propIndex == index->second.end()) {
        VLOG(1) << "No prop found : " << prop;
        return Value::kNullValue;
    }
    auto colId = propIndex->second;
    DCHECK_GT(row.size(), colId);
    return row[colId];
}

Value PropIter::getVertex() const {
    if (!valid()) {
        return Value::kNullValue;
    }

    auto vidVal = getColumn(nebula::kVid);
    if (!SchemaUtil::isValidVid(vidVal)) {
        return Value::kNullValue;
    }
    Vertex vertex;
    vertex.vid = vidVal;
    auto& tagPropsMap = dsIndex_.propsMap;
    bool isVertexProps = true;
    auto& row = *(iter_->row_);
    for (auto& tagProp : tagPropsMap) {
        for (auto& propIndex : tagProp.second) {
            if (row[propIndex.second].empty()) {
                // Not current vertex's prop
                isVertexProps = false;
                break;
            }
        }
        if (!isVertexProps) {
            isVertexProps = true;
            continue;
        }
        Tag tag;
        tag.name = tagProp.first;
        for (auto& propIndex : tagProp.second) {
            tag.props.emplace(propIndex.first, row[propIndex.second]);
        }
        vertex.tags.emplace_back(std::move(tag));
    }
    return Value(std::move(vertex));
}

Value PropIter::getEdge() const {
    if (!valid()) {
        return Value::kNullValue;
    }
    Edge edge;
    auto& edgePropsMap = dsIndex_.propsMap;
    bool isEdgeProps = true;
    auto row = *(iter_->row_);
    for (auto& edgeProp : edgePropsMap) {
        for (auto& propIndex : edgeProp.second) {
            if (row[propIndex.second].empty()) {
                // Not current edge's prop
                isEdgeProps = false;
                break;
            }
        }
        if (!isEdgeProps) {
            isEdgeProps = true;
            continue;
        }
        auto edgeName = edgeProp.first;
        edge.name = edgeProp.first;

        auto type = getEdgeProp(edgeName, kType);
        if (!type.isInt()) {
            return Value::kNullBadType;
        }
        edge.type = type.getInt();

        auto& srcVal = getEdgeProp(edgeName, kSrc);
        if (!SchemaUtil::isValidVid(srcVal)) {
            return Value::kNullBadType;
        }
        edge.src = srcVal;

        auto& dstVal = getEdgeProp(edgeName, kDst);
        if (!SchemaUtil::isValidVid(dstVal)) {
            return Value::kNullBadType;
        }
        edge.dst = dstVal;

        auto rank = getEdgeProp(edgeName, kRank);
        if (!rank.isInt()) {
            return Value::kNullBadType;
        }
        edge.ranking = rank.getInt();

        for (auto& propIndex : edgeProp.second) {
            if (propIndex.first == kSrc || propIndex.first == kDst ||
                propIndex.first == kType || propIndex.first == kRank) {
                continue;
            }
            edge.props.emplace(propIndex.first, row[propIndex.second]);
        }
        return Value(std::move(edge));
    }
    return Value::kNullValue;
}

List PropIter::getVertices() {
    DCHECK(iter_ == rows_.begin());
    List vertices;
    vertices.values.reserve(size());
    for (; valid(); next()) {
        vertices.values.emplace_back(getVertex());
    }
    reset();
    return vertices;
}

List PropIter::getEdges() {
    DCHECK(iter_ == rows_.begin());
    List edges;
    edges.values.reserve(size());
    for (; valid(); next()) {
        auto edge = getEdge();
        if (edge.isEdge()) {
            const_cast<Edge&>(edge.getEdge()).format();
        }
        edges.values.emplace_back(std::move(edge));
    }
    reset();
    return edges;
}

const Value& PropIter::getColumn(int32_t index) const {
    return getColumnByIndex(index, iter_);
}

std::ostream& operator<<(std::ostream& os, Iterator::Kind kind) {
    switch (kind) {
        case Iterator::Kind::kDefault:
            os << "default";
            break;
        case Iterator::Kind::kSequential:
            os << "sequential";
            break;
        case Iterator::Kind::kGetNeighbors:
            os << "get neighbors";
            break;
        case Iterator::Kind::kJoin:
            os << "join";
            break;
        case Iterator::Kind::kProp:
            os << "Prop";
            break;
    }
    os << " iterator";
    return os;
}

std::ostream& operator<<(std::ostream& os, LogicalRow::Kind kind) {
    switch (kind) {
        case LogicalRow::Kind::kGetNeighbors:
            os << "get neighbors row";
            break;
        case LogicalRow::Kind::kSequential:
            os << "sequential row";
            break;
        case LogicalRow::Kind::kJoin:
            os << "join row";
            break;
        case LogicalRow::Kind::kProp:
            os << "prop row";
            break;
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, const LogicalRow& row) {
    std::stringstream ss;
    size_t cnt = 0;
    for (auto* seg : row.segments()) {
        if (seg == nullptr) {
            ss << "nullptr";
        } else {
            ss << *seg;
        }
        if (cnt < row.size() - 1) {
            ss << ",";
        }
        ++cnt;
    }
    os << ss.str();
    return os;
}
}  // namespace graph
}  // namespace nebula
