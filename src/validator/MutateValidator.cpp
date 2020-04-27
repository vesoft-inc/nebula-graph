/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "MutateValidator.h"
#include "util/SchemaCommon.h"

namespace nebula {
namespace graph {
Status InsertVerticesValidator::validateImpl() {
    auto status = Status::OK();
    do {
        if (!spaceChosen()) {
            status = Status::Error("Please choose a graph space with `USE spaceName' firstly");
            break;
        }

        status = check();
        if (!status.ok()) {
            break;
        }
        status = prepareVertices();
        if (!status.ok()) {
            break;
        }
    } while (false);
    return status;
}

Status InsertVerticesValidator::toPlan() {
    return Status::OK();
}

Status InsertVerticesValidator::check() {
    auto spaceId = validateContext_->whichSpace().id;
    rows_ = sentence_->rows();
    if (rows_.empty()) {
        return Status::Error("VALUES cannot be empty");
    }

    auto tagItems = sentence_->tagItems();
    overwritable_ = sentence_->overwritable();

    schemas_.reserve(tagItems.size());

    for (auto& item : tagItems) {
        auto *tagName = item->tagName();
        auto tagStatus = validateContext_->schemaMng()->toTagID(spaceId, *tagName);
        if (!tagStatus.ok()) {
            LOG(ERROR) << "No schema found for " << *tagName;
            return Status::Error("No schema found for `%s'", tagName->c_str());
        }

        auto tagId = tagStatus.value();
        auto schema = validateContext_->schemaMng()->getTagSchema(spaceId, tagId);
        if (schema == nullptr) {
            LOG(ERROR) << "No schema found for " << *tagName;
            return Status::Error("No schema found for `%s'", tagName->c_str());
        }

        std::vector<std::string> names;
        auto props = item->properties();
        // Check prop name is in schema
        for (auto *it : props) {
            if (schema->getFieldIndex(*it) < 0) {
                LOG(ERROR) << "Unknown column `" << *it << "' in schema";
                return Status::Error("Unknown column `%s' in schema", it->c_str());
            }
            propSize_++;
            names.emplace_back(*it);
        }
        tagPropNames_[tagId] = names;
        schemas_.emplace_back(tagId, schema);
    }
    return Status::OK();
}

Status InsertVerticesValidator::prepareVertices() {
    vertices_.reserve(rows_.size());
    for (auto i = 0u; i < rows_.size(); i++) {
        auto *row = rows_[i];
        if (propSize_ != row->values().size()) {
            return Status::Error("Wrong number of value");
        }
        auto idStatus = SchemaCommon::toVertexID(row->id());
        if (!idStatus.ok()) {
            return idStatus.status();
        }
        auto vertexId = std::move(idStatus).value();
        auto valsRet = SchemaCommon::toValueVec(row->values());
        if (valsRet.ok()) {
            return valsRet.status();
        }
        auto values = std::move(valsRet).value();

        std::vector<storage::cpp2::NewTag> tags(schemas_.size());
        int32_t handleValueNum = 0;
        for (auto count = 0u; count < schemas_.size(); count++) {
            auto tagId = schemas_[count].first;
            auto schema = schemas_[count].second;
            auto &propNames = tagPropNames_[tagId];
            std::vector<Value> props;
            props.reserve(propNames.size());
            for (auto index = 0u; index < propNames.size(); index++) {
                auto schemaType = schema->getFieldType(propNames[index]);
                auto valueStatus = SchemaCommon::toSchemaValue(schemaType, values[handleValueNum]);
                if (!valueStatus.ok()) {
                    return valueStatus.status();
                }
                props.emplace_back(std::move(valueStatus).value());
                handleValueNum++;
            }
            auto &tag = tags[count];
            tag.set_tag_id(tagId);
            tag.set_props(std::move(props));
        }

        storage::cpp2::NewVertex vertex;
        vertex.set_id(vertexId);
        vertex.set_tags(std::move(tags));
        vertices_.emplace_back(std::move(vertex));
    }

    return Status::OK();
}

Status InsertEdgesValidator::validateImpl() {
    auto status = Status::OK();
    do {
        if (!spaceChosen()) {
            status = Status::Error("Please choose a graph space with `USE spaceName' firstly");
            break;
        }

        status = check();
        if (!status.ok()) {
            break;
        }

        status = prepareEdges();
        if (!status.ok()) {
            break;
        }
    } while (false);
    return status;
}

Status InsertEdgesValidator::toPlan() {
    return Status::OK();
}

Status InsertEdgesValidator::check() {
    auto spaceId = validateContext_->whichSpace().id;
    overwritable_ = sentence_->overwritable();
    auto edgeStatus = validateContext_->schemaMng()->toEdgeType(spaceId, *sentence_->edge());
    if (!edgeStatus.ok()) {
        return edgeStatus.status();
    }
    edgeType_ = edgeStatus.value();
    auto props = sentence_->properties();
    rows_ = sentence_->rows();

    schema_ = validateContext_->schemaMng()->getEdgeSchema(spaceId, edgeType_);
    if (schema_ == nullptr) {
        LOG(ERROR) << "No schema found for " << sentence_->edge();
        return Status::Error("No schema found for `%s'", sentence_->edge()->c_str());
    }

    // Check prop name is in schema
    for (auto *it : props) {
        if (schema_->getFieldIndex(*it) < 0) {
            LOG(ERROR) << "Unknown column `" << *it << "' in schema";
            return Status::Error("Unknown column `%s' in schema", it->c_str());
        }
        propNames_.emplace_back(*it);
    }

    return Status::OK();
}

Status InsertEdgesValidator::prepareEdges() {;
    edges_.reserve(rows_.size());
    for (auto i = 0u; i < rows_.size(); i++) {
        auto *row = rows_[i];
        if (propNames_.size() != row->values().size()) {
            return Status::Error("Wrong number of value");
        }
        auto idStatus = SchemaCommon::toVertexID(row->srcid());
        if (!idStatus.ok()) {
            return idStatus.status();
        }
        auto srcId = std::move(idStatus).value();
        idStatus = SchemaCommon::toVertexID(row->dstid());
        if (!idStatus.ok()) {
            return idStatus.status();
        }
        auto dstId = std::move(idStatus).value();

        int64_t rank = row->rank();

        auto valsRet = SchemaCommon::toValueVec(row->values());
        if (valsRet.ok()) {
            return valsRet.status();
        }
        auto values = std::move(valsRet).value();
        std::vector<Value> props;
        for (auto index = 0u; index < propNames_.size(); index++) {
            auto schemaType = schema_->getFieldType(propNames_[index]);
            auto valueStatus = SchemaCommon::toSchemaValue(schemaType, values[index]);
            if (!valueStatus.ok()) {
                return valueStatus.status();
            }
            props.emplace_back(std::move(valueStatus).value());
        }

        storage::cpp2::NewEdge edge;
        edge.key.set_src(srcId);
        edge.key.set_dst(dstId);
        edge.key.set_ranking(rank);
        edge.key.set_edge_type(edgeType_);
        edge.set_props(std::move(props));
        edge.__isset.key = true;
        edge.__isset.props = true;
        edges_.emplace_back(std::move(edge));
    }

    return Status::OK();
}
}  // namespace graph
}  // namespace nebula

