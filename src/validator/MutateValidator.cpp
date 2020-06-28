/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "validator/MutateValidator.h"
#include "util/SchemaUtil.h"
#include "planner/Mutate.h"
#include "planner/Query.h"

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
<<<<<<< HEAD
    auto* plan = qctx_->plan();
    auto *doNode = InsertVertices::make(plan,
                                        nullptr,
                                        vctx_->whichSpace().id,
                                        vertices_,
                                        tagPropNames_,
                                        overwritable_);
=======
    auto *plan = qctx_->plan();
    InsertVertices* doNode = nullptr;
    if (plan->empty()) {
        auto *start = StartNode::make(plan);
        doNode = InsertVertices::make(plan,
                                      start,
                                      std::move(vertices_),
                                      std::move(tagPropNames_),
                                      overwritable_);
    } else {
        doNode = InsertVertices::make(plan,
                                      plan->root(),
                                      std::move(vertices_),
                                      std::move(tagPropNames_),
                                      overwritable_);
    }
>>>>>>> Support DML,DDL to use inputNode
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status InsertVerticesValidator::check() {
    auto sentence = static_cast<InsertVerticesSentence*>(sentence_);
    rows_ = sentence->rows();
    if (rows_.empty()) {
        return Status::Error("VALUES cannot be empty");
    }

    auto tagItems = sentence->tagItems();
    overwritable_ = sentence->overwritable();

    schemas_.reserve(tagItems.size());

    for (auto& item : tagItems) {
        auto *tagName = item->tagName();
        // Firstly get from the validateContext
        auto schema = vctx_->getSchema(*tagName);
        if (schema == nullptr) {
            // Secondly get from the cache
            auto spaceId = qctx_->rctx()->session()->space();
            if (spaceId < 0) {
                LOG(ERROR) << "Space was not chosen";
                return Status::Error("Space was not chosen");
            }
            schema = qctx_->schemaMng()->getTagSchema(spaceId, *tagName);
            if (schema == nullptr) {
                LOG(ERROR) << "No schema found for " << *tagName;
                return Status::Error("No schema found for `%s'", tagName->c_str());
            }
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
        tagPropNames_[*tagName] = names;
        schemas_.emplace_back(*tagName, schema);
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
        auto idStatus = SchemaUtil::toVertexID(row->id());
        if (!idStatus.ok()) {
            return idStatus.status();
        }
        auto vertexId = std::move(idStatus).value();
        auto valsRet = SchemaUtil::toValueVec(row->values());
        if (!valsRet.ok()) {
            return valsRet.status();
        }
        auto values = std::move(valsRet).value();

        std::vector<storage::cpp2::NewTag> tags(schemas_.size());
        int32_t handleValueNum = 0;
        for (auto count = 0u; count < schemas_.size(); count++) {
            auto name = schemas_[count].first;
            auto schema = schemas_[count].second;
            auto &propNames = tagPropNames_[name];
            std::vector<Value> props;
            props.reserve(propNames.size());
            for (auto index = 0u; index < propNames.size(); index++) {
                auto schemaType = schema->getFieldType(propNames[index]);
                auto valueStatus = SchemaUtil::toSchemaValue(schemaType, values[handleValueNum]);
                if (!valueStatus.ok()) {
                    return valueStatus.status();
                }
                props.emplace_back(std::move(valueStatus).value());
                handleValueNum++;
            }
            auto &tag = tags[count];
            tag.set_tag_name(name);
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
<<<<<<< HEAD
    auto* plan = qctx_->plan();
    auto *doNode = InsertEdges::make(plan,
                                     nullptr,
                                     vctx_->whichSpace().id,
                                     edges_,
                                     propNames_,
                                     overwritable_);
=======
    auto *plan = qctx_->plan();
    InsertEdges* doNode = nullptr;
    if (plan->empty()) {
        auto *start = StartNode::make(plan);
        doNode = InsertEdges::make(plan,
                                   start,
                                   std::move(edges_),
                                   std::move(propNames_),
                                   overwritable_);
    } else {
        doNode = InsertEdges::make(plan,
                                   plan->root(),
                                   std::move(edges_),
                                   std::move(propNames_),
                                   overwritable_);
    }
>>>>>>> Support DML,DDL to use inputNode
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

Status InsertEdgesValidator::check() {
    auto sentence = static_cast<InsertEdgesSentence*>(sentence_);
    overwritable_ = sentence->overwritable();
    edgeName_ = *sentence->edge();

    // Firstly get from the validateContext
    schema_ = vctx_->getSchema(edgeName_);
    if (schema_ == nullptr) {
        // Secondly get from the cache
        auto spaceId = qctx_->rctx()->session()->space();
        if (spaceId < 0) {
            LOG(ERROR) << "Space was not chosen";
            return Status::Error("Space was not chosen");
        }

        schema_ = qctx_->schemaMng()->getEdgeSchema(spaceId, edgeName_);
        if (schema_ == nullptr) {
            LOG(ERROR) << "No schema found for " << edgeName_;
            return Status::Error("No schema found for `%s'", edgeName_.c_str());
        }
    }

    auto props = sentence->properties();
    rows_ = sentence->rows();
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
    edges_.reserve(rows_.size()*2);
    for (auto i = 0u; i < rows_.size(); i++) {
        auto *row = rows_[i];
        if (propNames_.size() != row->values().size()) {
            return Status::Error("Wrong number of value");
        }
        auto idStatus = SchemaUtil::toVertexID(row->srcid());
        if (!idStatus.ok()) {
            return idStatus.status();
        }
        auto srcId = std::move(idStatus).value();
        idStatus = SchemaUtil::toVertexID(row->dstid());
        if (!idStatus.ok()) {
            return idStatus.status();
        }
        auto dstId = std::move(idStatus).value();

        int64_t rank = row->rank();

        auto valsRet = SchemaUtil::toValueVec(row->values());
        if (!valsRet.ok()) {
            return valsRet.status();
        }
        auto values = std::move(valsRet).value();
        std::vector<Value> props;
        for (auto index = 0u; index < propNames_.size(); index++) {
            auto schemaType = schema_->getFieldType(propNames_[index]);
            auto valueStatus = SchemaUtil::toSchemaValue(schemaType, values[index]);
            if (!valueStatus.ok()) {
                return valueStatus.status();
            }
            props.emplace_back(std::move(valueStatus).value());
        }

        // outbound
        storage::cpp2::NewEdge edge;
        edge.set_src(srcId);
        edge.set_dst(dstId);
        edge.set_ranking(rank);
        edge.set_edge_name(edgeName_);
        edge.set_props(std::move(props));
        edge.__isset.props = true;
        edges_.emplace_back(edge);

        // inbound
        edge.set_src(dstId);
        edge.set_dst(srcId);
        edge.set_reversely(true);
        edge.set_edge_name(edgeName_);
        edges_.emplace_back(std::move(edge));
    }

    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
