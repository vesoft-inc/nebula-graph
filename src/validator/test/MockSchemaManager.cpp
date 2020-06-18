/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "validator/test/MockSchemaManager.h"

namespace nebula {
namespace graph {

// space: test
// tag:
//     person(name string, age int8)
// edge:
//     like(start timestamp, end timestamp)
void MockSchemaManager::init() {
    spaceNameIds_.emplace("test", 1);
    tagNameIds_.emplace("person", 2);
    tagIdNames_.emplace(2, "person");
    edgeNameIds_.emplace("like", 3);
    edgeIdNames_.emplace(3, "like");

    std::shared_ptr<meta::NebulaSchemaProvider> personSchema(new meta::NebulaSchemaProvider(0));
    personSchema->addField("name", meta::cpp2::PropertyType::STRING);
    personSchema->addField("age", meta::cpp2::PropertyType::INT8);
    Tags tagSchemas;
    tagSchemas.emplace(2, personSchema);
    tagSchemas_.emplace(1, std::move(tagSchemas));

    std::shared_ptr<meta::NebulaSchemaProvider> likeSchema(new meta::NebulaSchemaProvider(0));
    likeSchema->addField("start", meta::cpp2::PropertyType::TIMESTAMP);
    likeSchema->addField("end", meta::cpp2::PropertyType::DATETIME);
    Edges edgeSchemas;
    edgeSchemas.emplace(3, likeSchema);
    edgeSchemas_.emplace(1, std::move(edgeSchemas));
}

std::shared_ptr<const nebula::meta::NebulaSchemaProvider>
MockSchemaManager::getTagSchema(GraphSpaceID space,
                                 TagID tag,
                                 SchemaVer) {
    auto spaceIt = tagSchemas_.find(space);
    if (spaceIt == tagSchemas_.end()) {
        // Not found
        return std::shared_ptr<const nebula::meta::NebulaSchemaProvider>();
    }
    auto tagIt = spaceIt->second.find(tag);
    if (tagIt == spaceIt->second.end()) {
        // Not found
        return std::shared_ptr<const nebula::meta::NebulaSchemaProvider>();
    }
    return tagIt->second;
}

std::shared_ptr<const nebula::meta::NebulaSchemaProvider>
MockSchemaManager::getEdgeSchema(GraphSpaceID space,
                                  EdgeType edge,
                                  SchemaVer) {
    auto spaceIt = edgeSchemas_.find(space);
    if (spaceIt == edgeSchemas_.end()) {
        // Not found
        return std::shared_ptr<const nebula::meta::NebulaSchemaProvider>();
    }
    auto edgeIt = spaceIt->second.find(edge);
    if (edgeIt == spaceIt->second.end()) {
        // Not found
        return std::shared_ptr<const nebula::meta::NebulaSchemaProvider>();
    }

    return edgeIt->second;
}

StatusOr<GraphSpaceID> MockSchemaManager::toGraphSpaceID(folly::StringPiece spaceName) {
    auto findIt = spaceNameIds_.find(spaceName.str());
    if (findIt != spaceNameIds_.end()) {
        return findIt->second;
    }
    return Status::Error("Space `%s' not found", spaceName.str().c_str());
}

StatusOr<TagID> MockSchemaManager::toTagID(GraphSpaceID space, folly::StringPiece tagName) {
    auto findIt = tagSchemas_.find(space);
    if (findIt == tagSchemas_.end()) {
        return Status::Error("Space `%d' not found", space);
    }
    auto tagFindIt = tagNameIds_.find(tagName.str());
    if (tagFindIt != tagNameIds_.end()) {
        return tagFindIt->second;
    }
    return Status::Error("TagName `%s' not found", tagName.str().c_str());
}

StatusOr<std::string> MockSchemaManager::toTagName(GraphSpaceID space, TagID tagId) {
    auto findIt = tagSchemas_.find(space);
    if (findIt == tagSchemas_.end()) {
        return Status::Error("Space `%d' not found", space);
    }
    auto tagFindIt = tagIdNames_.find(tagId);
    if (tagFindIt != tagIdNames_.end()) {
        return tagFindIt->second;
    }
    return Status::Error("TagID `%d' not found", tagId);
}

StatusOr<EdgeType> MockSchemaManager::toEdgeType(GraphSpaceID space, folly::StringPiece typeName) {
    auto findIt = edgeSchemas_.find(space);
    if (findIt == edgeSchemas_.end()) {
        return Status::Error("Space `%d' not found", space);
    }
    auto edgeFindIt = edgeNameIds_.find(typeName.str());
    if (edgeFindIt != edgeNameIds_.end()) {
        return edgeFindIt->second;
    }
    return Status::Error("EdgeName `%s' not found", typeName.str().c_str());
}

StatusOr<std::string> MockSchemaManager::toEdgeName(GraphSpaceID space, EdgeType edgeType) {
    auto findIt = edgeSchemas_.find(space);
    if (findIt == edgeSchemas_.end()) {
        return Status::Error("Space `%d' not found", space);
    }
    auto edgeFindIt = edgeIdNames_.find(edgeType);
    if (edgeFindIt != edgeIdNames_.end()) {
        return edgeFindIt->second;
    }
    return Status::Error("EdgeType `%d' not found", edgeType);
}

StatusOr<std::vector<std::string>> MockSchemaManager::getAllEdge(GraphSpaceID) {
    std::vector<std::string> edgeNames;
    for (const auto &item : edgeNameIds_) {
        edgeNames.emplace_back(item.first);
    }
    return edgeNames;
}
}  // namespace graph
}  // namespace nebula
