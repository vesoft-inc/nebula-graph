/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/Mutate.h"

#include "common/interface/gen-cpp2/graph_types.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "util/ToJson.h"

namespace nebula {
namespace graph {

std::unique_ptr<cpp2::PlanNodeDescription> InsertVertices::explain() const {
    auto desc = SingleInputNode::explain();
    auto description = desc->get_description();
    description->emplace("spaceId", folly::to<std::string>(spaceId_));
    description->emplace("overwritable", folly::to<std::string>(overwritable_));

    folly::dynamic tagPropsArr = folly::dynamic::array();
    for (const auto &p : tagPropNames_) {
        folly::dynamic obj = folly::dynamic::object();
        obj.insert("tagId", p.first);
        obj.insert("props", util::toJson(p.second));
        tagPropsArr.push_back(obj);
    }
    description->emplace("tagPropNames", folly::toJson(tagPropsArr));
    description->emplace("vertices", folly::toJson(util::toJson(vertices_)));

    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> InsertEdges::explain() const {
    auto desc = SingleInputNode::explain();
    auto description = desc->get_description();
    description->emplace("spaceId", folly::to<std::string>(spaceId_));
    description->emplace("overwritable", folly::to<std::string>(overwritable_));
    description->emplace("propNames", folly::toJson(util::toJson(propNames_)));
    description->emplace("edges", folly::toJson(util::toJson(edges_)));
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> Update::explain() const {
    auto desc = SingleInputNode::explain();
    auto description = desc->get_description();
    description->emplace("spaceId", folly::to<std::string>(spaceId_));
    description->emplace("schemaName", schemaName_);
    description->emplace("insertable", folly::to<std::string>(insertable_));
    description->emplace("updatedProps", folly::toJson(util::toJson(updatedProps_)));
    description->emplace("returnProps", folly::toJson(util::toJson(returnProps_)));
    description->emplace("condition", condition_);
    description->emplace("yieldNames", folly::toJson(util::toJson(yieldNames_)));
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> UpdateVertex::explain() const {
    auto desc = Update::explain();
    auto description = desc->get_description();
    description->emplace("vid", folly::to<std::string>(vId_));
    description->emplace("tagId", folly::to<std::string>(tagId_));
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> UpdateEdge::explain() const {
    auto desc = Update::explain();
    auto description = desc->get_description();
    description->emplace("srcId", srcId_);
    description->emplace("dstId", dstId_);
    description->emplace("rank", folly::to<std::string>(rank_));
    description->emplace("edgeType", folly::to<std::string>(edgeType_));
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> DeleteVertices::explain() const {
    auto desc = SingleInputNode::explain();
    auto description = desc->get_description();
    description->emplace("space", folly::to<std::string>(space_));
    description->emplace("vidRef", vidRef_ ? vidRef_->toString() : "");
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> DeleteEdges::explain() const {
    auto desc = SingleInputNode::explain();
    auto description = desc->get_description();
    description->emplace("space", folly::to<std::string>(space_));
    description->emplace("edgeKeyRefs", folly::toJson(util::toJson(edgeKeyRefs_)));
    return desc;
}

}   // namespace graph
}   // namespace nebula
