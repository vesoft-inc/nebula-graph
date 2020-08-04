/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/Query.h"

#include <folly/String.h>

#include "common/interface/gen-cpp2/graph_types.h"
#include "util/ToJson.h"

using folly::stringPrintf;

namespace nebula {
namespace graph {

std::unique_ptr<cpp2::PlanNodeDescription> Explore::explain() const {
    auto desc = SingleInputNode::explain();
    auto description = desc->get_description();
    description->emplace("space", folly::to<std::string>(space_));
    description->emplace("dedup", folly::to<std::string>(dedup_));
    description->emplace("limit", folly::to<std::string>(limit_));
    description->emplace("filter", filter_);
    description->emplace("orderBy", folly::toJson(util::toJson(orderBy_)));
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> GetNeighbors::explain() const {
    auto desc = Explore::explain();
    auto description = desc->get_description();
    description->emplace("src", src_ ? src_->toString() : "");
    description->emplace("edgeTypes", folly::toJson(util::toJson(edgeTypes_)));
    description->emplace("edgeDirection" ,
                         storage::cpp2::_EdgeDirection_VALUES_TO_NAMES.at(edgeDirection_));
    description->emplace("vertexProps",
                         vertexProps_ ? folly::toJson(util::toJson(*vertexProps_)) : "");
    description->emplace("edgeProps", edgeProps_ ? folly::toJson(util::toJson(*edgeProps_)) : "");
    description->emplace("statProps", statProps_ ? folly::toJson(util::toJson(*statProps_)) : "");
    description->emplace("exprs", exprs_ ? folly::toJson(util::toJson(*exprs_)) : "");
    description->emplace("random", folly::to<std::string>(random_));
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> GetVertices::explain() const {
    auto desc = Explore::explain();
    auto description = desc->get_description();
    description->emplace("vertices", folly::toJson(util::toJson(vertices_)));
    description->emplace("src", src_ ? src_->toString() : "");
    description->emplace("props", folly::toJson(util::toJson(props_)));
    description->emplace("exprs", folly::toJson(util::toJson(exprs_)));
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> GetEdges::explain() const {
    auto desc = Explore::explain();
    auto description = desc->get_description();
    description->emplace("edges", folly::toJson(util::toJson(edges_)));
    description->emplace("src", src_ ? src_->toString() : "");
    description->emplace("type", util::toJson(type_));
    description->emplace("ranking", ranking_ ? ranking_->toString() : "");
    description->emplace("dst", dst_ ? dst_->toString() : "");
    description->emplace("props", folly::toJson(util::toJson(props_)));
    description->emplace("exprs", folly::toJson(util::toJson(exprs_)));
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> IndexScan::explain() const {
    auto desc = Explore::explain();
    // TODO
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> Filter::explain() const {
    auto desc = SingleInputNode::explain();
    auto description = desc->get_description();
    description->emplace("condition", condition_ ? condition_->toString() : "");
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> Project::explain() const {
    auto desc = SingleInputNode::explain();
    auto description = desc->get_description();
    description->emplace("columns", cols_ ? cols_->toString() : "");
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> Sort::explain() const {
    auto desc = SingleInputNode::explain();
    auto description = desc->get_description();
    description->emplace("factors", folly::toJson(util::toJson(factors_)));
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> Limit::explain() const {
    auto desc = SingleInputNode::explain();
    auto description = desc->get_description();
    description->emplace("offset", folly::to<std::string>(offset_));
    description->emplace("count", folly::to<std::string>(count_));
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> Aggregate::explain() const {
    auto desc = SingleInputNode::explain();
    auto description = desc->get_description();
    description->emplace("groupKeys", folly::toJson(util::toJson(groupKeys_)));
    folly::dynamic itemArr = folly::dynamic::array();
    for (const auto &item : groupItems_) {
        folly::dynamic itemObj = folly::dynamic::object();
        itemObj.insert("distinct", item.distinct);
        itemObj.insert("funcType", static_cast<uint8_t>(item.func));
        itemObj.insert("expr", item.expr ? item.expr->toString() : "");
        itemArr.push_back(itemObj);
    }
    description->emplace("groupItems", folly::toJson(itemArr));
    return desc;
}
std::unique_ptr<cpp2::PlanNodeDescription> SwitchSpace::explain() const {
    auto desc = SingleInputNode::explain();
    auto description = desc->get_description();
    description->emplace("space", spaceName_);
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> DataCollect::explain() const {
    auto desc = SingleInputNode::explain();
    auto description = desc->get_description();
    description->emplace("vars", folly::toJson(util::toJson(vars_)));
    description->emplace("kind", collectKind_ == CollectKind::kSubgraph ? "subgraph" : "row");
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> DataJoin::explain() const {
    auto desc = SingleInputNode::explain();
    auto description = desc->get_description();
    description->emplace("leftVar", folly::toJson(util::toJson(leftVar_)));
    description->emplace("rightVar", folly::toJson(util::toJson(rightVar_)));
    description->emplace("hashKeys", folly::toJson(util::toJson(hashKeys_)));
    description->emplace("probeKeys", folly::toJson(util::toJson(probeKeys_)));
    return desc;
}

}   // namespace graph
}   // namespace nebula
