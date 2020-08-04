
/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/Admin.h"

#include "common/interface/gen-cpp2/graph_types.h"
#include "util/ToJson.h"

namespace nebula {
namespace graph {

std::unique_ptr<cpp2::PlanNodeDescription> CreateSpace::explain() const {
    auto desc = SingleInputNode::explain();
    desc->get_description()->emplace("ifNotExists", folly::to<std::string>(ifNotExists_));
    desc->get_description()->emplace("spaceDesc", folly::toJson(util::toJson(props_)));
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> DropSpace::explain() const {
    auto desc = SingleInputNode::explain();
    desc->get_description()->emplace("spaceName", spaceName_);
    desc->get_description()->emplace("ifExists", folly::to<std::string>(ifExists_));
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> DescSpace::explain() const {
    auto desc = SingleInputNode::explain();
    desc->get_description()->emplace("spaceName", spaceName_);
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> ShowCreateSpace::explain() const {
    auto desc = SingleInputNode::explain();
    desc->get_description()->emplace("spaceName", spaceName_);
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> DropSnapshot::explain() const {
    auto desc = SingleInputNode::explain();
    desc->get_description()->emplace("snapshotName", snapshotName_);
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> ShowParts::explain() const {
    auto desc = SingleInputNode::explain();
    addDescription("spaceId", folly::to<std::string>(spaceId_), desc.get());
    addDescription("partIds", folly::toJson(util::toJson(partIds_)), desc.get());
    return desc;
}

}   // namespace graph
}   // namespace nebula
