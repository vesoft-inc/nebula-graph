/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/Maintain.h"

#include <sstream>

#include "common/interface/gen-cpp2/graph_types.h"
#include "util/ToJson.h"

namespace nebula {
namespace graph {

std::unique_ptr<cpp2::PlanNodeDescription> CreateSchemaNode::explain() const {
    auto desc = SingleInputNode::explain();
    auto description = desc->get_description();
    description->emplace("name", name_);
    description->emplace("ifNotExists", folly::to<std::string>(ifNotExists_));
    description->emplace("schema", folly::toJson(util::toJson(schema_)));
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> AlterSchemaNode::explain() const {
    auto desc = SingleInputNode::explain();
    auto description = desc->get_description();
    description->emplace("space", folly::to<std::string>(space_));
    description->emplace("name", name_);
    description->emplace("schemaItems", folly::toJson(util::toJson(schemaItems_)));
    description->emplace("schemaProp", folly::toJson(util::toJson(schemaProp_)));
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> DescSchema::explain() const {
    auto desc = SingleInputNode::explain();
    desc->get_description()->emplace("name", name_);
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> DropSchema::explain() const {
    auto desc = SingleInputNode::explain();
    desc->get_description()->emplace("name", name_);
    desc->get_description()->emplace("ifExists", folly::to<std::string>(ifExists_));
    return desc;
}

}   // namespace graph
}   // namespace nebula
