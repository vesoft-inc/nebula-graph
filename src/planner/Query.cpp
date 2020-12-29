/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/Query.h"

#include <folly/String.h>
#include <folly/json.h>

#include "util/ToJson.h"

using folly::stringPrintf;

namespace nebula {
namespace graph {

std::unique_ptr<PlanNodeDescription> Explore::explain() const {
    auto desc = SingleInputNode::explain();
    addDescription("space", folly::to<std::string>(space_), desc.get());
    addDescription("dedup", util::toJson(dedup_), desc.get());
    addDescription("limit", folly::to<std::string>(limit_), desc.get());
    auto filter = filter_.empty() ? filter_ : Expression::decode(filter_)->toString();
    addDescription("filter", filter, desc.get());
    addDescription("orderBy", folly::toJson(util::toJson(orderBy_)), desc.get());
    return desc;
}

GetNeighbors* GetNeighbors::clone(QueryContext* qctx) const {
    auto newGN = GetNeighbors::make(qctx, nullptr, space_);
    newGN->clone(*this);
    return newGN;
}

std::unique_ptr<PlanNodeDescription> GetNeighbors::explain() const {
    auto desc = Explore::explain();
    addDescription("src", src_ ? src_->toString() : "", desc.get());
    addDescription("edgeTypes", folly::toJson(util::toJson(edgeTypes_)), desc.get());
    addDescription("edgeDirection",
                   storage::cpp2::_EdgeDirection_VALUES_TO_NAMES.at(edgeDirection_),
                   desc.get());
    addDescription(
        "vertexProps", vertexProps_ ? folly::toJson(util::toJson(*vertexProps_)) : "", desc.get());
    addDescription(
        "edgeProps", edgeProps_ ? folly::toJson(util::toJson(*edgeProps_)) : "", desc.get());
    addDescription(
        "statProps", statProps_ ? folly::toJson(util::toJson(*statProps_)) : "", desc.get());
    addDescription("exprs", exprs_ ? folly::toJson(util::toJson(*exprs_)) : "", desc.get());
    addDescription("random", util::toJson(random_), desc.get());
    return desc;
}

void GetNeighbors::clone(const GetNeighbors& g) {
    Explore::clone(g);
    setSrc(qctx_->objPool()->add(g.src_->clone().release()));
    setEdgeTypes(g.edgeTypes_);
    setEdgeDirection(g.edgeDirection_);
    setRandom(g.random_);
    if (g.vertexProps_) {
        auto vertexProps = *g.vertexProps_;
        auto vertexPropsPtr = std::make_unique<decltype(vertexProps)>(vertexProps);
        setVertexProps(std::move(vertexPropsPtr));
    }

    if (g.edgeProps_) {
        auto edgeProps = *g.edgeProps_;
        auto edgePropsPtr = std::make_unique<decltype(edgeProps)>(std::move(edgeProps));
        setEdgeProps(std::move(edgePropsPtr));
    }

    if (g.statProps_) {
        auto statProps = *g.statProps_;
        auto statPropsPtr = std::make_unique<decltype(statProps)>(std::move(statProps));
        setStatProps(std::move(statPropsPtr));
    }

    if (g.exprs_) {
        auto exprs = *g.exprs_;
        auto exprsPtr = std::make_unique<decltype(exprs)>(exprs);
        setExprs(std::move(exprsPtr));
    }
}


std::unique_ptr<PlanNodeDescription> GetVertices::explain() const {
    auto desc = Explore::explain();
    addDescription("src", src_ ? src_->toString() : "", desc.get());
    addDescription("props", folly::toJson(util::toJson(props_)), desc.get());
    addDescription("exprs", folly::toJson(util::toJson(exprs_)), desc.get());
    return desc;
}

std::unique_ptr<PlanNodeDescription> GetEdges::explain() const {
    auto desc = Explore::explain();
    addDescription("src", src_ ? src_->toString() : "", desc.get());
    addDescription("type", util::toJson(type_), desc.get());
    addDescription("ranking", ranking_ ? ranking_->toString() : "", desc.get());
    addDescription("dst", dst_ ? dst_->toString() : "", desc.get());
    addDescription("props", folly::toJson(util::toJson(props_)), desc.get());
    addDescription("exprs", folly::toJson(util::toJson(exprs_)), desc.get());
    return desc;
}

IndexScan* IndexScan::clone(QueryContext* qctx) const {
    auto* scan = IndexScan::make(
        qctx, nullptr, space(), nullptr, nullptr, isEdge(), schemaId());
    scan->clone(*this);
    return scan;
}

void IndexScan::clone(const IndexScan &g) {
    Explore::clone(g);
    if (g.contexts_ != nullptr) {
        contexts_ = std::make_unique<std::vector<storage::cpp2::IndexQueryContext>>(*g.contexts_);
    }
    if (g.returnCols_ != nullptr) {
        returnCols_ = std::make_unique<std::vector<std::string>>(*g.returnCols_);
    }
    isEdge_ = g.isEdge_;
    schemaId_ = g.schemaId_;
    isEmptyResultSet_ = g.isEmptyResultSet_;
}

std::unique_ptr<PlanNodeDescription> IndexScan::explain() const {
    auto desc = Explore::explain();
    addDescription("schemaId", util::toJson(schemaId_), desc.get());
    addDescription("isEdge", util::toJson(isEdge_), desc.get());
    addDescription("returnCols", folly::toJson(util::toJson(*returnCols_)), desc.get());
    addDescription("indexCtx", folly::toJson(util::toJson(*contexts_)), desc.get());
    return desc;
}

std::unique_ptr<PlanNodeDescription> Filter::explain() const {
    auto desc = SingleInputNode::explain();
    addDescription("condition", condition_ ? condition_->toString() : "", desc.get());
    return desc;
}

Project* Project::clone(QueryContext* qctx) const {
    auto newProj = Project::make(qctx, nullptr, nullptr);
    newProj->clone(*this);
    return newProj;
}

void Project::clone(const Project &p) {
    SingleInputNode::clone(p);
    cols_ = qctx_->objPool()->makeAndAdd<YieldColumns>();
    for (const auto &col : p.columns()->columns()) {
        cols_->addColumn(col->clone().release());
    }
}

std::unique_ptr<PlanNodeDescription> Project::explain() const {
    auto desc = SingleInputNode::explain();
    addDescription("columns", cols_ ? cols_->toString() : "", desc.get());
    return desc;
}

std::unique_ptr<PlanNodeDescription> Unwind::explain() const {
    auto desc = SingleInputNode::explain();
    addDescription("unwind", cols_? cols_->toString() : "", desc.get());
    return desc;
}

std::unique_ptr<PlanNodeDescription> Sort::explain() const {
    auto desc = SingleInputNode::explain();
    addDescription("factors", folly::toJson(util::toJson(factorsString())), desc.get());
    return desc;
}

Limit* Limit::clone(QueryContext* qctx) const {
    auto newLimit = Limit::make(qctx, nullptr, offset_, count_);
    newLimit->clone(*this);
    return newLimit;
}

void Limit::clone(const Limit &l) {
    SingleInputNode::clone(l);
    offset_ = l.offset_;
    count_ = l.count_;
}

std::unique_ptr<PlanNodeDescription> Limit::explain() const {
    auto desc = SingleInputNode::explain();
    addDescription("offset", folly::to<std::string>(offset_), desc.get());
    addDescription("count", folly::to<std::string>(count_), desc.get());
    return desc;
}

std::unique_ptr<PlanNodeDescription> TopN::explain() const {
    auto desc = SingleInputNode::explain();
    addDescription("factors", folly::toJson(util::toJson(factorsString())), desc.get());
    addDescription("offset", folly::to<std::string>(offset_), desc.get());
    addDescription("count", folly::to<std::string>(count_), desc.get());
    return desc;
}

std::unique_ptr<PlanNodeDescription> Aggregate::explain() const {
    auto desc = SingleInputNode::explain();
    addDescription("groupKeys", folly::toJson(util::toJson(groupKeys_)), desc.get());
    folly::dynamic itemArr = folly::dynamic::array();
    for (const auto& item : groupItems_) {
        folly::dynamic itemObj = folly::dynamic::object();
        itemObj.insert("distinct", util::toJson(item.distinct));
        itemObj.insert("funcType", static_cast<uint8_t>(item.func));
        itemObj.insert("expr", item.expr ? item.expr->toString() : "");
        itemArr.push_back(itemObj);
    }
    addDescription("groupItems", folly::toJson(itemArr), desc.get());
    return desc;
}
std::unique_ptr<PlanNodeDescription> SwitchSpace::explain() const {
    auto desc = SingleInputNode::explain();
    addDescription("space", spaceName_, desc.get());
    return desc;
}

std::unique_ptr<PlanNodeDescription> DataCollect::explain() const {
    auto desc = SingleDependencyNode::explain();
    addDescription("inputVar", folly::toJson(util::toJson(inputVars_)), desc.get());
    switch (collectKind_) {
        case CollectKind::kSubgraph: {
            addDescription("kind", "SUBGRAPH", desc.get());
            break;
        }
        case CollectKind::kRowBasedMove: {
            addDescription("kind", "ROW", desc.get());
            break;
        }
        case CollectKind::kMToN: {
            addDescription("kind", "M TO N", desc.get());
            break;
        }
        case CollectKind::kBFSShortest: {
            addDescription("kind", "BFS SHORTEST", desc.get());
            break;
        }
        case CollectKind::kAllPaths: {
            addDescription("kind", "ALL PATHS", desc.get());
            break;
        }
        case CollectKind::kMultiplePairShortest: {
            addDescription("kind", "Multiple Pair Shortest", desc.get());
            break;
        }
    }
    return desc;
}

std::unique_ptr<PlanNodeDescription> DataJoin::explain() const {
    auto desc = SingleDependencyNode::explain();
    folly::dynamic inputVar = folly::dynamic::object();
    inputVar.insert("leftVar", util::toJson(leftVar_));
    inputVar.insert("rightVar", util::toJson(rightVar_));
    addDescription("inputVar", folly::toJson(inputVar), desc.get());
    addDescription("hashKeys", folly::toJson(util::toJson(hashKeys_)), desc.get());
    addDescription("probeKeys", folly::toJson(util::toJson(probeKeys_)), desc.get());
    return desc;
}

}   // namespace graph
}   // namespace nebula
