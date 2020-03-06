/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLANNODE_H_
#define PLANNER_PLANNODE_H_

#include "base/Base.h"
#include "filter/Expressions.h"

namespace nebula {
namespace graph {

/**
 * PlanNode is an abstraction of nodes in an execution plan which
 * is a kind of directed cyclic graph.
 */
class StartNode;
class PlanNode {
public:
    enum class Kind : uint8_t {
        kUnknown = 0,
        kStart,
        kGetNeighbors,
        kGetVertices,
        kGetEdges,
        kReadIndex,
        kFilter,
        kUnion,
        kIntersect,
        kMinus,
        kProject,
        kSort,
        kLimit,
        kAggregate,
        kSelector,
        kLoop,
        kBuildShortestPath,
        kRegisterVariable,
        kRegisterSpaceToSession,
    };

    PlanNode() = default;

    PlanNode(std::vector<std::string>&& colNames,
             std::shared_ptr<PlanNode>&& next) {
        outputColNames_ = std::move(colNames);
        next_ = std::move(next);
    }

    virtual ~PlanNode() = default;

    Kind kind() const {
        return kind_;
    }

    /**
     * To explain how a query would be executed
     */
    virtual std::string explain() const = 0;

    std::vector<std::string> outputColNames() const {
        return outputColNames_;
    }

    const PlanNode* next() {
        return next_.get();
    }

    void setOutputColNames(std::vector<std::string>&& cols) {
        outputColNames_ = std::move(cols);
    }

    void setNext(std::shared_ptr<PlanNode>&& next) {
        next_ = std::move(next);
    }

    /**
     * Append a sub-plan to another one.
     */
    Status append(std::shared_ptr<PlanNode> start);

    /**
     * Merge two sub-plan.
     */
    Status merge(std::shared_ptr<StartNode> start);

protected:
    Kind                        kind_{Kind::kUnknown};
    std::vector<std::string>    outputColNames_;
    std::shared_ptr<PlanNode>   next_;
};

/**
 * An execution plan will start from a StartNode.
 */
class StartNode final : public PlanNode {
public:
    StartNode() {
        kind_ = PlanNode::Kind::kStart;
    }

    StartNode(std::vector<std::string>&& colNames,
              std::shared_ptr<PlanNode>&& next) : PlanNode(std::move(colNames), std::move(next)) {
        kind_ = PlanNode::Kind::kStart;
    }

    std::string explain() const override {
        return "Start";
    }
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_PLANNODE_H_
