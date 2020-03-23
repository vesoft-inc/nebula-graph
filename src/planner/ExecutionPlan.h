/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLANNODE_H_
#define PLANNER_PLANNODE_H_

#include "base/Base.h"
#include "filter/Expressions.h"
#include "planner/IdGenerator.h"

namespace nebula {
namespace graph {

/**
 * PlanNode is an abstraction of nodes in an execution plan which
 * is a kind of directed cyclic graph.
 */
class PlanNode {
public:
    enum class Kind : uint8_t {
        kUnknown = 0,
        kStart,
        kEnd,
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
        kRegisterSpaceToSession,
        kDedup,
    };

    PlanNode() = default;

    virtual ~PlanNode() = default;

    /**
     * To explain how a query would be executed
     */
    virtual std::string explain() const = 0;

    Kind kind() const {
        return kind_;
    }

    int64_t id() const {
        return id_;
    }

    void setId(int64_t id) {
        id_ = id;
    }

protected:
    Kind                                     kind_{Kind::kUnknown};
    int64_t                                  id_{IdGenerator::INVALID_ID};
    using VariableName = std::string;
    std::unordered_set<VariableName>         availableVars_;
};

class ExecutionPlan final {
public:
    ExecutionPlan() {
        id_ = EPIdGenerator::instance().id();
    }

    void setRoot(PlanNode* root) {
        root_ = root;
    }

    PlanNode* addPlanNode(std::unique_ptr<PlanNode>&& node) {
        node->setId(nodeIdGen_.id());
        auto* tmp = node.get();
        nodes_.emplace_back(std::move(node));
        return tmp;
    }

private:
    int64_t                                 id_{IdGenerator::INVALID_ID};
    PlanNode*                               root_;
    std::vector<std::unique_ptr<PlanNode>>  nodes_;
    IdGenerator                             nodeIdGen_;
};

/**
 * The StartNode and EndNode are only used in a subplan.
 */
class StartNode final : public PlanNode {
public:
    StartNode() {
        kind_ = PlanNode::Kind::kStart;
    }

    std::string explain() const override {
        return "Start";
    }
};

class EndNode final : public PlanNode {
public:
    EndNode() {
        kind_ = PlanNode::Kind::kEnd;
    }

    static PlanNode* make(ExecutionPlan* plan) {
        auto end = std::make_unique<EndNode>();
        return plan->addPlanNode(std::move(end));
    }

    std::string explain() const override {
        return "End";
    }
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_PLANNODE_H_
