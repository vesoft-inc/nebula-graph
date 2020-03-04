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

class PlanNode;

/**
 * The StateTransition tells executor which node it would transfer to.
 */
using StateTransitionTable = std::vector<std::shared_ptr<PlanNode>>;

class StateTransition {
public:
    enum class State : int8_t {
        kUnknown = 0,
        kTrue = 1,
        kFalse = 2,
        kConcurrency = 3
    };

    const Expression* expr() const {
        return expr_.get();
    }

    const StateTransitionTable& table() const {
        return table_;
    }

    void setNthNext(size_t i, std::shared_ptr<PlanNode> node) {
        table_[i] = node;
    }

    void setTable(StateTransitionTable&& table) {
        table_ = std::move(table);
    }

    void addNodes(StateTransitionTable&& table) {
        table_.insert(table_.end(),
                std::make_move_iterator(table.begin()),
                std::make_move_iterator(table.end()));
    }

private:
    std::unique_ptr<Expression>             expr_;
    StateTransitionTable                    table_;
};

class StartNode;

/**
 * PlanNode is an abstraction of nodes in an execution plan which
 * is a kind of directed cyclic graph.
 */
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
        kAggregate
    };

    PlanNode() = default;

    PlanNode(std::vector<std::string>&& colNames, StateTransition&& stateTrans) {
        outputColNames_ = std::move(colNames);
        stateTrans_ = std::move(stateTrans);
    }

    virtual ~PlanNode() = default;

    void setOutputColNames(std::vector<std::string>&& cols) {
        outputColNames_ = std::move(cols);
    }

    void setStateTrans(StateTransition&& stateTrans) {
        stateTrans_ = std::move(stateTrans);
    }

    void setPreTrans(StateTransition&& prevTrans) {
        prevTrans_ = std::move(prevTrans);
    }

    // Replace old successor plan node with new one
    void replace(std::shared_ptr<PlanNode> old, std::shared_ptr<PlanNode> newNext);

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

    /**
     * Execution engine will calculate the state by this expression.
     */
    const Expression* stateTransExpr() {
        return stateTrans_.expr();
    }

    /**
     * This table is used for finding the next node(s) to be executed.
     */
    const StateTransitionTable& table() const {
        return stateTrans_.table();
    }

    /**
     * This table is used for finding the previous node(s) to be executed.
     */
    const StateTransitionTable& prevTable() const {
        return prevTrans_.table();
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
    StateTransition             stateTrans_;

    // Previous plan nodes which are depended by this one
    StateTransition prevTrans_;
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
             StateTransition&& stateTrans) : PlanNode(std::move(colNames), std::move(stateTrans)) {
        kind_ = PlanNode::Kind::kStart;
    }

    std::string explain() const override {
        return "Start";
    }
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_PLANNODE_H_
