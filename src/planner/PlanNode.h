/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLANNODE_H_
#define PLANNER_PLANNODE_H_

#include "base/Base.h"
#include "expression/Expression.h"
#include "planner/IdGenerator.h"

namespace nebula {
namespace graph {

class ExecutionPlan;

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
        kSwitchSpace,
        kDedup,
        kMultiOutputs,
        kCreateSpace,
        kCreateTag,
        kCreateEdge,
        kDescSpace,
        kDescTag,
        kDescEdge,
        kInsertVertices,
        kInsertEdges,
        // user related
        kCreateUser,
    };

    PlanNode(ExecutionPlan* plan, Kind kind);

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

    std::string varName() const {
        return varGenerated_;
    }

    const ExecutionPlan* plan() const {
        return plan_;
    }

    void setId(int64_t id) {
        id_ = id;
        varGenerated_ = folly::stringPrintf("%s_%ld", toString(kind_), id_);
    }

    void setPlan(ExecutionPlan* plan) {
        plan_ = plan;
    }

protected:
    static const char* toString(Kind kind);

    Kind                                     kind_{Kind::kUnknown};
    int64_t                                  id_{IdGenerator::INVALID_ID};
    ExecutionPlan*                           plan_{nullptr};
    using VariableName = std::string;
    std::unordered_set<VariableName>         availableVars_;
    VariableName                             varGenerated_;
};

// Some template node such as Create template for the node create something(user,tag...)
// Fit the conflict create process
class CreateNode : public PlanNode {
public:
    CreateNode(ExecutionPlan* plan, Kind kind, bool ifNotExist = false)
        : PlanNode(plan, kind), ifNotExist_(ifNotExist) {}

    bool ifNotExist() const {
        return ifNotExist_;
    }

private:
    bool ifNotExist_{false};
};

}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_PLANNODE_H_
