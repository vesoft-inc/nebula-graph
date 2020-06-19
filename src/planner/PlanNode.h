/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLANNODE_H_
#define PLANNER_PLANNODE_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "util/IdGenerator.h"

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
        kSelect,
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
        kDropUser,
        kUpdateUser,
        kGrantRole,
        kRevokeRole,
        kChangePassword,
        kListUserRoles,
        kListUsers,
        kListRoles,
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

    void setOutputVar(std::string var) {
        outputVar_ = std::move(var);
    }

    std::string varName() const {
        return outputVar_;
    }

    const ExecutionPlan* plan() const {
        return plan_;
    }

    std::vector<std::string> colNames() const {
        return colNames_;
    }

    void setId(int64_t id) {
        id_ = id;
        outputVar_ = folly::stringPrintf("%s_%ld", toString(kind_), id_);
    }

    void setPlan(ExecutionPlan* plan) {
        plan_ = plan;
    }

    void setColNames(std::vector<std::string>&& cols) {
        colNames_ = std::move(cols);
    }

    static const char* toString(Kind kind);

protected:
    Kind                                     kind_{Kind::kUnknown};
    int64_t                                  id_{IdGenerator::INVALID_ID};
    ExecutionPlan*                           plan_{nullptr};
    using VariableName = std::string;
    VariableName                             outputVar_;
    std::vector<std::string>                 colNames_;
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

class DropNode : public PlanNode {
public:
    DropNode(ExecutionPlan* plan, Kind kind, bool ifExist = false)
        : PlanNode(plan, kind), ifExist_(ifExist) {}

    bool ifExist() const {
        return ifExist_;
    }

private:
    bool ifExist_{false};
};

std::ostream& operator<<(std::ostream& os, PlanNode::Kind kind);

}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_PLANNODE_H_
