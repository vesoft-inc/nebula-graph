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
        kSwitchSpace,
        kCreateSpace,
        kCreateTag,
        kCreateEdge,
        kDescSpace,
        kDescTag,
        kDescEdge,
        // Put no input plan node above
        kAnchorNoInput,

        kGetNeighbors,
        kGetVertices,
        kGetEdges,
        kReadIndex,
        kFilter,
        kProject,
        kSort,
        kLimit,
        kAggregate,
        kDedup,
        kMultiOutputs,
        kInsertVertices,
        kInsertEdges,
        kSelector,
        kLoop,
        // Put single input plan node above
        kAnchorSingleInput,

        kUnion,
        kIntersect,
        kMinus,
        // Put biInput plan node above
        kAnchorBiInput,
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

    // Is derived from no input node
    bool isNoInput() const {
        return static_cast<uint8_t>(kind_) > static_cast<uint8_t>(Kind::kUnknown) &&
            static_cast<uint8_t>(kind_) < static_cast<uint8_t>(Kind::kAnchorNoInput);
    }

    // Is derived from single input node
    bool isSingleInput() const {
        return static_cast<uint8_t>(kind_) > static_cast<uint8_t>(Kind::kAnchorNoInput) &&
            static_cast<uint8_t>(kind_) < static_cast<uint8_t>(Kind::kAnchorSingleInput);
    }

    // Is derived from biInput node
    bool isBiInput() const {
        return static_cast<uint8_t>(kind_) > static_cast<uint8_t>(Kind::kAnchorSingleInput) &&
            static_cast<uint8_t>(kind_) < static_cast<uint8_t>(Kind::kAnchorBiInput);
    }

protected:
    Kind                                     kind_{Kind::kUnknown};
    int64_t                                  id_{IdGenerator::INVALID_ID};
    ExecutionPlan*                           plan_{nullptr};
    using VariableName = std::string;
    VariableName                             outputVar_;
    std::vector<std::string>                 colNames_;
};

std::ostream& operator<<(std::ostream& os, PlanNode::Kind kind);
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_PLANNODE_H_
