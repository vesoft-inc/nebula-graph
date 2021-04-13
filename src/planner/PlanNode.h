/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLANNODE_H_
#define PLANNER_PLANNODE_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "common/graph/Response.h"
#include "context/QueryContext.h"
#include "context/Symbols.h"

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
        kGetNeighbors,
        kGetVertices,
        kGetEdges,
        kIndexScan,
        kFilter,
        kUnion,
        kUnionAllVersionVar,
        kIntersect,
        kMinus,
        kProject,
        kUnwind,
        kSort,
        kTopN,
        kLimit,
        kAggregate,
        kSelect,
        kLoop,
        kSwitchSpace,
        kDedup,
        kPassThrough,
        kAssign,
        // schema related
        kCreateSpace,
        kCreateTag,
        kCreateEdge,
        kDescSpace,
        kShowCreateSpace,
        kDescTag,
        kDescEdge,
        kAlterTag,
        kAlterEdge,
        kShowSpaces,
        kShowTags,
        kShowEdges,
        kShowCreateTag,
        kShowCreateEdge,
        kDropSpace,
        kDropTag,
        kDropEdge,
        // index related
        kCreateTagIndex,
        kCreateEdgeIndex,
        kDropTagIndex,
        kDropEdgeIndex,
        kDescTagIndex,
        kDescEdgeIndex,
        kShowCreateTagIndex,
        kShowCreateEdgeIndex,
        kShowTagIndexes,
        kShowEdgeIndexes,
        kShowTagIndexStatus,
        kShowEdgeIndexStatus,
        kInsertVertices,
        kInsertEdges,
        kBalanceLeaders,
        kBalance,
        kStopBalance,
        kResetBalance,
        kShowBalance,
        kSubmitJob,
        kShowHosts,
        kDataCollect,
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
        kCreateSnapshot,
        kDropSnapshot,
        kShowSnapshots,
        kLeftJoin,
        kInnerJoin,
        kDeleteVertices,
        kDeleteEdges,
        kUpdateVertex,
        kUpdateEdge,
        kShowParts,
        kShowCharset,
        kShowCollation,
        kShowStats,
        kShowConfigs,
        kShowGroups,
        kShowZones,
        kSetConfig,
        kGetConfig,
        kBFSShortest,
        kProduceSemiShortestPath,
        kConjunctPath,
        kProduceAllPaths,
        kCartesianProduct,
        kSubgraph,
        // zone related
        kAddGroup,
        kDropGroup,
        kDescribeGroup,
        kAddZoneIntoGroup,
        kDropZoneFromGroup,
        kAddZone,
        kDropZone,
        kDescribeZone,
        kAddHostIntoZone,
        kDropHostFromZone,
        // listener related
        kAddListener,
        kRemoveListener,
        kShowListener,
        // text service related
        kShowTSClients,
        kSignInTSService,
        kSignOutTSService,
        kDownload,
        kIngest,
    };

    PlanNode(QueryContext* qctx, Kind kind);

    virtual ~PlanNode() = default;

    // Describe plan node
    virtual std::unique_ptr<PlanNodeDescription> explain() const;

    virtual void calcCost();

    Kind kind() const {
        return kind_;
    }

    int64_t id() const {
        return id_;
    }

    QueryContext* qctx() const {
        return qctx_;
    }

    bool isSingleInput() const {
        return numDeps() == 1U;
    }

    void setOutputVar(const std::string &var);

    const std::string& outputVar(size_t index = 0) const {
        DCHECK_LT(index, outputVars_.size());
        return outputVars_[index]->name;
    }

    Variable* outputVarPtr(size_t index = 0) const {
        DCHECK_LT(index, outputVars_.size());
        return outputVars_[index];
    }

    const std::vector<Variable*>& outputVars() const {
        return outputVars_;
    }

    std::vector<std::string> colNames() const {
        DCHECK(!outputVars_.empty());
        return outputVars_[0]->colNames;
    }

    const std::vector<std::string>& colNamesRef() const {
        DCHECK(!outputVars_.empty());
        return outputVars_[0]->colNames;
    }

    void setId(int64_t id) {
        id_ = id;
    }

    void setColNames(std::vector<std::string>&& cols) {
        DCHECK(!outputVars_.empty());
        outputVars_[0]->colNames = std::move(cols);
    }

    void setColNames(const std::vector<std::string>& cols) {
        DCHECK(!outputVars_.empty());
        outputVars_[0]->colNames = cols;
    }

    const PlanNode* dep(size_t index = 0) const {
        DCHECK_LT(index, dependencies_.size());
        return dependencies_.at(index);
    }

    void setDep(size_t index, const PlanNode* dep) {
        DCHECK_LT(index, dependencies_.size());
        dependencies_[index] = DCHECK_NOTNULL(dep);
    }

    void addDep(const PlanNode* dep) {
        dependencies_.emplace_back(dep);
    }

    size_t numDeps() const {
        return dependencies_.size();
    }

    std::string inputVar(size_t idx = 0UL) const {
        DCHECK_LT(idx, inputVars_.size());
        return inputVars_[idx] ? inputVars_[idx]->name : "";
    }

    void setInputVar(const std::string& varname, size_t idx = 0UL);

    const std::vector<Variable*>& inputVars() const {
        return inputVars_;
    }

    void releaseSymbols();

    static const char* toString(Kind kind);
    std::string toString() const;

    double cost() const {
        return cost_;
    }

protected:
    static void addDescription(std::string key, std::string value, PlanNodeDescription* desc);

    void readVariable(const std::string& varname);
    void readVariable(Variable* varPtr);
    void clone(const PlanNode &node) {
        // TODO maybe shall copy cost_ and dependencies_ too
        inputVars_ = node.inputVars_;
        outputVars_ = node.outputVars_;
    }

    QueryContext*                            qctx_{nullptr};
    Kind                                     kind_{Kind::kUnknown};
    int64_t                                  id_{-1};
    double                                   cost_{0.0};
    std::vector<const PlanNode*>             dependencies_;
    std::vector<Variable*>                   inputVars_;
    std::vector<Variable*>                   outputVars_;
};

std::ostream& operator<<(std::ostream& os, PlanNode::Kind kind);

// Dependencies will cover the inputs, For example bi input require bi dependencies as least,
// but single dependencies may don't need any inputs (I.E admin plan node)
// Single dependecy without input
// It's useful for admin plan node
class SingleDependencyNode : public PlanNode {
public:
    void dependsOn(const PlanNode* dep) {
        setDep(0, dep);
    }

protected:
    SingleDependencyNode(QueryContext* qctx, Kind kind, const PlanNode* dep)
        : PlanNode(qctx, kind) {
        addDep(dep);
    }

    void clone(const SingleDependencyNode &node) {
        PlanNode::clone(node);
    }

    std::unique_ptr<PlanNodeDescription> explain() const override;
};

class SingleInputNode : public SingleDependencyNode {
public:
    std::unique_ptr<PlanNodeDescription> explain() const override;

protected:
    void clone(const SingleInputNode &node) {
        SingleDependencyNode::clone(node);
    }

    SingleInputNode(QueryContext* qctx, Kind kind, const PlanNode* dep)
        : SingleDependencyNode(qctx, kind, dep) {
        if (dep != nullptr) {
            readVariable(dep->outputVarPtr());
        } else {
            inputVars_.emplace_back(nullptr);
        }
    }
};

class BiInputNode : public PlanNode {
public:
    void setLeftDep(const PlanNode* left) {
        setDep(0, left);
    }

    void setRightDep(const PlanNode* right) {
        setDep(1, right);
    }

    void setLeftVar(const std::string& leftVar) {
        setInputVar(leftVar, 0);
    }

    void setRightVar(const std::string& rightVar) {
        setInputVar(rightVar, 1);
    }

    const PlanNode* left() const {
        return dep(0);
    }

    const PlanNode* right() const {
        return dep(1);
    }

    const std::string& leftInputVar() const {
        return inputVars_[0]->name;
    }

    const std::string& rightInputVar() const {
        return inputVars_[1]->name;
    }

    std::unique_ptr<PlanNodeDescription> explain() const override;

protected:
    BiInputNode(QueryContext* qctx, Kind kind, const PlanNode* left, const PlanNode* right);
};

}  // namespace graph
}  // namespace nebula

#endif  // PLANNER_PLANNODE_H_
