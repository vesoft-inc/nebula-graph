/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLANNODE_H_
#define PLANNER_PLANNODE_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "context/Symbols.h"

namespace nebula {
namespace graph {

namespace cpp2 {
class PlanNodeDescription;
}   // namespace cpp2

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
        kIntersect,
        kMinus,
        kProject,
        kSort,
        kTopN,
        kLimit,
        kAggregate,
        kSelect,
        kLoop,
        kSwitchSpace,
        kDedup,
        kPassThrough,
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
        kRebuildTagIndex,
        kRebuildEdgeIndex,
        kInsertVertices,
        kInsertEdges,
        kBalanceLeaders,
        kBalance,
        kStopBalance,
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
        kDataJoin,
        kDeleteVertices,
        kDeleteEdges,
        kUpdateVertex,
        kUpdateEdge,
        kShowParts,
        kShowCharset,
        kShowCollation,
        kShowConfigs,
        kSetConfig,
        kGetConfig,
        kBFSShortest,
        kConjunctPath,
    };

    PlanNode(int64_t id, Kind kind, SymbolTable* symTable);

    virtual ~PlanNode() = default;

    // Describe plan node
    virtual std::unique_ptr<cpp2::PlanNodeDescription> explain() const;

    virtual void calcCost();

    Kind kind() const {
        return kind_;
    }

    int64_t id() const {
        return id_;
    }

    void setOutputVar(std::string var) {
        DCHECK_EQ(1, outputVars_.size());
        auto* outputVarPtr = symTable_->findVar(var);
        DCHECK(outputVarPtr != nullptr);
        auto oldVar = outputVars_[0]->name;
        outputVarPtr->colNames = outputVars_[0]->colNames;
        outputVars_[0] = outputVarPtr;
        symTable_->updateAllOccurence(oldVar, outputVarPtr->name);
    }

    std::string outputVar(size_t index = 0) const {
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

    const std::vector<const PlanNode*>& dependencies() const {
        return dependencies_;
    }

    static const char* toString(Kind kind);

    double cost() const {
        return cost_;
    }

protected:
    static void addDescription(std::string key, std::string value, cpp2::PlanNodeDescription* desc);

    Kind                                     kind_{Kind::kUnknown};
    int64_t                                  id_{-1};
    SymbolTable*                             symTable_{nullptr};
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
    SingleDependencyNode(int64_t id, Kind kind, const PlanNode* dep, SymbolTable* symTable)
        : PlanNode(id, kind, symTable) {
        dependencies_.emplace_back(dep);
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;
};

class SingleInputNode : public SingleDependencyNode {
public:
    void setInputVar(std::string inputVar) {
        DCHECK(!inputVars_.empty());
        auto* inputVarPtr = symTable_->findVar(inputVar);
        DCHECK(inputVarPtr != nullptr);
        inputVars_[0] = inputVarPtr;
    }

    std::string inputVar() const {
        DCHECK(!inputVars_.empty());
        if (inputVars_[0] != nullptr) {
            return inputVars_[0]->name;
        } else {
            return "";
        }
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

protected:
    SingleInputNode(int64_t id, Kind kind, const PlanNode* dep, SymbolTable* symTable)
        : SingleDependencyNode(id, kind, dep, symTable) {
        if (dep != nullptr) {
            inputVars_.emplace_back(dep->outputVarPtr());
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

    void setLeftVar(std::string leftVar) {
        DCHECK_GE(inputVars_.size(), 1);
        auto* leftVarPtr = symTable_->findVar(leftVar);
        DCHECK(leftVarPtr != nullptr);
        inputVars_[0] = leftVarPtr;
    }

    void setRightVar(std::string rightVar) {
        DCHECK_EQ(inputVars_.size(), 2);
        auto* rightVarPtr = symTable_->findVar(rightVar);
        DCHECK(rightVarPtr != nullptr);
        inputVars_[1] = rightVarPtr;
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

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

protected:
    BiInputNode(int64_t id, Kind kind, PlanNode* left, PlanNode* right, SymbolTable* symTable)
        : PlanNode(id, kind, symTable) {
        DCHECK(left != nullptr);
        DCHECK(right != nullptr);
        dependencies_.emplace_back(left);
        dependencies_.emplace_back(right);
        inputVars_.emplace_back(left->outputVarPtr());
        inputVars_.emplace_back(right->outputVarPtr());
    }
};

}  // namespace graph
}  // namespace nebula

#endif  // PLANNER_PLANNODE_H_
