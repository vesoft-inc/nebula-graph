/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CONTEXT_SYMBOLS_H_
#define CONTEXT_SYMBOLS_H_

#include "util/ObjectPool.h"

namespace nebula {
namespace graph {

class PlanNode;

struct ColDef {
    ColDef(std::string n, Value::Type t) {
        name = std::move(n);
        type = std::move(t);
    }

    bool operator==(const ColDef& rhs) const {
        return name == rhs.name && type == rhs.type;
    }

    std::string name;
    Value::Type type;
};

using ColsDef = std::vector<ColDef>;

struct Variable {
    explicit Variable(std::string n) : name(std::move(n)) {}

    std::string name;
    Value::Type type{Value::Type::DATASET};
    // Valid if type is dataset.
    std::vector<std::string> colNames;

    PlanNode* origin;
    std::unordered_set<Variable*> derivatives;
    std::unordered_set<Variable*> dependencies;
    std::unordered_set<PlanNode*> readBy;
    std::unordered_set<PlanNode*> writtenBy;
};

class SymbolTable final {
public:
    explicit SymbolTable(ObjectPool* objPool) {
        DCHECK(objPool != nullptr);
        objPool_ = objPool;
    }

    Variable* newVariable(std::string name) {
        VLOG(1) << "New variable for: " << name;
        auto* variable = objPool_->makeAndAdd<Variable>(name);
        addVar(std::move(name), variable);
        return variable;
    }

    void addVar(std::string varName, Variable* variable) {
        vars_.emplace(std::move(varName), variable);
    }

    bool addDerivative(std::string varName, std::string derivative) {
        VLOG(1) << "Add derivative: " << varName << " -> " << derivative;
        auto var = vars_.find(varName);
        if (var == vars_.end()) {
            return false;
        }
        auto der = vars_.find(derivative);
        if (der == vars_.end()) {
            return false;
        }
        var->second->derivatives.emplace(der->second);
        return true;
    }

    bool addDependency(std::string varName, std::string dependency) {
        VLOG(1) << "Add dependency: " << varName << " <- " << dependency;
        auto var = vars_.find(varName);
        if (var == vars_.end()) {
            return false;
        }
        auto dep = vars_.find(dependency);
        if (dep == vars_.end()) {
            return false;
        }
        var->second->dependencies.emplace(dep->second);
        return true;
    }

    bool readBy(std::string varName, PlanNode* node) {
        auto var = vars_.find(varName);
        if (var == vars_.end()) {
            return false;
        }
        var->second->readBy.emplace(node);
        return true;
    }

    bool writtenBy(std::string varName, PlanNode* node) {
        auto var = vars_.find(varName);
        if (var == vars_.end()) {
            return false;
        }
        var->second->writtenBy.emplace(node);
        return true;
    }

    bool deleteDerivative(std::string varName, std::string derivative) {
        auto var = vars_.find(varName);
        if (var == vars_.end()) {
            return false;
        }
        auto der = vars_.find(derivative);
        if (der == vars_.end()) {
            return false;
        }
        var->second->derivatives.erase(der->second);
        return true;
    }

    bool deleteDependency(std::string varName, std::string dependency) {
        auto var = vars_.find(varName);
        if (var == vars_.end()) {
            return false;
        }
        auto dep = vars_.find(dependency);
        if (dep == vars_.end()) {
            return false;
        }
        var->second->dependencies.erase(dep->second);
        return true;
    }

    bool deleteRead(std::string varName, PlanNode* node) {
        auto var = vars_.find(varName);
        if (var == vars_.end()) {
            return false;
        }
        var->second->readBy.erase(node);
        return true;
    }

    bool deleteWritten(std::string varName, PlanNode* node) {
        auto var = vars_.find(varName);
        if (var == vars_.end()) {
            return false;
        }
        var->second->writtenBy.erase(node);
        return true;
    }

    bool updateAllOccurence(std::string oldVar, std::string newVar) {
        VLOG(1) << "Update ocur: " << oldVar << " -> " << newVar;
        if (oldVar == newVar) {
            return true;
        }

        auto old = vars_.find(oldVar);
        if (old == vars_.end()) {
            return false;
        }
        auto newVariable = vars_.find(newVar);
        if (newVariable == vars_.end()) {
            return false;
        }

        newVariable->second->derivatives.insert(old->second->derivatives.begin(),
                                                old->second->derivatives.end());
        old->second->derivatives.clear();
        newVariable->second->dependencies.insert(old->second->dependencies.begin(),
                                                 old->second->dependencies.begin());
        old->second->dependencies.clear();
        for (auto& var : vars_) {
            if (var.second->derivatives.erase(old->second) > 0) {
                var.second->derivatives.emplace(newVariable->second);
            }
            if (var.second->dependencies.erase(old->second) > 0) {
                var.second->dependencies.emplace(newVariable->second);
            }
        }
        return true;
    }

    Variable* getVar(const std::string& varName) {
        auto var = vars_.find(varName);
        if (var == vars_.end()) {
            return nullptr;
        } else {
            return var->second;
        }
    }

private:
    ObjectPool*                                                             objPool_{nullptr};
    // var name -> variable
    std::unordered_map<std::string, Variable*>                              vars_;
};
}  // namespace graph
}  // namespace nebula
#endif
