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

    void addOrigin(std::string varName, PlanNode* node) {
        origins_.emplace(std::move(varName), node);
    }

    void addVar(std::string varName, Variable* variable) {
        vars_.emplace(std::move(varName), variable);
    }

    void addDerivative(std::string varName, std::string derivative) {
        VLOG(1) << "Add derivative: " << varName << " -> " << derivative;
        derivatives_[varName].emplace(derivative);
        VLOG(1) << "derivative size: " << derivatives_.size();
    }

    void addDependency(std::string varName, std::string dependency) {
        VLOG(1) << "Add deoendency: " << varName << " <- " << dependency;
        dependencies_[varName].emplace(dependency);
        VLOG(1) << "dependencies size: " << dependencies_.size();
    }

    void deleteDerivative(std::string varName, std::string derivative) {
        auto derivatives = derivatives_.find(varName);
        if (derivatives != derivatives_.end()) {
            derivatives->second.erase(derivative);
        }
    }

    void deleteDependency(std::string varName, std::string dependency) {
        auto dependencies = dependencies_.find(varName);
        if (dependencies != dependencies_.end()) {
            dependencies->second.erase(dependency);
        }
    }

    void updateAllOccurence(std::string oldVar, std::string newVar) {
        VLOG(1) << "Update ocur: " << oldVar << " -> " << newVar;
        if (oldVar == newVar) {
            return;
        }

        auto oldDerivative = derivatives_.find(oldVar);
        if (oldDerivative != derivatives_.end()) {
            derivatives_.emplace(newVar, std::move(oldDerivative->second));
            derivatives_.erase(oldVar);
        }
        for (auto& derivative : derivatives_) {
            if (derivative.second.erase(oldVar) > 0) {
                derivative.second.emplace(newVar);
            }
        }

        auto oldDependency = dependencies_.find(oldVar);
        if (oldDependency != dependencies_.end()) {
            dependencies_.emplace(newVar, std::move(oldDependency->second));
            dependencies_.erase(oldVar);
        }
        for (auto& dependency : dependencies_) {
            if (dependency.second.erase(oldVar) > 0) {
                dependency.second.emplace(newVar);
            }
        }
    }

    Variable* getVar(const std::string& varName) {
        auto var = vars_.find(varName);
        if (var == vars_.end()) {
            return nullptr;
        } else {
            return var->second;
        }
    }

    PlanNode* getOrigin(const std::string& varName) {
        auto node = origins_.find(varName);
        if (node == origins_.end()) {
            return nullptr;
        } else {
            return node->second;
        }
    }

    std::unordered_set<std::string>& getDerivatives(const std::string& varName) {
        static std::unordered_set<std::string> emptyDerivatives;
        auto derivatives = derivatives_.find(varName);
        if (derivatives == derivatives_.end()) {
            return emptyDerivatives;
        } else {
            return derivatives->second;
        }
    }

    std::unordered_set<std::string>& getDependencies(const std::string& varName) {
        static std::unordered_set<std::string> emptyDependencies;
        auto dependencies = dependencies_.find(varName);
        if (dependencies == dependencies_.end()) {
            return emptyDependencies;
        } else {
            return dependencies->second;
        }
    }

private:
    ObjectPool*                                                             objPool_{nullptr};
    // var name -> plan node
    std::unordered_map<std::string, PlanNode*>                              origins_;
    // var name -> variable
    std::unordered_map<std::string, Variable*>                              vars_;
    // var name -> derivatives
    std::unordered_map<std::string, std::unordered_set<std::string>>        derivatives_;
    // var name -> dependencies
    std::unordered_map<std::string, std::unordered_set<std::string>>        dependencies_;
};
}  // namespace graph
}  // namespace nebula
#endif
