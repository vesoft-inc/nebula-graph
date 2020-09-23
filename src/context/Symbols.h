/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CONTEXT_SYMBOLS_H_
#define CONTEXT_SYMBOLS_H_

#include "planner/PlanNode.h"

namespace nebula {
namespace graph {

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
    std::string name;
    Value::Type type{Value::Type::DATASET};
    // Valid if type is dataset.
    ColsDef columns;
};

class SymbolTable final {
public:
    void addOrigin(std::string varName, PlanNode* node) {
        origins_.emplace(std::move(varName), node);
    }

    void addVar(std::string varName, Variable* variable) {
        vars_.emplace(std::move(varName), variable);
    }

    void addDerivative(std::string varName, folly::StringPiece derivative) {
        derivatives_[varName].emplace_back(derivative);
    }

    void addDependency(std::string varName, folly::StringPiece dependency) {
        dependencies_[varName].emplace_back(dependency);
    }

private:
    // var name -> plan node
    std::unordered_map<std::string, PlanNode*>                       origins_;
    // var name -> variable
    std::unordered_map<std::string, Variable*>                       vars_;
    // var name -> derivatives
    std::unordered_map<std::string, std::vector<folly::StringPiece>> derivatives_;
    // var name -> dependencies
    std::unordered_map<std::string, std::vector<folly::StringPiece>> dependencies_;
};
}  // namespace graph
}  // namespace nebula
#endif
