/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "context/Symbols.h"

#include <sstream>

#include <folly/String.h>

#include "planner/Planner.h"

namespace nebula {
namespace graph {

template <typename Container, typename Fn>
std::string join(const Container& container, Fn fn, const std::string& delimiter = ",") {
    std::vector<std::string> strs;
    for (auto iter = std::begin(container), end = std::end(container); iter != end; ++iter) {
        strs.emplace_back(fn(*iter));
    }
    return folly::join(delimiter, strs);
}

std::string printNode(const PlanNode* node) {
    return folly::stringPrintf("%s_%ld", PlanNode::toString(node->kind()), node->id());
}

std::string Variable::toString() const {
    std::stringstream ss;
    ss << "name: " << name << ", type: " << type << ", colNames: <" << folly::join(",", colNames)
       << ">, readBy: <" << join(readBy, printNode) << ">, writtenBy: <"
       << join(writtenBy, printNode) << ">";
    return ss.str();
}

std::string SymbolTable::toString() const {
    std::stringstream ss;
    ss << "SymTable: [";
    for (const auto& p : vars_) {
        ss << "\n" << p.first << ": ";
        if (p.second) {
            ss << p.second->toString();
        }
    }
    ss << "\n]";
    return ss.str();
}

}   // namespace graph
}   // namespace nebula
