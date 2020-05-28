/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "context/ExecutionContext.h"

namespace nebula {
namespace graph {
void ExecutionContext::setValue(const std::string& name, Value&& val) {
    auto& hist = valueMap_[name];
    hist.emplace_back(Result::buildDefault(std::move(val)));
}


void ExecutionContext::setResult(const std::string& name, Result&& result) {
    auto& hist = valueMap_[name];
    hist.emplace_back(std::move(result));
}


void ExecutionContext::deleteValue(const std::string& name) {
    valueMap_.erase(name);
}


size_t ExecutionContext::numVersions(const std::string& name) const {
    auto it = valueMap_.find(name);
    CHECK(it != valueMap_.end());
    return it->second.size();
}


// Only keep the last several versoins of the Value
void ExecutionContext::truncHistory(const std::string& name, size_t numVersionsToKeep) {
    auto it = valueMap_.find(name);
    if (it != valueMap_.end()) {
        if (it->second.size() <= numVersionsToKeep) {
            return;
        }
        // Only keep the latest N values
        it->second.erase(it->second.begin(), it->second.end() - numVersionsToKeep);
    }
}


// Get the latest version of the value
const Value& ExecutionContext::getValue(const std::string& name) const {
    auto it = valueMap_.find(name);
    if (it != valueMap_.end()) {
        return it->second.back().value();
    } else {
        return kEmpty;
    }
}


Value&& ExecutionContext::moveValue(const std::string& name) {
    auto it = valueMap_.find(name);
    if (it != valueMap_.end()) {
        return it->second.back().moveValue();
    } else {
        return std::move(Value());
    }
}


const Result& ExecutionContext::getResult(const std::string& name) const {
    static const Result kEmptyResult;
    auto it = valueMap_.find(name);
    if (it != valueMap_.end()) {
        return it->second.back();
    } else {
        return kEmptyResult;
    }
}


const std::vector<Result>& ExecutionContext::getHistory(const std::string& name) const {
    static const std::vector<Result> kEmptyList;

    auto it = valueMap_.find(name);
    if (it != valueMap_.end()) {
        return it->second;
    } else {
        return kEmptyList;
    }
}
}  // namespace graph
}  // namespace nebula
