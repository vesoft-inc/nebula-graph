/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_EXECUTIONCONTEXT_H_
#define EXEC_EXECUTIONCONTEXT_H_

#include <cstdint>
#include <list>
#include <string>
#include <unordered_map>

#include "base/Status.h"
#include "interface/gen-cpp2/graph_types.h"

namespace nebula {
namespace graph {

class ExecutionContext final {
public:
    using DataSet = cpp2::Row;
    using DataSetList = std::list<cpp2::Row>;

    ExecutionContext() = default;

    void setValueReservation(int64_t valueReservation) {
        valueReservation_ = valueReservation;
    }

    // Register a executor in the context
    Status addExecutorResult(const std::string& executorId, DataSetList dataset);

    // Add variables value
    // TODO: Consider variable reference implementation
    Status addVariable(const std::string& varName, DataSet dataset);

private:
    // Check whether values have reached the limitation
    Status checkValues() const;

    // Count all values of this plan fragment
    uint64_t valueCounts_;

    // Number of value reservation for all executors
    int64_t valueReservation_{-1};

    // Map between executor's unique id and its result data after finishing execution
    std::unordered_map<std::string, std::list<DataSetList>> execData_;

    // Store all variable's values of query globally
    std::unordered_map<std::string, DataSet> variables_;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_EXECUTIONCONTEXT_H_
